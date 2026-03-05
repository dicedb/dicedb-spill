#include "valkeymodule.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define MIN_MAX_MEMORY_MB      20  // Minimum max-memory limit: 20MB
#define DEFAULT_NUM_DATABASES  16  // Fallback if CONFIG GET databases fails

// Logging control: set to 1 to enable debug/verbose logging, 0 to disable
#define SPILL_DEBUG 0

#if SPILL_DEBUG
#define LOG(ctx, level, ...) ValkeyModule_Log(ctx, level, __VA_ARGS__)
#else
#define LOG(ctx, level, ...) (void)0
#endif

// One RocksDB instance per Valkey database.
// num_databases is set at load time from `CONFIG GET databases`.
// Shared options/read-options/write-options across all instances.
static rocksdb_t             **dbs          = NULL;
static int                     num_databases = 0;
static rocksdb_options_t      *options  = NULL;
static rocksdb_readoptions_t  *roptions = NULL;
static rocksdb_writeoptions_t *woptions = NULL;
static ValkeyModuleCtx        *module_ctx = NULL;

// RW lock: cleanup thread holds read lock during PerformCleanup;
// TruncateRocksDB holds write lock (exclusive) during destroy+reopen.
static pthread_rwlock_t db_rwlock = PTHREAD_RWLOCK_INITIALIZER;

typedef struct {
    uint64_t num_keys_stored;       // Active keys across all DB instances (dynamically tracked)
    uint64_t total_keys_written;    // Total write operations since server restart (cumulative)
    uint64_t total_keys_restored;   // Keys restored since server restart
    uint64_t total_keys_cleaned;    // Cumulative keys cleaned since server restart
    uint64_t last_num_keys_cleaned; // Keys cleaned in most recent cleanup job
    int64_t  last_cleanup_at;       // Unix timestamp (seconds) of last cleanup, 0 if never run
    uint64_t total_bytes_written;   // Bytes written since server restart (includes metadata)
    uint64_t total_bytes_read;      // Bytes read since server restart (includes metadata)
} SpillStats;

static SpillStats stats = {0};

static const char *ERR_DB_NOT_INIT    = "ERR RocksDB not initialized";
static const char *ERR_CORRUPTED_DATA = "ERR Corrupted data in RocksDB";

typedef struct {
    char  *path;
    size_t max_memory;       // Total memory budget across all DB instances
    int    cleanup_interval; // Periodic cleanup interval in seconds
} SpillConfig;

static SpillConfig config = {
    .path             = NULL,
    .max_memory       = 256 * 1024 * 1024, // Default: 256MB total memory budget
    .cleanup_interval = 300                 // Default: 5 minutes (300 seconds)
};

// Cleanup thread management
static pthread_t       cleanup_thread;
static volatile int    cleanup_thread_running = 0;

// Forward declarations
static uint64_t PerformCleanup(void);

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

// Returns a heap-allocated path string for the given DB number.
// Caller must free() the result.
static char *DBPath(int dbnum) {
    int   len  = snprintf(NULL, 0, "%s/db%d", config.path, dbnum) + 1;
    char *path = malloc(len);
    if (path) snprintf(path, len, "%s/db%d", config.path, dbnum);
    return path;
}

// ---------------------------------------------------------------------------
// Low-level RocksDB helpers
// ---------------------------------------------------------------------------

static void DeleteKeyFromDB(int dbnum, const char *keyname, size_t keylen) {
    if (dbnum < 0 || dbnum >= num_databases || !dbs[dbnum]) return;

    char *err = NULL;
    rocksdb_delete(dbs[dbnum], woptions, keyname, keylen, &err);
    if (err) {
        LOG(module_ctx, "warning",
            "failed to delete key from RocksDB[%d]: %.*s, error: %s",
            dbnum, (int)keylen, keyname, err);
        free(err);
    } else {
        LOG(module_ctx, "debug",
            "deleted key from RocksDB[%d]: %.*s", dbnum, (int)keylen, keyname);
    }
}

// Restore a key from RocksDB[dbnum] back into Valkey.
// Returns: 0 on success, -1 on error, -2 if not found, -3 if expired.
static int RestoreKeyFromDB(ValkeyModuleCtx *ctx, int dbnum,
                            ValkeyModuleString *key,
                            const char *keyname, size_t keylen) {
    if (dbnum < 0 || dbnum >= num_databases || !dbs[dbnum]) return -1;

    char  *err = NULL;
    size_t vallen;
    char  *val = rocksdb_get(dbs[dbnum], roptions, keyname, keylen, &vallen, &err);

    if (err != NULL) {
        LOG(ctx, "warning", "error getting key=%.*s from RocksDB[%d]: %s",
            (int)keylen, keyname, dbnum, err);
        free(err);
        return -1;
    }

    if (val == NULL) {
        LOG(ctx, "notice", "key=%.*s not found in RocksDB[%d]",
            (int)keylen, keyname, dbnum);
        return -2;
    }

    LOG(ctx, "notice", "key=%.*s found in RocksDB[%d], value_len=%zu bytes",
        (int)keylen, keyname, dbnum, vallen);

    if (vallen < sizeof(int64_t)) {
        free(val);
        return -1;
    }

    char    *ptr = val;
    int64_t  expiry_time_ms;
    memcpy(&expiry_time_ms, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);

    // Check expiry
    if (expiry_time_ms > 0) {
        int64_t current_time_ms = (int64_t)time(NULL) * 1000;
        if (expiry_time_ms <= current_time_ms) {
            LOG(ctx, "notice",
                "key=%.*s has expired (expiry=%lld, now=%lld), deleting",
                (int)keylen, keyname,
                (long long)expiry_time_ms, (long long)current_time_ms);
            DeleteKeyFromDB(dbnum, keyname, keylen);
            free(val);
            return -3;
        }
    }

    size_t             dump_len  = vallen - sizeof(int64_t);
    ValkeyModuleString *dump_data = ValkeyModule_CreateString(ctx, ptr, dump_len);

    ValkeyModuleCallReply *reply;
    if (expiry_time_ms > 0) {
        int64_t current_time_ms = (int64_t)time(NULL) * 1000;
        int64_t ttl_ms = expiry_time_ms - current_time_ms;
        if (ttl_ms < 0) ttl_ms = 1;
        LOG(ctx, "debug", "restoring key=%.*s with TTL=%lld ms",
            (int)keylen, keyname, (long long)ttl_ms);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, ttl_ms, dump_data, "REPLACE");
    } else {
        LOG(ctx, "debug", "restoring key=%.*s with no expiry",
            (int)keylen, keyname);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, 0, dump_data, "REPLACE");
    }

    free(val);
    ValkeyModule_FreeString(ctx, dump_data);

    int result = 0;
    if (reply && ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ERROR) {
        LOG(ctx, "debug", "key=%.*s restored successfully from RocksDB[%d]",
            (int)keylen, keyname, dbnum);
        __sync_fetch_and_add(&stats.total_keys_restored, 1);
        __sync_fetch_and_add(&stats.total_bytes_read, vallen);
        DeleteKeyFromDB(dbnum, keyname, keylen);
        __sync_fetch_and_sub(&stats.num_keys_stored, 1);
    } else {
        LOG(ctx, "warning", "failed to restore key=%.*s from RocksDB[%d]: %s",
            (int)keylen, keyname, dbnum,
            reply ? (ValkeyModule_CallReplyStringPtr(reply, NULL)
                         ? ValkeyModule_CallReplyStringPtr(reply, NULL)
                         : "no reply")
                  : "no reply");
        result = -1;
    }

    if (reply) ValkeyModule_FreeCallReply(reply);
    return result;
}

// ---------------------------------------------------------------------------
// Database count discovery
// ---------------------------------------------------------------------------

// Queries `CONFIG GET databases` and returns the configured number of databases.
// Falls back to DEFAULT_NUM_DATABASES on any error.
static int ReadNumDatabases(ValkeyModuleCtx *ctx) {
    ValkeyModuleCallReply *reply =
        ValkeyModule_Call(ctx, "CONFIG", "cc", "GET", "databases");

    if (!reply ||
        ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ARRAY ||
        ValkeyModule_CallReplyLength(reply) < 2) {
        ValkeyModule_Log(ctx, "warning",
            "CONFIG GET databases failed, "
            "falling back to %d databases", DEFAULT_NUM_DATABASES);
        if (reply) ValkeyModule_FreeCallReply(reply);
        return DEFAULT_NUM_DATABASES;
    }

    // Reply is ["databases", "<n>"]
    ValkeyModuleCallReply *val_reply =
        ValkeyModule_CallReplyArrayElement(reply, 1);

    size_t      len;
    const char *val_str = ValkeyModule_CallReplyStringPtr(val_reply, &len);

    int n = DEFAULT_NUM_DATABASES;
    if (val_str && len > 0) {
        n = atoi(val_str);
        if (n <= 0) {
            ValkeyModule_Log(ctx, "warning",
                "unexpected databases value '%.*s', "
                "falling back to %d", (int)len, val_str, DEFAULT_NUM_DATABASES);
            n = DEFAULT_NUM_DATABASES;
        }
    }

    ValkeyModule_FreeCallReply(reply);
    ValkeyModule_Log(ctx, "notice",
        "detected %d databases from server config", n);
    return n;
}

// ---------------------------------------------------------------------------
// Configuration parsing
// ---------------------------------------------------------------------------

int ParseModuleArgs(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    for (int i = 0; i < argc; i += 2) {
        if (i + 1 >= argc) break;

        size_t      keylen, vallen;
        const char *key   = ValkeyModule_StringPtrLen(argv[i],     &keylen);
        const char *value = ValkeyModule_StringPtrLen(argv[i + 1], &vallen);

        if (strcasecmp(key, "path") == 0) {
            if (config.path) free(config.path);
            config.path = strdup(value);
        } else if (strcasecmp(key, "max-memory") == 0 ||
                   strcasecmp(key, "max_memory") == 0) {
            config.max_memory = (size_t)atoll(value);
            if (config.max_memory < MIN_MAX_MEMORY_MB * 1024 * 1024) {
                ValkeyModule_Log(ctx, "warning",
                    "max-memory must be at least %dMB, got %zu bytes",
                    MIN_MAX_MEMORY_MB, config.max_memory);
                return VALKEYMODULE_ERR;
            }
        } else if (strcasecmp(key, "cleanup-interval") == 0 ||
                   strcasecmp(key, "cleanup_interval") == 0) {
            config.cleanup_interval = atoi(value);
            if (config.cleanup_interval < 0) {
                ValkeyModule_Log(ctx, "warning",
                    "cleanup-interval must be non-negative, got %d",
                    config.cleanup_interval);
                return VALKEYMODULE_ERR;
            }
        }
    }

    if (!config.path) {
        ValkeyModule_Log(ctx, "warning", "'path' parameter is required");
        return VALKEYMODULE_ERR;
    }

    return VALKEYMODULE_OK;
}

// ---------------------------------------------------------------------------
// RocksDB lifecycle
// ---------------------------------------------------------------------------

// Opens a single RocksDB instance at {config.path}/db{dbnum}.
// Any existing data at that path is destroyed first so the module always
// starts with an empty store on load.
// options/roptions/woptions must already be initialised before calling this.
static int OpenOneDB(ValkeyModuleCtx *ctx, int dbnum) {
    char *path = DBPath(dbnum);
    if (!path) {
        ValkeyModule_Log(ctx, "warning",
            "failed to allocate path for db%d", dbnum);
        return VALKEYMODULE_ERR;
    }

    // Create a directory at path path
    if (mkdir(path, 0700) != 0 && errno != EEXIST) {
        ValkeyModule_Log(ctx, "warning",
            "failed to create db%d directory at %s: %s",
            dbnum, path, strerror(errno));
        free(path);
        return VALKEYMODULE_ERR;
    }

    // Wipe any leftover data from a previous run.
    char *err = NULL;
    rocksdb_destroy_db(options, path, &err);
    if (err) {
        // Not fatal — the directory may simply not exist yet.
        free(err);
        err = NULL;
    }

    dbs[dbnum] = rocksdb_open(options, path, &err);
    free(path);

    if (err != NULL) {
        ValkeyModule_Log(ctx, "warning",
            "failed to open RocksDB[%d]: %s", dbnum, err);
        free(err);
        dbs[dbnum] = NULL;
        return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

int InitRocksDB(ValkeyModuleCtx *ctx) {
    // Allocate the dbs array now that num_databases is known.
    dbs = calloc(num_databases, sizeof(rocksdb_t *));
    if (!dbs) {
        ValkeyModule_Log(ctx, "warning",
            "failed to allocate dbs array for %d databases", num_databases);
        return VALKEYMODULE_ERR;
    }

    // Memory budget: one shared block cache + per-DB write buffers.
    // block_cache is shared across all instances (same rocksdb_cache_t pointer).
    // write_buffer_size is stored in options and applies per DB instance.
    size_t block_cache_size  = 8 * 1024 * 1024;  // 8MB shared block cache
    size_t per_db_memory     = config.max_memory / num_databases;
    size_t write_buffer_size = (per_db_memory * 2) / 3;
    // Remaining ~1/3 per DB covers indexes, bloom filters, and other structures.

    LOG(ctx, "debug",
        "initializing %d RocksDB instances under path=%s, "
        "block_cache=%zuMB (shared), write_buffer=%zuMB/db",
        num_databases, config.path,
        block_cache_size / (1024 * 1024),
        write_buffer_size / (1024 * 1024));

    options = rocksdb_options_create();

    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_options_set_error_if_exists(options, 0);

    // Disable paranoid checks — trades some safety for better write performance
    rocksdb_options_set_paranoid_checks(options, 0);

    // Snappy compression — reduces disk usage for evicted data (2-4x typical)
    rocksdb_options_set_compression(options, rocksdb_snappy_compression);

    // Write buffer size per DB — larger buffers reduce write amplification
    rocksdb_options_set_write_buffer_size(options, write_buffer_size);

    // 3 concurrent memtables for smoother write flow during bursts
    rocksdb_options_set_max_write_buffer_number(options, 3);

    // Limit file handles per DB instance
    rocksdb_options_set_max_open_files(options, 500);

    // Optimize for point lookups — critical for restoring individual keys on cache miss
    rocksdb_options_optimize_for_point_lookup(options, 64);

    rocksdb_options_set_allow_mmap_reads(options, 0);
    rocksdb_options_set_allow_mmap_writes(options, 0);

    // Limit background threads to avoid CPU contention with Valkey
    rocksdb_options_set_max_background_compactions(options, 2);

    // Dynamic level bytes — reduces space amplification
    rocksdb_options_set_level_compaction_dynamic_level_bytes(options, 1);

    // 64MB SST files balance compaction overhead vs. number of files
    rocksdb_options_set_target_file_size_base(options, 64 * 1024 * 1024);

    rocksdb_block_based_table_options_t *table_options =
        rocksdb_block_based_options_create();

    // Shared block cache across all DB instances
    rocksdb_cache_t *cache = rocksdb_cache_create_lru(block_cache_size);
    rocksdb_block_based_options_set_block_cache(table_options, cache);

    // Bloom filter — ~1% false positive rate, avoids unnecessary disk reads
    rocksdb_filterpolicy_t *filter_policy =
        rocksdb_filterpolicy_create_bloom(10);
    rocksdb_block_based_options_set_filter_policy(table_options, filter_policy);

    rocksdb_block_based_options_set_block_size(table_options, 4 * 1024);
    rocksdb_block_based_options_set_block_restart_interval(table_options, 16);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(table_options, 1);
    rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(
        table_options, 1);

    rocksdb_options_set_block_based_table_factory(options, table_options);
    rocksdb_block_based_options_destroy(table_options);

    roptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(roptions, 0);
    rocksdb_readoptions_set_fill_cache(roptions, 1);

    woptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(woptions, 0);
    rocksdb_writeoptions_disable_WAL(woptions, 0);

    // Open one RocksDB instance per database
    for (int i = 0; i < num_databases; i++) {
        if (OpenOneDB(ctx, i) != VALKEYMODULE_OK) {
            for (int j = 0; j < i; j++) {
                if (dbs[j]) { rocksdb_close(dbs[j]); dbs[j] = NULL; }
            }
            free(dbs);                              dbs      = NULL;
            rocksdb_options_destroy(options);       options  = NULL;
            rocksdb_readoptions_destroy(roptions);  roptions = NULL;
            rocksdb_writeoptions_destroy(woptions); woptions = NULL;
            return VALKEYMODULE_ERR;
        }
    }

    ValkeyModule_Log(ctx, "notice",
        "RocksDB opened %d instances (shared block_cache=%zuMB, "
        "write_buffer=%zuMB/db)",
        num_databases,
        block_cache_size / (1024 * 1024),
        write_buffer_size / (1024 * 1024));

    return VALKEYMODULE_OK;
}

void CleanupRocksDB(void) {
    if (dbs) {
        for (int i = 0; i < num_databases; i++) {
            if (dbs[i]) { rocksdb_close(dbs[i]); dbs[i] = NULL; }
        }
        free(dbs);
        dbs = NULL;
    }
    if (options)  { rocksdb_options_destroy(options);       options  = NULL; }
    if (roptions) { rocksdb_readoptions_destroy(roptions);  roptions = NULL; }
    if (woptions) { rocksdb_writeoptions_destroy(woptions); woptions = NULL; }
    if (config.path) { free(config.path); config.path = NULL; }
}

// ---------------------------------------------------------------------------
// Keyspace event handlers
// ---------------------------------------------------------------------------

int PremissNotification(ValkeyModuleCtx *ctx, int type,
                        const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);

    if (!event || !key) return 0;
    if (strcmp(event, "premiss") != 0) return 0;

    int dbnum = ValkeyModule_GetSelectedDb(ctx);
    if (dbnum < 0 || dbnum >= num_databases || !dbs[dbnum]) return 0;

    size_t      keylen;
    const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

    LOG(ctx, "notice", "premiss db=%d key=%.*s (len=%zu)",
        dbnum, (int)keylen, keyname, keylen);

    RestoreKeyFromDB(ctx, dbnum, key, keyname, keylen);
    return 0;
}

int PreevictionKeyNotification(ValkeyModuleCtx *ctx, int type,
                               const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);

    if (!event || !key) return 0;
    if (strcmp(event, "preeviction") != 0) return 0;

    int dbnum = ValkeyModule_GetSelectedDb(ctx);
    if (dbnum < 0 || dbnum >= num_databases || !dbs[dbnum]) return 0;

    size_t      keylen;
    const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

    LOG(ctx, "notice", "preeviction db=%d key=%.*s (len=%zu)",
        dbnum, (int)keylen, keyname, keylen);

    ValkeyModuleCallReply *dump_reply = ValkeyModule_Call(ctx, "DUMP", "s", key);
    if (!dump_reply ||
        ValkeyModule_CallReplyType(dump_reply) != VALKEYMODULE_REPLY_STRING) {
        LOG(ctx, "warning",
            "DUMP failed for key=%.*s in db=%d, reply_type=%d",
            (int)keylen, keyname, dbnum,
            dump_reply ? ValkeyModule_CallReplyType(dump_reply) : -1);
        if (dump_reply) ValkeyModule_FreeCallReply(dump_reply);
        return 0;
    }

    size_t      dump_len;
    const char *dump_data = ValkeyModule_CallReplyStringPtr(dump_reply, &dump_len);
    if (!dump_data) {
        ValkeyModule_FreeCallReply(dump_reply);
        return 0;
    }

    LOG(ctx, "debug", "DUMP ok for key=%.*s in db=%d, dump_len=%zu",
        (int)keylen, keyname, dbnum, dump_len);

    ValkeyModuleCallReply *ttl_reply = ValkeyModule_Call(ctx, "PTTL", "s", key);
    int64_t expiry_time_ms = -1;

    if (ttl_reply &&
        ValkeyModule_CallReplyType(ttl_reply) == VALKEYMODULE_REPLY_INTEGER) {
        int64_t pttl = ValkeyModule_CallReplyInteger(ttl_reply);
        if (pttl > 0) {
            expiry_time_ms = (int64_t)time(NULL) * 1000 + pttl;
            LOG(ctx, "debug",
                "PTTL for key=%.*s in db=%d is %lld ms, expiry=%lld",
                (int)keylen, keyname, dbnum,
                (long long)pttl, (long long)expiry_time_ms);
        } else {
            expiry_time_ms = pttl; // -1 (no expiry) or -2 (key gone)
        }
    }

    size_t total_len    = sizeof(int64_t) + dump_len;
    char  *combined_data = malloc(total_len);
    if (!combined_data) {
        LOG(ctx, "warning",
            "malloc failed for key=%.*s in db=%d, total_len=%zu",
            (int)keylen, keyname, dbnum, total_len);
        ValkeyModule_FreeCallReply(dump_reply);
        if (ttl_reply) ValkeyModule_FreeCallReply(ttl_reply);
        return 0;
    }

    memcpy(combined_data,                    &expiry_time_ms, sizeof(int64_t));
    memcpy(combined_data + sizeof(int64_t),  dump_data,       dump_len);

    // Determine if this is a new key in RocksDB[dbnum]
    size_t existing_len;
    char  *existing_err = NULL;
    char  *existing_val = rocksdb_get(dbs[dbnum], roptions,
                                      keyname, keylen,
                                      &existing_len, &existing_err);
    int is_new_key = (existing_val == NULL && existing_err == NULL);
    if (existing_val) free(existing_val);
    if (existing_err) free(existing_err);

    char *err = NULL;
    rocksdb_put(dbs[dbnum], woptions, keyname, keylen,
                combined_data, total_len, &err);
    if (err != NULL) {
        LOG(ctx, "warning",
            "failed to persist key=%.*s in db=%d: %s",
            (int)keylen, keyname, dbnum, err);
        free(err);
    } else {
        __sync_fetch_and_add(&stats.total_keys_written, 1);
        __sync_fetch_and_add(&stats.total_bytes_written, total_len);
        if (is_new_key) {
            __sync_fetch_and_add(&stats.num_keys_stored, 1);
        }
        LOG(ctx, "notice",
            "stored key=%.*s in db=%d, size=%zu, expiry=%lld ms",
            (int)keylen, keyname, dbnum, total_len, (long long)expiry_time_ms);
    }

    free(combined_data);
    ValkeyModule_FreeCallReply(dump_reply);
    if (ttl_reply) ValkeyModule_FreeCallReply(ttl_reply);

    return 0;
}

// ---------------------------------------------------------------------------
// spill.restore command
// ---------------------------------------------------------------------------

int RestoreCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);

    int dbnum = ValkeyModule_GetSelectedDb(ctx);
    if (dbnum < 0 || dbnum >= num_databases || !dbs[dbnum]) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    size_t      keylen;
    const char *keyname = ValkeyModule_StringPtrLen(argv[1], &keylen);

    if (!keyname || keylen == 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Invalid key data");
    }

    LOG(ctx, "debug", "restore command db=%d key=%.*s", dbnum, (int)keylen, keyname);

    int result = RestoreKeyFromDB(ctx, dbnum, argv[1], keyname, keylen);

    switch (result) {
        case  0: return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        case -2: return ValkeyModule_ReplyWithNull(ctx);
        case -3: return ValkeyModule_ReplyWithError(ctx, "ERR Key has expired");
        default: return ValkeyModule_ReplyWithError(ctx, ERR_CORRUPTED_DATA);
    }
}

// ---------------------------------------------------------------------------
// Cleanup thread
// ---------------------------------------------------------------------------

static void *CleanupThreadFunc(void *arg) {
    VALKEYMODULE_NOT_USED(arg);

    ValkeyModule_Log(module_ctx, "notice",
        "cleanup thread started with interval=%d seconds",
        config.cleanup_interval);

    while (cleanup_thread_running) {
        for (int i = 0; i < config.cleanup_interval && cleanup_thread_running; i++) {
            sleep(1);
        }
        if (!cleanup_thread_running) break;
        if (config.cleanup_interval <= 0) continue;

        ValkeyModule_Log(module_ctx, "debug", "starting periodic cleanup");
        uint64_t removed = PerformCleanup();
        ValkeyModule_Log(module_ctx, "debug",
            "periodic cleanup removed %llu keys", (unsigned long long)removed);
    }

    ValkeyModule_Log(module_ctx, "notice", "cleanup thread stopped");
    return NULL;
}

// ---------------------------------------------------------------------------
// Key counting
// ---------------------------------------------------------------------------

// Counts active (non-expired) keys across all RocksDB instances.
static uint64_t CountActiveKeys(void) {
    uint64_t active_keys     = 0;
    int64_t  current_time_ms = (int64_t)time(NULL) * 1000;

    for (int i = 0; i < num_databases; i++) {
        if (!dbs[i]) continue;

        rocksdb_iterator_t *iter = rocksdb_create_iterator(dbs[i], roptions);
        rocksdb_iter_seek_to_first(iter);

        while (rocksdb_iter_valid(iter)) {
            size_t      vallen;
            const char *val = rocksdb_iter_value(iter, &vallen);

            if (val && vallen >= sizeof(int64_t)) {
                int64_t ttl_ms;
                memcpy(&ttl_ms, val, sizeof(int64_t));
                if (ttl_ms <= 0 || ttl_ms >= current_time_ms) active_keys++;
            }
            rocksdb_iter_next(iter);
        }

        char *err = NULL;
        rocksdb_iter_get_error(iter, &err);
        rocksdb_iter_destroy(iter);
        if (err) free(err);
    }

    return active_keys;
}

// ---------------------------------------------------------------------------
// Periodic cleanup
// ---------------------------------------------------------------------------

// Removes expired keys from all RocksDB instances.
// Returns number of keys removed.
static uint64_t PerformCleanup(void) {
    pthread_rwlock_rdlock(&db_rwlock);

    uint64_t num_keys_scanned = 0;
    uint64_t num_keys_cleaned = 0;
    int64_t  current_time_ms  = (int64_t)time(NULL) * 1000;

    for (int i = 0; i < num_databases; i++) {
        if (!dbs[i]) continue;

        rocksdb_iterator_t *iter = rocksdb_create_iterator(dbs[i], roptions);
        rocksdb_iter_seek_to_first(iter);

        while (rocksdb_iter_valid(iter) && cleanup_thread_running) {
            size_t      keylen, vallen;
            const char *keyname = rocksdb_iter_key(iter, &keylen);
            const char *val     = rocksdb_iter_value(iter, &vallen);

            num_keys_scanned++;

            if (val && vallen >= sizeof(int64_t)) {
                int64_t ttl_ms;
                memcpy(&ttl_ms, val, sizeof(int64_t));

                if (ttl_ms > 0 && ttl_ms < current_time_ms) {
                    DeleteKeyFromDB(i, keyname, keylen);
                    num_keys_cleaned++;
                    __sync_fetch_and_sub(&stats.num_keys_stored, 1);
                    LOG(module_ctx, "debug",
                        "expired key removed from db=%d: %.*s "
                        "(ttl=%lld, now=%lld)",
                        i, (int)keylen, keyname,
                        (long long)ttl_ms, (long long)current_time_ms);
                }
            }

            rocksdb_iter_next(iter);
        }

        char *err = NULL;
        rocksdb_iter_get_error(iter, &err);
        rocksdb_iter_destroy(iter);
        if (err) {
            LOG(module_ctx, "warning",
                "cleanup iteration error on db%d: %s", i, err);
            free(err);
        }
    }

    __sync_fetch_and_add(&stats.total_keys_cleaned, num_keys_cleaned);
    stats.last_num_keys_cleaned = num_keys_cleaned;
    stats.last_cleanup_at       = (int64_t)time(NULL);

    LOG(module_ctx, "debug",
        "cleanup completed: checked=%llu, removed=%llu",
        (unsigned long long)num_keys_scanned,
        (unsigned long long)num_keys_cleaned);

    pthread_rwlock_unlock(&db_rwlock);
    return num_keys_cleaned;
}

// ---------------------------------------------------------------------------
// FLUSHDB / FLUSHALL handler
// ---------------------------------------------------------------------------

// Destroys and recreates a single RocksDB instance.
// Must be called with db_rwlock write-locked.
static void TruncateOneDB(ValkeyModuleCtx *ctx, int dbnum) {
    if (dbnum < 0 || dbnum >= num_databases) return;

    char *path = DBPath(dbnum);
    if (!path) {
        ValkeyModule_Log(ctx, "warning",
            "failed to allocate path for db%d during truncate", dbnum);
        return;
    }

    if (dbs[dbnum]) {
        rocksdb_close(dbs[dbnum]);
        dbs[dbnum] = NULL;
    }

    char *err = NULL;
    rocksdb_destroy_db(options, path, &err);
    if (err) {
        ValkeyModule_Log(ctx, "warning",
            "failed to destroy RocksDB[%d]: %s", dbnum, err);
        free(err);
        err = NULL;
    }

    dbs[dbnum] = rocksdb_open(options, path, &err);
    free(path);

    if (err) {
        ValkeyModule_Log(ctx, "warning",
            "failed to reopen RocksDB[%d] after truncate: %s", dbnum, err);
        free(err);
        dbs[dbnum] = NULL;
    }
}

// Truncates a specific DB (FLUSHDB) or all DBs (FLUSHALL when dbnum == -1).
// Acquires the write lock so the cleanup thread cannot use any DB concurrently.
static void TruncateRocksDB(ValkeyModuleCtx *ctx, int dbnum) {
    pthread_rwlock_wrlock(&db_rwlock);

    if (dbnum == -1) {
        // FLUSHALL — wipe every database
        for (int i = 0; i < num_databases; i++) {
            TruncateOneDB(ctx, i);
        }
        stats.num_keys_stored = 0;
        ValkeyModule_Log(ctx, "notice",
            "all %d RocksDB instances wiped (FLUSHALL)", num_databases);
    } else if (dbnum >= 0 && dbnum < num_databases) {
        // FLUSHDB — wipe only the specified database
        TruncateOneDB(ctx, dbnum);
        ValkeyModule_Log(ctx, "notice",
            "RocksDB[%d] wiped (FLUSHDB)", dbnum);
    }

    pthread_rwlock_unlock(&db_rwlock);

    // Recount active keys for FLUSHDB (FLUSHALL already reset to 0 above).
    if (dbnum >= 0 && dbnum < num_databases) {
        stats.num_keys_stored = CountActiveKeys();
    }
}

// Server event callback fired on FLUSHDB / FLUSHALL.
static void FlushDBHandler(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                           uint64_t subevent, void *data) {
    VALKEYMODULE_NOT_USED(eid);

    // Act at END — Valkey has already cleared its in-memory data.
    if (subevent != VALKEYMODULE_SUBEVENT_FLUSHDB_END) return;

    ValkeyModuleFlushInfo *fi    = (ValkeyModuleFlushInfo *)data;
    int                   dbnum = fi ? fi->dbnum : -1;

    ValkeyModule_Log(ctx, "notice",
        "FLUSH event received (dbnum=%d), wiping RocksDB", dbnum);

    TruncateRocksDB(ctx, dbnum);
}

// ---------------------------------------------------------------------------
// spill.cleanup command
// ---------------------------------------------------------------------------

int CleanupCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    int any_open = 0;
    for (int i = 0; i < num_databases; i++) {
        if (dbs[i]) { any_open = 1; break; }
    }
    if (!any_open) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    uint64_t num_keys_scanned = 0;
    uint64_t num_keys_cleaned = 0;
    int64_t  current_time_ms  = (int64_t)time(NULL) * 1000;

    for (int i = 0; i < num_databases; i++) {
        if (!dbs[i]) continue;

        rocksdb_iterator_t *iter = rocksdb_create_iterator(dbs[i], roptions);
        rocksdb_iter_seek_to_first(iter);

        while (rocksdb_iter_valid(iter)) {
            size_t      keylen, vallen;
            const char *keyname = rocksdb_iter_key(iter, &keylen);
            const char *val     = rocksdb_iter_value(iter, &vallen);

            num_keys_scanned++;

            if (val && vallen >= sizeof(int64_t)) {
                int64_t ttl_ms;
                memcpy(&ttl_ms, val, sizeof(int64_t));

                if (ttl_ms > 0 && ttl_ms < current_time_ms) {
                    DeleteKeyFromDB(i, keyname, keylen);
                    num_keys_cleaned++;
                    __sync_fetch_and_sub(&stats.num_keys_stored, 1);
                    LOG(ctx, "debug",
                        "expired key removed from db=%d: %.*s "
                        "(ttl=%lld, now=%lld)",
                        i, (int)keylen, keyname,
                        (long long)ttl_ms, (long long)current_time_ms);
                }
            }

            rocksdb_iter_next(iter);
        }

        char *err = NULL;
        rocksdb_iter_get_error(iter, &err);
        rocksdb_iter_destroy(iter);

        if (err != NULL) {
            LOG(ctx, "warning",
                "cleanup iteration error on db%d: %s", i, err);
            ValkeyModule_ReplyWithError(ctx, err);
            free(err);
            return VALKEYMODULE_OK;
        }
    }

    __sync_fetch_and_add(&stats.total_keys_cleaned, num_keys_cleaned);
    stats.last_num_keys_cleaned = num_keys_cleaned;
    stats.last_cleanup_at       = (int64_t)time(NULL);

    LOG(ctx, "debug", "cleanup command completed: checked=%llu, removed=%llu",
        (unsigned long long)num_keys_scanned,
        (unsigned long long)num_keys_cleaned);

    ValkeyModule_ReplyWithArray(ctx, 4);
    ValkeyModule_ReplyWithSimpleString(ctx, "num_keys_scanned");
    ValkeyModule_ReplyWithLongLong(ctx, num_keys_scanned);
    ValkeyModule_ReplyWithSimpleString(ctx, "num_keys_cleaned");
    ValkeyModule_ReplyWithLongLong(ctx, num_keys_cleaned);

    return VALKEYMODULE_OK;
}

// ---------------------------------------------------------------------------
// INFO callback
// ---------------------------------------------------------------------------

void SpillInfoFunc(ValkeyModuleInfoCtx *ctx, int for_crash_report) {
    VALKEYMODULE_NOT_USED(for_crash_report);

    ValkeyModule_InfoAddSection(ctx, "stats");
    ValkeyModule_InfoAddFieldULongLong(ctx, "num_keys_stored",       stats.num_keys_stored);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_keys_written",    stats.total_keys_written);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_keys_restored",   stats.total_keys_restored);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_keys_cleaned",    stats.total_keys_cleaned);
    ValkeyModule_InfoAddFieldULongLong(ctx, "last_num_keys_cleaned", stats.last_num_keys_cleaned);
    ValkeyModule_InfoAddFieldLongLong (ctx, "last_cleanup_at",       stats.last_cleanup_at);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_bytes_written",   stats.total_bytes_written);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_bytes_read",      stats.total_bytes_read);

    ValkeyModule_InfoAddSection(ctx, "config");
    ValkeyModule_InfoAddFieldCString  (ctx, "path",                  config.path ? config.path : "");
    ValkeyModule_InfoAddFieldULongLong(ctx, "max_memory_bytes",      config.max_memory);
    ValkeyModule_InfoAddFieldLongLong (ctx, "cleanup_interval_seconds", config.cleanup_interval);
}

// ---------------------------------------------------------------------------
// Module load / unload
// ---------------------------------------------------------------------------

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "spill", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    module_ctx = ValkeyModule_GetDetachedThreadSafeContext(ctx);

    LOG(ctx, "notice", "loading module with %d arguments", argc);

    if (ParseModuleArgs(ctx, argv, argc) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "failed to parse module arguments");
        return VALKEYMODULE_ERR;
    }

    num_databases = ReadNumDatabases(ctx);

    if (InitRocksDB(ctx) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "failed to initialize RocksDB instances");
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    // All DB instances were wiped clean on open — start from zero.
    stats.num_keys_stored = 0;
    ValkeyModule_Log(ctx, "notice",
        "started fresh across %d RocksDB instances", num_databases);

    if (ValkeyModule_SubscribeToKeyspaceEvents(
            ctx, VALKEYMODULE_NOTIFY_PREEVICTION,
            PreevictionKeyNotification) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_SubscribeToKeyspaceEvents(
            ctx, VALKEYMODULE_NOTIFY_PREMISS,
            PremissNotification) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_SubscribeToServerEvent(
            ctx, ValkeyModuleEvent_FlushDB,
            FlushDBHandler) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning",
            "failed to subscribe to FlushDB server event");
        // Not fatal — continue without FLUSH support
    }

    if (ValkeyModule_CreateCommand(
            ctx, "spill.restore", RestoreCommand, "write", 1, 1, 1)
            == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(
            ctx, "spill.cleanup", CleanupCommand, "write", 0, 0, 0)
            == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_RegisterInfoFunc(ctx, SpillInfoFunc) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "failed to register info callback");
        // Not fatal
    }

    if (config.cleanup_interval > 0) {
        cleanup_thread_running = 1;
        if (pthread_create(&cleanup_thread, NULL, CleanupThreadFunc, NULL) != 0) {
            ValkeyModule_Log(ctx, "warning", "failed to create cleanup thread");
            cleanup_thread_running = 0;
        } else {
            ValkeyModule_Log(ctx, "notice",
                "cleanup thread started with interval=%d seconds",
                config.cleanup_interval);
        }
    } else {
        ValkeyModule_Log(ctx, "notice",
            "periodic cleanup disabled (cleanup_interval=0)");
    }

    ValkeyModule_Log(ctx, "notice",
        "module loaded successfully, path=%s, max_memory=%zuMB, "
        "cleanup_interval=%ds, num_databases=%d",
        config.path, config.max_memory / (1024 * 1024),
        config.cleanup_interval, num_databases);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    ValkeyModule_Log(ctx, "notice",
        "unloading module, stats: stored=%llu restored=%llu cleaned=%llu",
        (unsigned long long)stats.num_keys_stored,
        (unsigned long long)stats.total_keys_restored,
        (unsigned long long)stats.total_keys_cleaned);

    if (cleanup_thread_running) {
        ValkeyModule_Log(ctx, "notice", "stopping cleanup thread");
        cleanup_thread_running = 0;
        pthread_join(cleanup_thread, NULL);
        ValkeyModule_Log(ctx, "notice", "cleanup thread stopped");
    }

    if (module_ctx) {
        ValkeyModule_FreeThreadSafeContext(module_ctx);
        module_ctx = NULL;
    }
    CleanupRocksDB();
    return VALKEYMODULE_OK;
}
