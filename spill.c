#include "valkeymodule.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <pthread.h>

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define MIN_MAX_MEMORY_MB 20  // Minimum max-memory limit: 20MB

// Logging control: set to 1 to enable debug/verbose logging, 0 to disable
#define SPILL_DEBUG 0

#if SPILL_DEBUG
#define LOG(ctx, level, ...) ValkeyModule_Log(ctx, level, __VA_ARGS__)
#else
#define LOG(ctx, level, ...) (void)0
#endif

static rocksdb_t *db = NULL;
static rocksdb_options_t *options = NULL;
static rocksdb_readoptions_t *roptions = NULL;
static rocksdb_writeoptions_t *woptions = NULL;
static ValkeyModuleCtx *module_ctx = NULL;

typedef struct {
    uint64_t num_keys_stored;      // Active keys in RocksDB (dynamically tracked: init at load, +1 on new key write, -1 on restore/cleanup)
    uint64_t total_keys_written;   // Total write operations since server restart (cumulative, includes overwrites)
    uint64_t total_keys_restored;  // Keys restored since server restart
    uint64_t total_keys_cleaned;   // Cumulative keys cleaned since server restart (manual or automatic cleanup)
    uint64_t last_num_keys_cleaned; // Keys cleaned in most recent cleanup job (manual or automatic)
    int64_t last_cleanup_at;       // Unix timestamp (seconds) of last cleanup, 0 if never run
    uint64_t total_bytes_written;  // Bytes written since server restart (includes metadata)
    uint64_t total_bytes_read;     // Bytes read since server restart (includes metadata)
} SpillStats;

static SpillStats stats = {0};

static const char *ERR_DB_NOT_INIT = "ERR RocksDB not initialized";
static const char *ERR_CORRUPTED_DATA = "ERR Corrupted data in RocksDB";

typedef struct {
    char *path;
    size_t max_memory;  // Maximum memory budget for RocksDB (block cache + write buffers)
    int cleanup_interval;  // Periodic cleanup interval in seconds
} SpillConfig;

static SpillConfig config = {
    .path = NULL,
    .max_memory = 256 * 1024 * 1024,  // Default: 256MB total memory budget
    .cleanup_interval = 300  // Default: 5 minutes (300 seconds)
};

// Cleanup thread management
static pthread_t cleanup_thread;
static volatile int cleanup_thread_running = 0;

// Forward declarations
static uint64_t PerformCleanup();

static void DeleteKeyFromDB(const char *keyname, size_t keylen) {
    if (!db) return;

    char *err = NULL;
    rocksdb_delete(db, woptions, keyname, keylen, &err);
    if (err) {
        LOG(module_ctx, "warning", "spill: failed to delete key from RocksDB: %.*s, error: %s",
            (int)keylen, keyname, err);
        free(err);
    } else {
        LOG(module_ctx, "debug", "spill: deleted key from RocksDB: %.*s", (int)keylen, keyname);
    }
}

// Helper function to restore a key from RocksDB
// Returns: 0 on success, -1 on error, -2 if key not found, -3 if expired
static int RestoreKeyFromDB(ValkeyModuleCtx *ctx, ValkeyModuleString *key, const char *keyname, size_t keylen) {
    if (!db) return -1;

    char *err = NULL;
    size_t vallen;
    char *val = rocksdb_get(db, roptions, keyname, keylen, &vallen, &err);

    if (err != NULL) {
        LOG(ctx, "warning", "spill: error getting key=%.*s from RocksDB: %s", (int)keylen, keyname, err);
        free(err);
        return -1;
    }

    if (val == NULL) {
        LOG(ctx, "notice", "spill: key=%.*s not found not in RocksDB", (int)keylen, keyname);
        return -2;
    }

    LOG(ctx, "notice", "spill: key=%.*s found in RocksDB with value_len=%zu bytes", (int)keylen, keyname, vallen);

    // Validate data format
    if (vallen < sizeof(int64_t)) {
        free(val);
        return -1;
    }

    char *ptr = val;
    int64_t expiry_time_ms;
    memcpy(&expiry_time_ms, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);

    // Check if key has expired
    if (expiry_time_ms > 0) {
        int64_t current_time_ms = (int64_t)time(NULL) * 1000;
        if (expiry_time_ms <= current_time_ms) {
            LOG(ctx, "notice", "spill: key=%.*s has expired (expiry=%lld, now=%lld), deleting",
                (int)keylen, keyname, (long long)expiry_time_ms, (long long)current_time_ms);
            DeleteKeyFromDB(keyname, keylen);
            free(val);
            return -3;
        }
    }

    size_t dump_len = vallen - sizeof(int64_t);
    ValkeyModuleString *dump_data = ValkeyModule_CreateString(ctx, ptr, dump_len);

    // Convert absolute expiry time back to relative TTL for RESTORE
    ValkeyModuleCallReply *reply;
    if (expiry_time_ms > 0) {
        int64_t current_time_ms = (int64_t)time(NULL) * 1000;
        int64_t ttl_ms = expiry_time_ms - current_time_ms;
        if (ttl_ms < 0) ttl_ms = 1;  // Minimum 1ms TTL
        LOG(ctx, "debug", "spill: restoring key=%.*s with TTL=%lld ms", (int)keylen, keyname, (long long)ttl_ms);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, ttl_ms, dump_data, "REPLACE");
    } else {
        LOG(ctx, "debug", "spill: restoring key=%.*s with no expiry", (int)keylen, keyname);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, 0, dump_data, "REPLACE");
    }

    free(val);
    ValkeyModule_FreeString(ctx, dump_data);

    int result = 0;
    if (reply && ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ERROR) {
        LOG(ctx, "debug", "spill: key=%.*s restored successfully", (int)keylen, keyname);
        __sync_fetch_and_add(&stats.total_keys_restored, 1);
        __sync_fetch_and_add(&stats.total_bytes_read, vallen);
        DeleteKeyFromDB(keyname, keylen);
        __sync_fetch_and_sub(&stats.num_keys_stored, 1);
    } else {
        LOG(ctx, "warning", "spill: failed to restore key=%.*s: %s", (int)keylen, keyname,
            reply ? (ValkeyModule_CallReplyStringPtr(reply, NULL) ? ValkeyModule_CallReplyStringPtr(reply, NULL) : "no reply") : "no reply");
        result = -1;
    }

    if (reply) {
        ValkeyModule_FreeCallReply(reply);
    }

    return result;
}

int ParseModuleArgs(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    for (int i = 0; i < argc; i += 2) {
        if (i + 1 >= argc) break;

        size_t keylen, vallen;
        const char *key = ValkeyModule_StringPtrLen(argv[i], &keylen);
        const char *value = ValkeyModule_StringPtrLen(argv[i + 1], &vallen);

        if (strcasecmp(key, "path") == 0) {
            if (config.path) free(config.path);
            config.path = strdup(value);
        } else if (strcasecmp(key, "max-memory") == 0 || strcasecmp(key, "max_memory") == 0) {
            config.max_memory = (size_t)atoll(value);
            if (config.max_memory < MIN_MAX_MEMORY_MB * 1024 * 1024) {
                ValkeyModule_Log(ctx, "warning", "max-memory must be at least %dMB, got %zu bytes", MIN_MAX_MEMORY_MB, config.max_memory);
                return VALKEYMODULE_ERR;
            }
        } else if (strcasecmp(key, "cleanup-interval") == 0 || strcasecmp(key, "cleanup_interval") == 0) {
            config.cleanup_interval = atoi(value);
            if (config.cleanup_interval < 0) {
                ValkeyModule_Log(ctx, "warning", "cleanup-interval must be non-negative, got %d", config.cleanup_interval);
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

int InitRocksDB(ValkeyModuleCtx *ctx) {
    char *err = NULL;
    LOG(ctx, "debug", "spill: initializing RocksDB at path=%s with max_memory=%zu bytes",
        config.path, config.max_memory);

    // Calculate memory distribution from max_memory budget
    size_t block_cache_size = 8 * 1024 * 1024;              // 8MB for block cache (read cache)
    size_t remaining_memory = config.max_memory - block_cache_size;
    size_t write_buffer_size = (remaining_memory * 2) / 3;  // 2/3 of remaining for write buffers
    // Remaining 1/3 is used for indexes, bloom filters, and other structures

    options = rocksdb_options_create();

    // Create DB if missing and don't error if exists
    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_options_set_error_if_exists(options, 0);

    // When enabled, RocksDB becomes much stricter about detecting corruption or unexpected errors.
    // Verifies checksums more strictly
    // Fails fast on file corruption
    // Disable paranoid checks - trades some safety for better write performance in eviction path
    rocksdb_options_set_paranoid_checks(options, 0);

    // Enable Snappy compression - reduces disk usage for evicted data (2-4x compression typical)
    rocksdb_options_set_compression(options, rocksdb_snappy_compression);

    // Memtable Configuration
    // Write buffer size - larger buffers reduce write amplification but use more memory
    rocksdb_options_set_write_buffer_size(options, write_buffer_size);

    // Max write buffers - allows 3 concurrent memtables for smoother write flow during bursts
    rocksdb_options_set_max_write_buffer_number(options, 3);

    // Max open files - limits file handles to prevent exhausting system resources
    rocksdb_options_set_max_open_files(options, 1000);

    // Optimize for point lookups - critical since we restore individual keys on cache miss
    // make single-key lookups (Get) faster and cheaper.
    // takes block_cache_size as parameter to size internal data structures accordingly.
    rocksdb_options_optimize_for_point_lookup(options, 64);

    // Disable mmap for reads and writes to reduce memory overhead.
    rocksdb_options_set_allow_mmap_reads(options, 0);
    rocksdb_options_set_allow_mmap_writes(options, 0);

    // Background compactions - limits to 2 threads to avoid CPU contention with Redis/Valkey
    rocksdb_options_set_max_background_compactions(options, 2);

    // Dynamic level bytes - reduces space amplification by adjusting level sizes dynamically
    rocksdb_options_set_level_compaction_dynamic_level_bytes(options, 1);

    // SST file size - 64MB strikes balance between compaction overhead and number of files
    rocksdb_options_set_target_file_size_base(options, 64 * 1024 * 1024);

    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();

    // Block cache - crucial for read performance when restoring evicted keys
    rocksdb_cache_t *cache = rocksdb_cache_create_lru(block_cache_size);
    rocksdb_block_based_options_set_block_cache(table_options, cache);

    // Bloom filter - reduces unnecessary disk reads by filtering out non-existent keys (10 bits = ~1% false positive rate)
    rocksdb_filterpolicy_t *filter_policy = rocksdb_filterpolicy_create_bloom(10);
    rocksdb_block_based_options_set_filter_policy(table_options, filter_policy);

    // Block size - 4KB matches typical filesystem/SSD page size for efficient I/O
    rocksdb_block_based_options_set_block_size(table_options, 4 * 1024);

    // Block restart interval - affects prefix compression (lower = better compression, higher = faster lookups)
    rocksdb_block_based_options_set_block_restart_interval(table_options, 16);

    // Cache index and filter blocks - keeps metadata in memory for faster lookups
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(table_options, 1);

    // Pin L0 filters in cache - L0 is most frequently accessed, pinning improves read latency
    rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(table_options, 1);

    rocksdb_options_set_block_based_table_factory(options, table_options);
    rocksdb_block_based_options_destroy(table_options);

    db = rocksdb_open(options, config.path, &err);
    if (err != NULL) {
        ValkeyModule_Log(ctx, "warning", "failed to open RocksDB: %s", err);
        free(err);
        return VALKEYMODULE_ERR;
    }
    ValkeyModule_Log(ctx, "notice", "RocksDB opened successfully (block_cache=%zuMB, write_buffer=%zuMB)",
                     block_cache_size / (1024 * 1024), write_buffer_size / (1024 * 1024));

    roptions = rocksdb_readoptions_create();

    // Disable checksum verification - trades some data integrity for faster reads on cache miss
    rocksdb_readoptions_set_verify_checksums(roptions, 0);

    // Enable fill cache - ensures restored data gets cached for potential re-access
    rocksdb_readoptions_set_fill_cache(roptions, 1);

    woptions = rocksdb_writeoptions_create();

    // Async writes - don't wait for fsync, improves eviction throughput (data in OS cache)
    rocksdb_writeoptions_set_sync(woptions, 0);

    // Keep WAL enabled - provides crash recovery for evicted data (0 = WAL enabled)
    rocksdb_writeoptions_disable_WAL(woptions, 0);

    return VALKEYMODULE_OK;
}

void CleanupRocksDB(void) {
    if (db) {
        rocksdb_close(db);
        db = NULL;
    }
    if (options) {
        rocksdb_options_destroy(options);
        options = NULL;
    }
    if (roptions) {
        rocksdb_readoptions_destroy(roptions);
        roptions = NULL;
    }
    if (woptions) {
        rocksdb_writeoptions_destroy(woptions);
        woptions = NULL;
    }
    if (config.path) {
        free(config.path);
        config.path = NULL;
    }
}

int PremissNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);

    if (!event || !key || !db) {
        LOG(module_ctx, "warning", "spill: premiss called with invalid params: event=%p key=%p db=%p",
            (void*)event, (void*)key, (void*)db);
        return 0;
    }
    if (strcmp(event, "premiss") != 0) return 0;

    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

    LOG(ctx, "notice", "spill: premiss called for key=%.*s (len=%zu)", (int)keylen, keyname, keylen);
    LOG(ctx, "debug", "spill: checking RocksDB for key=%.*s", (int)keylen, keyname);

    RestoreKeyFromDB(ctx, key, keyname, keylen);
    return 0;
}

int PreevictionKeyNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);

    if (!event || !key || !db) {
        LOG(module_ctx, "warning", "spill: preeviction called with invalid params: event=%p key=%p db=%p",
            (void*)event, (void*)key, (void*)db);
        return 0;
    }
    if (strcmp(event, "preeviction") != 0) return 0;

    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

    LOG(ctx, "notice", "spill: preeviction called for key=%.*s (len=%zu)", (int)keylen, keyname, keylen);
    LOG(ctx, "debug", "spill: calling DUMP for key=%.*s", (int)keylen, keyname);

    ValkeyModuleCallReply *dump_reply = ValkeyModule_Call(ctx, "DUMP", "s", key);
    if (!dump_reply || ValkeyModule_CallReplyType(dump_reply) != VALKEYMODULE_REPLY_STRING) {
        LOG(ctx, "warning", "spill: DUMP failed for key=%.*s, reply_type=%d",
            (int)keylen, keyname, dump_reply ? ValkeyModule_CallReplyType(dump_reply) : -1);
        if (dump_reply) ValkeyModule_FreeCallReply(dump_reply);
        return 0;
    }

    size_t dump_len;
    const char *dump_data = ValkeyModule_CallReplyStringPtr(dump_reply, &dump_len);

    LOG(ctx, "debug", "spill: DUMP successful for key=%.*s, dump_len=%zu", (int)keylen, keyname, dump_len);

    if (!dump_data) {
        ValkeyModule_FreeCallReply(dump_reply);
        return 0;
    }

    LOG(ctx, "debug", "spill: calling PTTL for key=%.*s", (int)keylen, keyname);
    ValkeyModuleCallReply *ttl_reply = ValkeyModule_Call(ctx, "PTTL", "s", key);
    int64_t pttl = -1;
    int64_t expiry_time_ms = -1;  // Absolute expiry timestamp

    if (ttl_reply && ValkeyModule_CallReplyType(ttl_reply) == VALKEYMODULE_REPLY_INTEGER) {
        pttl = ValkeyModule_CallReplyInteger(ttl_reply);
        // Convert relative TTL to absolute expiry timestamp
        if (pttl > 0) {
            int64_t current_time_ms = (int64_t)time(NULL) * 1000;
            expiry_time_ms = current_time_ms + pttl;
            LOG(ctx, "debug", "spill: PTTL for key=%.*s is %lld ms, expiry_time=%lld",
                (int)keylen, keyname, (long long)pttl, (long long)expiry_time_ms);
        } else {
            // pttl is -1 (no expiry) or -2 (key doesn't exist)
            expiry_time_ms = pttl;
            LOG(ctx, "debug", "spill: PTTL for key=%.*s returned %lld (no expiry)",
                (int)keylen, keyname, (long long)pttl);
        }
    }

    size_t total_len = sizeof(int64_t) + dump_len;
    LOG(ctx, "debug", "spill: storing key=%.*s, total_key_len=%zu (expiry_header=8 + value_len=%zu)",
        (int)keylen, keyname, total_len, dump_len);

    char *combined_data = malloc(total_len);
    if (!combined_data) {
        LOG(ctx, "warning", "spill: malloc failed for key=%.*s, total_key_len=%zu", (int)keylen, keyname, total_len);
        ValkeyModule_FreeCallReply(dump_reply);
        if (ttl_reply) ValkeyModule_FreeCallReply(ttl_reply);
        return 0;
    }

    char *ptr = combined_data;
    // Store absolute expiry timestamp instead of relative TTL
    memcpy(ptr, &expiry_time_ms, sizeof(int64_t));
    ptr += sizeof(int64_t);
    memcpy(ptr, dump_data, dump_len);

    LOG(ctx, "notice", "spill: storing key=%.*s directly to RocksDB", (int)keylen, keyname);

    // Check if key already exists in RocksDB
    size_t existing_len;
    char *existing_err = NULL;
    char *existing_val = rocksdb_get(db, roptions, keyname, keylen, &existing_len, &existing_err);
    int is_new_key = (existing_val == NULL && existing_err == NULL);
    if (existing_val) free(existing_val);
    if (existing_err) free(existing_err);

    char *err = NULL;
    rocksdb_put(db, woptions, keyname, keylen, combined_data, total_len, &err);
    if (err != NULL) {
        LOG(ctx, "warning", "spill: failed to persist key=%.*s: %s", (int)keylen, keyname, err);
        free(err);
    } else {
        __sync_fetch_and_add(&stats.total_keys_written, 1);
        __sync_fetch_and_add(&stats.total_bytes_written, total_len);
        if (is_new_key) {
            __sync_fetch_and_add(&stats.num_keys_stored, 1);
        }
        LOG(ctx, "notice", "spill: stored key=%.*s (len=%zu) to RocksDB, size=%zu, expiry=%lld ms",
            (int)keylen, keyname, keylen, total_len, (long long)expiry_time_ms);
    }

    free(combined_data);
    ValkeyModule_FreeCallReply(dump_reply);
    if (ttl_reply) ValkeyModule_FreeCallReply(ttl_reply);

    return 0;
}

int RestoreCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        return ValkeyModule_WrongArity(ctx);
    }

    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(argv[1], &keylen);

    LOG(ctx, "debug", "spill: restore command for key=%.*s", (int)keylen, keyname);

    if (!keyname || keylen == 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Invalid key data");
    }

    int result = RestoreKeyFromDB(ctx, argv[1], keyname, keylen);

    switch (result) {
        case 0:
            return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        case -2:
            return ValkeyModule_ReplyWithNull(ctx);
        case -3:
            return ValkeyModule_ReplyWithError(ctx, "ERR Key has expired");
        default:
            return ValkeyModule_ReplyWithError(ctx, ERR_CORRUPTED_DATA);
    }
}

// Background thread function for periodic cleanup
static void *CleanupThreadFunc(void *arg) {
    VALKEYMODULE_NOT_USED(arg);

    ValkeyModule_Log(module_ctx, "notice", "cleanup thread started with interval=%d seconds",
                     config.cleanup_interval);

    while (cleanup_thread_running) {
        // Sleep for the configured interval (in smaller chunks to allow for clean shutdown)
        for (int i = 0; i < config.cleanup_interval && cleanup_thread_running; i++) {
            sleep(1);
        }

        if (!cleanup_thread_running) break;

        // Skip cleanup if interval is 0 (disabled)
        if (config.cleanup_interval <= 0) continue;

        ValkeyModule_Log(module_ctx, "debug", "starting periodic cleanup");
        uint64_t removed = PerformCleanup();
        ValkeyModule_Log(module_ctx, "debug", "periodic cleanup removed %llu keys",
                        (unsigned long long)removed);
    }

    ValkeyModule_Log(module_ctx, "notice", "cleanup thread stopped");
    return NULL;
}

// Counts active keys in RocksDB (excluding expired ones)
static uint64_t CountActiveKeys() {
    if (!db) return 0;

    uint64_t active_keys = 0;
    int64_t current_time_ms = (int64_t)time(NULL) * 1000;

    rocksdb_iterator_t *iter = rocksdb_create_iterator(db, roptions);
    rocksdb_iter_seek_to_first(iter);

    while (rocksdb_iter_valid(iter)) {
        size_t vallen;
        const char *val = rocksdb_iter_value(iter, &vallen);

        // Check if key is not expired
        if (val && vallen >= sizeof(int64_t)) {
            int64_t ttl_ms;
            memcpy(&ttl_ms, val, sizeof(int64_t));

            // Count keys that are either permanent (ttl_ms <= 0) or not expired
            if (ttl_ms <= 0 || ttl_ms >= current_time_ms) {
                active_keys++;
            }
        }

        rocksdb_iter_next(iter);
    }

    char *err = NULL;
    rocksdb_iter_get_error(iter, &err);
    rocksdb_iter_destroy(iter);

    if (err != NULL) {
        LOG(module_ctx, "warning", "spill: error counting keys: %s", err);
        free(err);
    }

    return active_keys;
}

// Performs cleanup of expired keys from RocksDB
// Returns number of keys removed
static uint64_t PerformCleanup() {
    if (!db) return 0;

    uint64_t num_keys_scanned = 0;
    uint64_t num_keys_cleaned = 0;
    int64_t current_time_ms = (int64_t)time(NULL) * 1000;

    rocksdb_iterator_t *iter = rocksdb_create_iterator(db, roptions);
    rocksdb_iter_seek_to_first(iter);

    while (rocksdb_iter_valid(iter) && cleanup_thread_running) {
        size_t keylen, vallen;
        const char *keyname = rocksdb_iter_key(iter, &keylen);
        const char *val = rocksdb_iter_value(iter, &vallen);

        num_keys_scanned++;

        // Check if value has TTL header
        if (val && vallen >= sizeof(int64_t)) {
            int64_t ttl_ms;
            memcpy(&ttl_ms, val, sizeof(int64_t));

            // If TTL is set (positive) and has expired
            if (ttl_ms > 0 && ttl_ms < current_time_ms) {
                // Key has expired, delete it
                DeleteKeyFromDB(keyname, keylen);
                num_keys_cleaned++;
                __sync_fetch_and_sub(&stats.num_keys_stored, 1);
                LOG(ctx, "debug", "spill: expired key removed: %.*s (ttl=%lld, now=%lld)",
                    (int)keylen, keyname, (long long)ttl_ms, (long long)current_time_ms);
            }
        }

        rocksdb_iter_next(iter);
    }

    char *err = NULL;
    rocksdb_iter_get_error(iter, &err);
    rocksdb_iter_destroy(iter);

    if (err != NULL) {
        LOG(ctx, "warning", "spill: cleanup iteration error: %s", err);
        free(err);
    }

    __sync_fetch_and_add(&stats.total_keys_cleaned, num_keys_cleaned);
    stats.last_num_keys_cleaned = num_keys_cleaned;
    stats.last_cleanup_at = (int64_t)time(NULL);
    LOG(ctx, "debug", "spill: cleanup completed: checked=%llu, removed=%llu",
        (unsigned long long)num_keys_scanned, (unsigned long long)num_keys_cleaned);

    return num_keys_cleaned;
}

int CleanupCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    LOG(ctx, "debug", "spill: cleanup command started");

    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    // Iterate through RocksDB to find and remove expired keys
    uint64_t num_keys_scanned = 0;
    uint64_t num_keys_cleaned = 0;
    int64_t current_time_ms = (int64_t)time(NULL) * 1000;

    rocksdb_iterator_t *iter = rocksdb_create_iterator(db, roptions);
    rocksdb_iter_seek_to_first(iter);

    while (rocksdb_iter_valid(iter)) {
        size_t keylen, vallen;
        const char *keyname = rocksdb_iter_key(iter, &keylen);
        const char *val = rocksdb_iter_value(iter, &vallen);

        num_keys_scanned++;

        // Check if value has TTL header
        if (val && vallen >= sizeof(int64_t)) {
            int64_t ttl_ms;
            memcpy(&ttl_ms, val, sizeof(int64_t));

            // If TTL is set (positive) and has expired
            if (ttl_ms > 0 && ttl_ms < current_time_ms) {
                // Key has expired, delete it
                DeleteKeyFromDB(keyname, keylen);
                num_keys_cleaned++;
                __sync_fetch_and_sub(&stats.num_keys_stored, 1);
                LOG(ctx, "debug", "spill: expired key removed: %.*s (ttl=%lld, now=%lld)",
                    (int)keylen, keyname, (long long)ttl_ms, (long long)current_time_ms);
            }
        }

        rocksdb_iter_next(iter);
    }

    char *err = NULL;
    rocksdb_iter_get_error(iter, &err);
    rocksdb_iter_destroy(iter);

    if (err != NULL) {
        LOG(ctx, "warning", "spill: cleanup iteration error: %s", err);
        ValkeyModule_ReplyWithError(ctx, err);
        free(err);
        return VALKEYMODULE_OK;
    }

    __sync_fetch_and_add(&stats.total_keys_cleaned, num_keys_cleaned);
    stats.last_num_keys_cleaned = num_keys_cleaned;
    stats.last_cleanup_at = (int64_t)time(NULL);
    LOG(ctx, "debug", "spill: cleanup completed: checked=%llu, removed=%llu",
        (unsigned long long)num_keys_scanned, (unsigned long long)num_keys_cleaned);

    ValkeyModule_ReplyWithArray(ctx, 4);
    ValkeyModule_ReplyWithSimpleString(ctx, "num_keys_scanned");
    ValkeyModule_ReplyWithLongLong(ctx, num_keys_scanned);
    ValkeyModule_ReplyWithSimpleString(ctx, "num_keys_cleaned");
    ValkeyModule_ReplyWithLongLong(ctx, num_keys_cleaned);

    return VALKEYMODULE_OK;
}

void SpillInfoFunc(ValkeyModuleInfoCtx *ctx, int for_crash_report) {
    VALKEYMODULE_NOT_USED(for_crash_report);

    // Add Statistics section
    ValkeyModule_InfoAddSection(ctx, "stats");
    ValkeyModule_InfoAddFieldULongLong(ctx, "num_keys_stored", stats.num_keys_stored);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_keys_written", stats.total_keys_written);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_keys_restored", stats.total_keys_restored);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_keys_cleaned", stats.total_keys_cleaned);
    ValkeyModule_InfoAddFieldULongLong(ctx, "last_num_keys_cleaned", stats.last_num_keys_cleaned);
    ValkeyModule_InfoAddFieldLongLong(ctx, "last_cleanup_at", stats.last_cleanup_at);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_bytes_written", stats.total_bytes_written);
    ValkeyModule_InfoAddFieldULongLong(ctx, "total_bytes_read", stats.total_bytes_read);

    // Add Configuration section
    ValkeyModule_InfoAddSection(ctx, "config");
    ValkeyModule_InfoAddFieldCString(ctx, "path", config.path ? config.path : "");
    ValkeyModule_InfoAddFieldULongLong(ctx, "max_memory_bytes", config.max_memory);
    ValkeyModule_InfoAddFieldLongLong(ctx, "cleanup_interval_seconds", config.cleanup_interval);
}


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

    if (InitRocksDB(ctx) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "failed to initialize RocksDB");
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    // Count active keys in RocksDB at startup
    stats.num_keys_stored = CountActiveKeys();
    ValkeyModule_Log(ctx, "notice", "found %llu active keys in RocksDB",
                     (unsigned long long)stats.num_keys_stored);

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREEVICTION, PreevictionKeyNotification) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREMISS, PremissNotification) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "spill.restore", RestoreCommand, "write", 1, 1, 1) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "spill.cleanup", CleanupCommand, "write", 0, 0, 0) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    // Register info callback so Spill stats appear in INFO/INFO ALL output
    if (ValkeyModule_RegisterInfoFunc(ctx, SpillInfoFunc) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "failed to register info callback");
        // Not a fatal error, continue without INFO integration
    }

    // Start the cleanup thread if cleanup_interval > 0
    if (config.cleanup_interval > 0) {
        cleanup_thread_running = 1;
        if (pthread_create(&cleanup_thread, NULL, CleanupThreadFunc, NULL) != 0) {
            ValkeyModule_Log(ctx, "warning", "failed to create cleanup thread");
            cleanup_thread_running = 0;
            // Not a fatal error, continue without periodic cleanup
        } else {
            ValkeyModule_Log(ctx, "notice", "cleanup thread started with interval=%d seconds",
                           config.cleanup_interval);
        }
    } else {
        ValkeyModule_Log(ctx, "notice", "periodic cleanup disabled (cleanup_interval=0)");
    }

    ValkeyModule_Log(ctx, "notice", "module loaded successfully, path=%s, max_memory=%zuMB, cleanup_interval=%ds",
                     config.path, config.max_memory / (1024 * 1024), config.cleanup_interval);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    ValkeyModule_Log(ctx, "notice", "unloading module, stats: stored=%llu restored=%llu cleaned=%llu",
                     (unsigned long long)stats.num_keys_stored,
                     (unsigned long long)stats.total_keys_restored,
                     (unsigned long long)stats.total_keys_cleaned);

    // Stop the cleanup thread if it's running
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
