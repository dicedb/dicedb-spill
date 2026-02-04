#include "valkeymodule.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

// Logging control: set to 1 to enable debug/verbose logging, 0 to disable
#define INFCACHE_DEBUG 0

#if INFCACHE_DEBUG
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
    uint64_t keys_stored;
    uint64_t keys_restored;
    uint64_t keys_expired;
    uint64_t keys_cleaned;
    uint64_t bytes_written;
    uint64_t bytes_read;
} InfcacheStats;

static InfcacheStats stats = {0};

static const char *ERR_DB_NOT_INIT = "ERR RocksDB not initialized";
static const char *ERR_CORRUPTED_DATA = "ERR Corrupted data in RocksDB";

typedef struct {
    char *path;
    size_t max_memory;  // Maximum memory budget for RocksDB (block cache + write buffers)
} InfcacheConfig;

static InfcacheConfig config = {
    .path = NULL,
    .max_memory = 256 * 1024 * 1024  // Default: 256MB total memory budget
};

static void DeleteKeyFromDB(const char *keyname, size_t keylen) {
    if (!db) return;

    char *err = NULL;
    rocksdb_delete(db, woptions, keyname, keylen, &err);
    if (err) {
        LOG(module_ctx, "warning", "infcache: failed to delete key from RocksDB: %.*s, error: %s",
            (int)keylen, keyname, err);
        free(err);
    } else {
        LOG(module_ctx, "debug", "infcache: deleted key from RocksDB: %.*s", (int)keylen, keyname);
    }
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
            if (config.max_memory < 64 * 1024 * 1024) {
                ValkeyModule_Log(ctx, "warning", "infcache: max-memory too low (%zu bytes), using minimum 64MB", config.max_memory);
                config.max_memory = 64 * 1024 * 1024;
            }
        }
    }

    if (!config.path) {
        ValkeyModule_Log(ctx, "warning", "infcache: 'path' parameter is required");
        return VALKEYMODULE_ERR;
    }

    return VALKEYMODULE_OK;
}

int InitRocksDB(ValkeyModuleCtx *ctx) {
    char *err = NULL;
    LOG(ctx, "debug", "infcache: initializing RocksDB at path=%s with max_memory=%zu bytes",
        config.path, config.max_memory);

    // Calculate memory distribution from max_memory budget
    size_t block_cache_size = config.max_memory / 2;        // 50% for block cache (read cache)
    size_t write_buffer_size = config.max_memory / 6;       // ~17% per buffer (3 buffers = 50%)
    // Remaining memory (~33%) is used for indexes, bloom filters, and other structures

    options = rocksdb_options_create();

    // Create DB if missing - essential for first run when eviction tier doesn't exist yet
    rocksdb_options_set_create_if_missing(options, 1);

    // Don't error if exists - allows reusing existing eviction tier across restarts
    rocksdb_options_set_error_if_exists(options, 0);

    // Disable paranoid checks - trades some safety for better write performance in eviction path
    rocksdb_options_set_paranoid_checks(options, 0);

    // Enable Snappy compression - reduces disk usage for evicted data (2-4x compression typical)
    rocksdb_options_set_compression(options, rocksdb_snappy_compression);

    // Write buffer size - larger buffers reduce write amplification but use more memory
    rocksdb_options_set_write_buffer_size(options, write_buffer_size);

    // Max write buffers - allows 3 concurrent memtables for smoother write flow during bursts
    rocksdb_options_set_max_write_buffer_number(options, 3);

    // Max open files - limits file handles to prevent exhausting system resources
    rocksdb_options_set_max_open_files(options, 1000);

    // Optimize for point lookups - critical since we restore individual keys on cache miss
    rocksdb_options_optimize_for_point_lookup(options, 64);

    // Enable mmap for reads - reduces system call overhead and improves read performance
    rocksdb_options_set_allow_mmap_reads(options, 1);

    // Enable mmap for writes - improves write throughput during heavy eviction
    rocksdb_options_set_allow_mmap_writes(options, 1);

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
        ValkeyModule_Log(ctx, "warning", "infcache: failed to open RocksDB: %s", err);
        free(err);
        return VALKEYMODULE_ERR;
    }
    ValkeyModule_Log(ctx, "notice", "infcache: RocksDB opened successfully (block_cache=%zuMB, write_buffer=%zuMB)",
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
        LOG(module_ctx, "warning", "infcache: premiss called with invalid params: event=%p key=%p db=%p",
            (void*)event, (void*)key, (void*)db);
        return 0;
    }
    if (strcmp(event, "premiss") != 0) return 0;

    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

    LOG(ctx, "notice", "infcache: premiss called for key=%.*s (len=%zu)", (int)keylen, keyname, keylen);
    LOG(ctx, "debug", "infcache: checking RocksDB for key=%.*s", (int)keylen, keyname);
    char *err = NULL;
    size_t vallen;
    char *val = rocksdb_get(db, roptions, keyname, keylen, &vallen, &err);

    if (err != NULL) {
        LOG(ctx, "warning", "infcache: error getting key=%.*s from RocksDB: %s", (int)keylen, keyname, err);
        free(err);
        return 0;
    }

    if (val == NULL) {
        LOG(ctx, "notice", "infcache: key=%.*s not found not in RocksDB", (int)keylen, keyname);
        return 0;
    }

    LOG(ctx, "notice", "infcache: key=%.*s found in RocksDB with value_len=%zu bytes", (int)keylen, keyname, vallen);
    if (vallen < sizeof(int64_t)) {
        free(val);
        return 0;
    }

    char *ptr = val;
    int64_t expiry_time_ms;  // Absolute expiry timestamp
    memcpy(&expiry_time_ms, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);

    // Check if key has expired
    if (expiry_time_ms > 0) {
        int64_t current_time_ms = (int64_t)time(NULL) * 1000;
        if (expiry_time_ms <= current_time_ms) {
            LOG(ctx, "notice", "infcache: key=%.*s has expired (expiry=%lld, now=%lld), deleting",
                (int)keylen, keyname, (long long)expiry_time_ms, (long long)current_time_ms);
            __sync_fetch_and_add(&stats.keys_expired, 1);
            DeleteKeyFromDB(keyname, keylen);
            free(val);
            return 0;
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
        LOG(ctx, "debug", "infcache: restoring key=%.*s with TTL=%lld ms", (int)keylen, keyname, (long long)ttl_ms);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, ttl_ms, dump_data, "REPLACE");
    } else {
        LOG(ctx, "debug", "infcache: restoring key=%.*s with no expiry", (int)keylen, keyname);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, 0, dump_data, "REPLACE");
    }

    free(val);
    ValkeyModule_FreeString(ctx, dump_data);

    if (reply && ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ERROR) {
        LOG(ctx, "debug", "infcache: key=%.*s restored successfully", (int)keylen, keyname);
        __sync_fetch_and_add(&stats.keys_restored, 1);
        __sync_fetch_and_add(&stats.bytes_read, vallen);
        DeleteKeyFromDB(keyname, keylen);
    } else {
        LOG(ctx, "warning", "infcache: failed to restore key=%.*s: %s", (int)keylen, keyname,
            reply ? (ValkeyModule_CallReplyStringPtr(reply, NULL) ? ValkeyModule_CallReplyStringPtr(reply, NULL) : "no reply") : "no reply");
    }

    if (reply) {
        ValkeyModule_FreeCallReply(reply);
    }

    return 0;
}

int PreevictionKeyNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);

    if (!event || !key || !db) {
        LOG(module_ctx, "warning", "infcache: preeviction called with invalid params: event=%p key=%p db=%p",
            (void*)event, (void*)key, (void*)db);
        return 0;
    }
    if (strcmp(event, "preeviction") != 0) return 0;

    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

    LOG(ctx, "notice", "infcache: preeviction called for key=%.*s (len=%zu)", (int)keylen, keyname, keylen);
    LOG(ctx, "debug", "infcache: calling DUMP for key=%.*s", (int)keylen, keyname);

    ValkeyModuleCallReply *dump_reply = ValkeyModule_Call(ctx, "DUMP", "s", key);
    if (!dump_reply || ValkeyModule_CallReplyType(dump_reply) != VALKEYMODULE_REPLY_STRING) {
        LOG(ctx, "warning", "infcache: DUMP failed for key=%.*s, reply_type=%d",
            (int)keylen, keyname, dump_reply ? ValkeyModule_CallReplyType(dump_reply) : -1);
        if (dump_reply) ValkeyModule_FreeCallReply(dump_reply);
        return 0;
    }

    size_t dump_len;
    const char *dump_data = ValkeyModule_CallReplyStringPtr(dump_reply, &dump_len);

    LOG(ctx, "debug", "infcache: DUMP successful for key=%.*s, dump_len=%zu", (int)keylen, keyname, dump_len);

    if (!dump_data) {
        ValkeyModule_FreeCallReply(dump_reply);
        return 0;
    }

    LOG(ctx, "debug", "infcache: calling PTTL for key=%.*s", (int)keylen, keyname);
    ValkeyModuleCallReply *ttl_reply = ValkeyModule_Call(ctx, "PTTL", "s", key);
    int64_t pttl = -1;
    int64_t expiry_time_ms = -1;  // Absolute expiry timestamp

    if (ttl_reply && ValkeyModule_CallReplyType(ttl_reply) == VALKEYMODULE_REPLY_INTEGER) {
        pttl = ValkeyModule_CallReplyInteger(ttl_reply);
        // Convert relative TTL to absolute expiry timestamp
        if (pttl > 0) {
            int64_t current_time_ms = (int64_t)time(NULL) * 1000;
            expiry_time_ms = current_time_ms + pttl;
            LOG(ctx, "debug", "infcache: PTTL for key=%.*s is %lld ms, expiry_time=%lld",
                (int)keylen, keyname, (long long)pttl, (long long)expiry_time_ms);
        } else {
            // pttl is -1 (no expiry) or -2 (key doesn't exist)
            expiry_time_ms = pttl;
            LOG(ctx, "debug", "infcache: PTTL for key=%.*s returned %lld (no expiry)",
                (int)keylen, keyname, (long long)pttl);
        }
    }

    size_t total_len = sizeof(int64_t) + dump_len;
    LOG(ctx, "debug", "infcache: storing key=%.*s, total_key_len=%zu (expiry_header=8 + value_len=%zu)",
        (int)keylen, keyname, total_len, dump_len);

    char *combined_data = malloc(total_len);
    if (!combined_data) {
        LOG(ctx, "warning", "infcache: malloc failed for key=%.*s, total_key_len=%zu", (int)keylen, keyname, total_len);
        ValkeyModule_FreeCallReply(dump_reply);
        if (ttl_reply) ValkeyModule_FreeCallReply(ttl_reply);
        return 0;
    }

    char *ptr = combined_data;
    // Store absolute expiry timestamp instead of relative TTL
    memcpy(ptr, &expiry_time_ms, sizeof(int64_t));
    ptr += sizeof(int64_t);
    memcpy(ptr, dump_data, dump_len);

    LOG(ctx, "notice", "infcache: storing key=%.*s directly to RocksDB", (int)keylen, keyname);
    char *err = NULL;
    rocksdb_put(db, woptions, keyname, keylen, combined_data, total_len, &err);
    if (err != NULL) {
        LOG(ctx, "warning", "infcache: failed to persist key=%.*s: %s", (int)keylen, keyname, err);
        free(err);
    } else {
        __sync_fetch_and_add(&stats.keys_stored, 1);
        __sync_fetch_and_add(&stats.bytes_written, total_len);
        LOG(ctx, "notice", "infcache: stored key=%.*s (len=%zu) to RocksDB, size=%zu, expiry=%lld ms",
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

    LOG(ctx, "debug", "infcache: restore command for key=%.*s", (int)keylen, keyname);

    if (!keyname || keylen == 0) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Invalid key data");
    }

    char *err = NULL;
    size_t vallen;
    char *val = rocksdb_get(db, roptions, keyname, keylen, &vallen, &err);

    if (err != NULL) {
        ValkeyModule_ReplyWithError(ctx, err);
        free(err);
        return VALKEYMODULE_OK;
    }

    if (val == NULL) {
        return ValkeyModule_ReplyWithNull(ctx);
    }

    if (vallen < sizeof(int64_t)) {
        free(val);
        return ValkeyModule_ReplyWithError(ctx, ERR_CORRUPTED_DATA);
    }

    char *ptr = val;
    int64_t expiry_time_ms;  // Absolute expiry timestamp
    memcpy(&expiry_time_ms, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);

    // Check if key has expired
    if (expiry_time_ms > 0) {
        int64_t current_time_ms = (int64_t)time(NULL) * 1000;
        if (expiry_time_ms <= current_time_ms) {
            LOG(ctx, "notice", "infcache: key=%.*s has expired (expiry=%lld, now=%lld), deleting",
                (int)keylen, keyname, (long long)expiry_time_ms, (long long)current_time_ms);
            __sync_fetch_and_add(&stats.keys_expired, 1);
            DeleteKeyFromDB(keyname, keylen);
            free(val);
            return ValkeyModule_ReplyWithError(ctx, "ERR Key has expired");
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
        LOG(ctx, "debug", "infcache: restoring key=%.*s with TTL=%lld ms", (int)keylen, keyname, (long long)ttl_ms);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", argv[1], ttl_ms, dump_data, "REPLACE");
    } else {
        LOG(ctx, "debug", "infcache: restoring key=%.*s with no expiry", (int)keylen, keyname);
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", argv[1], 0, dump_data, "REPLACE");
    }

    free(val);
    ValkeyModule_FreeString(ctx, dump_data);

    if (reply && ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        const char *errmsg = ValkeyModule_CallReplyStringPtr(reply, NULL);
        LOG(ctx, "warning", "infcache: restore command failed for key=%.*s: %s",
            (int)keylen, keyname, errmsg ? errmsg : "unknown");
        ValkeyModule_ReplyWithError(ctx, errmsg);
    } else {
        LOG(ctx, "debug", "infcache: restore command succeeded for key=%.*s, size=%zu",
            (int)keylen, keyname, vallen);
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        __sync_fetch_and_add(&stats.keys_restored, 1);
        __sync_fetch_and_add(&stats.bytes_read, vallen);
        DeleteKeyFromDB(keyname, keylen);
    }

    if (reply) {
        ValkeyModule_FreeCallReply(reply);
    }

    return VALKEYMODULE_OK;
}

int CleanupCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    LOG(ctx, "debug", "infcache: cleanup command started");

    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    // Iterate through RocksDB to find and remove expired keys
    uint64_t keys_checked = 0;
    uint64_t keys_removed = 0;
    int64_t current_time_ms = (int64_t)time(NULL) * 1000;

    rocksdb_iterator_t *iter = rocksdb_create_iterator(db, roptions);
    rocksdb_iter_seek_to_first(iter);

    while (rocksdb_iter_valid(iter)) {
        size_t keylen, vallen;
        const char *keyname = rocksdb_iter_key(iter, &keylen);
        const char *val = rocksdb_iter_value(iter, &vallen);

        keys_checked++;

        // Check if value has TTL header
        if (val && vallen >= sizeof(int64_t)) {
            int64_t ttl_ms;
            memcpy(&ttl_ms, val, sizeof(int64_t));

            // If TTL is set (positive) and has expired
            if (ttl_ms > 0 && ttl_ms < current_time_ms) {
                // Key has expired, delete it
                DeleteKeyFromDB(keyname, keylen);
                keys_removed++;
                __sync_fetch_and_add(&stats.keys_expired, 1);
                LOG(ctx, "debug", "infcache: expired key removed: %.*s (ttl=%lld, now=%lld)",
                    (int)keylen, keyname, (long long)ttl_ms, (long long)current_time_ms);
            }
        }

        rocksdb_iter_next(iter);
    }

    char *err = NULL;
    rocksdb_iter_get_error(iter, &err);
    rocksdb_iter_destroy(iter);

    if (err != NULL) {
        LOG(ctx, "warning", "infcache: cleanup iteration error: %s", err);
        ValkeyModule_ReplyWithError(ctx, err);
        free(err);
        return VALKEYMODULE_OK;
    }

    __sync_fetch_and_add(&stats.keys_cleaned, keys_removed);
    LOG(ctx, "debug", "infcache: cleanup completed: checked=%llu, removed=%llu",
        (unsigned long long)keys_checked, (unsigned long long)keys_removed);

    ValkeyModule_ReplyWithArray(ctx, 4);
    ValkeyModule_ReplyWithSimpleString(ctx, "keys_checked");
    ValkeyModule_ReplyWithLongLong(ctx, keys_checked);
    ValkeyModule_ReplyWithSimpleString(ctx, "keys_removed");
    ValkeyModule_ReplyWithLongLong(ctx, keys_removed);

    return VALKEYMODULE_OK;
}

int StatsCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    ValkeyModule_ReplyWithArray(ctx, 12);

    ValkeyModule_ReplyWithSimpleString(ctx, "keys_stored");
    ValkeyModule_ReplyWithLongLong(ctx, stats.keys_stored);

    ValkeyModule_ReplyWithSimpleString(ctx, "keys_restored");
    ValkeyModule_ReplyWithLongLong(ctx, stats.keys_restored);

    ValkeyModule_ReplyWithSimpleString(ctx, "keys_expired");
    ValkeyModule_ReplyWithLongLong(ctx, stats.keys_expired);

    ValkeyModule_ReplyWithSimpleString(ctx, "keys_cleaned");
    ValkeyModule_ReplyWithLongLong(ctx, stats.keys_cleaned);

    ValkeyModule_ReplyWithSimpleString(ctx, "bytes_written");
    ValkeyModule_ReplyWithLongLong(ctx, stats.bytes_written);

    ValkeyModule_ReplyWithSimpleString(ctx, "bytes_read");
    ValkeyModule_ReplyWithLongLong(ctx, stats.bytes_read);

    return VALKEYMODULE_OK;
}

// Helper function to get RocksDB property as integer
static int64_t get_rocksdb_int_property(const char *property) {
    if (!db) return -1;

    char *value = rocksdb_property_value(db, property);
    if (!value) return -1;

    int64_t result = atoll(value);
    free(value);
    return result;
}

// Helper function to append formatted string to buffer
static void append_info_line(char **buffer, size_t *size, size_t *capacity,
                             const char *key, const char *value) {
    size_t needed = strlen(key) + strlen(value) + 3; // key:value\r\n

    while (*size + needed >= *capacity) {
        *capacity *= 2;
        *buffer = realloc(*buffer, *capacity);
    }

    *size += sprintf(*buffer + *size, "%s:%s\r\n", key, value);
}

static void append_info_line_int(char **buffer, size_t *size, size_t *capacity,
                                 const char *key, int64_t value) {
    char value_str[32];
    snprintf(value_str, sizeof(value_str), "%lld", (long long)value);
    append_info_line(buffer, size, capacity, key, value_str);
}

static void append_info_section(char **buffer, size_t *size, size_t *capacity,
                                const char *section_name) {
    size_t needed = strlen(section_name) + 4; // # section\r\n

    while (*size + needed >= *capacity) {
        *capacity *= 2;
        *buffer = realloc(*buffer, *capacity);
    }

    *size += sprintf(*buffer + *size, "# %s\r\n", section_name);
}

int InfoCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    // Initialize buffer for formatted output
    size_t capacity = 4096;
    size_t size = 0;
    char *buffer = malloc(capacity);
    if (!buffer) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Out of memory");
    }

    // Infcache section - our custom metrics
    append_info_section(&buffer, &size, &capacity, "Infcache");
    append_info_line_int(&buffer, &size, &capacity, "keys_stored", stats.keys_stored);
    append_info_line_int(&buffer, &size, &capacity, "keys_restored", stats.keys_restored);
    append_info_line_int(&buffer, &size, &capacity, "keys_expired", stats.keys_expired);
    append_info_line_int(&buffer, &size, &capacity, "keys_cleaned", stats.keys_cleaned);
    append_info_line_int(&buffer, &size, &capacity, "bytes_written", stats.bytes_written);
    append_info_line_int(&buffer, &size, &capacity, "bytes_read", stats.bytes_read);
    append_info_line(&buffer, &size, &capacity, "path", config.path);

    char max_mem_str[64];
    snprintf(max_mem_str, sizeof(max_mem_str), "%zu (%zuMB)",
             config.max_memory, config.max_memory / (1024 * 1024));
    append_info_line(&buffer, &size, &capacity, "max_memory", max_mem_str);

    // RocksDB Memory section
    append_info_section(&buffer, &size, &capacity, "RocksDB_Memory");

    int64_t block_cache_usage = get_rocksdb_int_property("rocksdb.block-cache-usage");
    if (block_cache_usage >= 0) {
        char usage_str[64];
        snprintf(usage_str, sizeof(usage_str), "%lld (%lldMB)",
                 (long long)block_cache_usage, (long long)(block_cache_usage / (1024 * 1024)));
        append_info_line(&buffer, &size, &capacity, "block_cache_usage", usage_str);
    }

    int64_t block_cache_pinned = get_rocksdb_int_property("rocksdb.block-cache-pinned-usage");
    if (block_cache_pinned >= 0) {
        char pinned_str[64];
        snprintf(pinned_str, sizeof(pinned_str), "%lld (%lldMB)",
                 (long long)block_cache_pinned, (long long)(block_cache_pinned / (1024 * 1024)));
        append_info_line(&buffer, &size, &capacity, "block_cache_pinned_usage", pinned_str);
    }

    int64_t memtable_size = get_rocksdb_int_property("rocksdb.cur-size-all-mem-tables");
    if (memtable_size >= 0) {
        char mem_str[64];
        snprintf(mem_str, sizeof(mem_str), "%lld (%lldMB)",
                 (long long)memtable_size, (long long)(memtable_size / (1024 * 1024)));
        append_info_line(&buffer, &size, &capacity, "memtable_size", mem_str);
    }

    int64_t table_readers_mem = get_rocksdb_int_property("rocksdb.estimate-table-readers-mem");
    if (table_readers_mem >= 0) {
        char readers_str[64];
        snprintf(readers_str, sizeof(readers_str), "%lld (%lldMB)",
                 (long long)table_readers_mem, (long long)(table_readers_mem / (1024 * 1024)));
        append_info_line(&buffer, &size, &capacity, "table_readers_mem", readers_str);
    }

    // RocksDB Storage section
    append_info_section(&buffer, &size, &capacity, "RocksDB_Storage");

    int64_t num_keys = get_rocksdb_int_property("rocksdb.estimate-num-keys");
    if (num_keys >= 0) {
        append_info_line_int(&buffer, &size, &capacity, "estimated_keys", num_keys);
    }

    int64_t live_data_size = get_rocksdb_int_property("rocksdb.estimate-live-data-size");
    if (live_data_size >= 0) {
        char size_str[64];
        snprintf(size_str, sizeof(size_str), "%lld (%lldMB)",
                 (long long)live_data_size, (long long)(live_data_size / (1024 * 1024)));
        append_info_line(&buffer, &size, &capacity, "live_data_size", size_str);
    }

    int64_t total_sst_size = get_rocksdb_int_property("rocksdb.total-sst-files-size");
    if (total_sst_size >= 0) {
        char sst_str[64];
        snprintf(sst_str, sizeof(sst_str), "%lld (%lldMB)",
                 (long long)total_sst_size, (long long)(total_sst_size / (1024 * 1024)));
        append_info_line(&buffer, &size, &capacity, "total_sst_files_size", sst_str);
    }

    int64_t num_snapshots = get_rocksdb_int_property("rocksdb.num-snapshots");
    if (num_snapshots >= 0) {
        append_info_line_int(&buffer, &size, &capacity, "num_snapshots", num_snapshots);
    }

    // RocksDB Compaction section
    append_info_section(&buffer, &size, &capacity, "RocksDB_Compaction");

    int64_t num_immutable = get_rocksdb_int_property("rocksdb.num-immutable-mem-table");
    if (num_immutable >= 0) {
        append_info_line_int(&buffer, &size, &capacity, "num_immutable_memtables", num_immutable);
    }

    int64_t flush_pending = get_rocksdb_int_property("rocksdb.mem-table-flush-pending");
    if (flush_pending >= 0) {
        append_info_line(&buffer, &size, &capacity, "memtable_flush_pending",
                        flush_pending ? "yes" : "no");
    }

    int64_t compaction_pending = get_rocksdb_int_property("rocksdb.compaction-pending");
    if (compaction_pending >= 0) {
        append_info_line(&buffer, &size, &capacity, "compaction_pending",
                        compaction_pending ? "yes" : "no");
    }

    int64_t bg_errors = get_rocksdb_int_property("rocksdb.background-errors");
    if (bg_errors >= 0) {
        append_info_line_int(&buffer, &size, &capacity, "background_errors", bg_errors);
    }

    int64_t base_level = get_rocksdb_int_property("rocksdb.base-level");
    if (base_level >= 0) {
        append_info_line_int(&buffer, &size, &capacity, "base_level", base_level);
    }

    // Get level file counts (L0-L6 are common)
    for (int level = 0; level <= 6; level++) {
        char property[64];
        snprintf(property, sizeof(property), "rocksdb.num-files-at-level%d", level);
        int64_t num_files = get_rocksdb_int_property(property);
        if (num_files >= 0) {
            char key[32];
            snprintf(key, sizeof(key), "num_files_L%d", level);
            append_info_line_int(&buffer, &size, &capacity, key, num_files);
        }
    }

    // Reply with formatted output
    ValkeyModule_ReplyWithVerbatimString(ctx, buffer, size);
    free(buffer);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "infcache", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    module_ctx = ValkeyModule_GetDetachedThreadSafeContext(ctx);

    LOG(ctx, "notice", "infcache: loading module with %d arguments", argc);

    if (ParseModuleArgs(ctx, argv, argc) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "infcache: failed to parse module arguments");
        return VALKEYMODULE_ERR;
    }

    if (InitRocksDB(ctx) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "infcache: failed to initialize RocksDB");
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREEVICTION, PreevictionKeyNotification) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREMISS, PremissNotification) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "infcache.restore", RestoreCommand, "write", 1, 1, 1) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "infcache.info", InfoCommand, "readonly", 0, 0, 0) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "infcache.cleanup", CleanupCommand, "write", 0, 0, 0) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "infcache.stats", StatsCommand, "readonly", 0, 0, 0) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    ValkeyModule_Log(ctx, "notice", "infcache: module loaded successfully, path=%s, max_memory=%zuMB",
                     config.path, config.max_memory / (1024 * 1024));

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    ValkeyModule_Log(ctx, "notice", "infcache: unloading module, stats: stored=%llu restored=%llu expired=%llu",
                     (unsigned long long)stats.keys_stored,
                     (unsigned long long)stats.keys_restored,
                     (unsigned long long)stats.keys_expired);

    if (module_ctx) {
        ValkeyModule_FreeThreadSafeContext(module_ctx);
        module_ctx = NULL;
    }
    CleanupRocksDB();
    return VALKEYMODULE_OK;
}
