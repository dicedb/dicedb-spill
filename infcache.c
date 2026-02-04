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

#define BLOOM_FILTER_BITS_PER_KEY 10
#define LRU_CACHE_SIZE (64 * 1024 * 1024)

static const char *ERR_DB_NOT_INIT = "ERR RocksDB not initialized";
static const char *ERR_CORRUPTED_DATA = "ERR Corrupted data in RocksDB";

typedef struct {
    char *db_path;
    int create_if_missing;
    int error_if_exists;
    int paranoid_checks;
    int compression;
    size_t write_buffer_size;
    int max_open_files;
    size_t block_size;
    int block_restart_interval;
    size_t max_file_size;
} InfcacheConfig;

static InfcacheConfig config = {
    .db_path = NULL,
    .create_if_missing = 1,
    .error_if_exists = 0,
    .paranoid_checks = 0,
    .compression = 1,
    .write_buffer_size = 64 * 1024 * 1024,
    .max_open_files = 1000,
    .block_size = 4 * 1024,
    .block_restart_interval = 16,
    .max_file_size = 64 * 1024 * 1024
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
            if (config.db_path) free(config.db_path);
            config.db_path = strdup(value);
        } else if (strcasecmp(key, "create_if_missing") == 0) {
            config.create_if_missing = atoi(value);
        } else if (strcasecmp(key, "error_if_exists") == 0) {
            config.error_if_exists = atoi(value);
        } else if (strcasecmp(key, "paranoid_checks") == 0) {
            config.paranoid_checks = atoi(value);
        } else if (strcasecmp(key, "compression") == 0) {
            config.compression = atoi(value);
        } else if (strcasecmp(key, "write_buffer_size") == 0) {
            config.write_buffer_size = (size_t)atoll(value);
        } else if (strcasecmp(key, "max_open_files") == 0) {
            config.max_open_files = atoi(value);
        } else if (strcasecmp(key, "block_size") == 0) {
            config.block_size = (size_t)atoll(value);
        } else if (strcasecmp(key, "block_restart_interval") == 0) {
            config.block_restart_interval = atoi(value);
        } else if (strcasecmp(key, "max_file_size") == 0) {
            config.max_file_size = (size_t)atoll(value);
        }
    }

    if (!config.db_path) {
        ValkeyModule_Log(ctx, "warning", "infcache: 'path' parameter is required");
        return VALKEYMODULE_ERR;
    }

    return VALKEYMODULE_OK;
}

int InitRocksDB(ValkeyModuleCtx *ctx) {
    char *err = NULL;
    LOG(ctx, "debug", "infcache: initializing RocksDB at path=%s", config.db_path);

    options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options, config.create_if_missing);
    rocksdb_options_set_error_if_exists(options, config.error_if_exists);
    rocksdb_options_set_paranoid_checks(options, config.paranoid_checks);

    if (config.compression) {
        rocksdb_options_set_compression(options, rocksdb_snappy_compression);
    } else {
        rocksdb_options_set_compression(options, rocksdb_no_compression);
    }

    rocksdb_options_set_write_buffer_size(options, config.write_buffer_size);
    rocksdb_options_set_max_open_files(options, config.max_open_files);
    rocksdb_options_optimize_for_point_lookup(options, 64);
    rocksdb_options_set_allow_mmap_reads(options, 1);
    rocksdb_options_set_allow_mmap_writes(options, 1);
    rocksdb_options_set_max_background_compactions(options, 2);
    rocksdb_options_set_level_compaction_dynamic_level_bytes(options, 1);

    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();
    rocksdb_cache_t *cache = rocksdb_cache_create_lru(LRU_CACHE_SIZE);
    rocksdb_block_based_options_set_block_cache(table_options, cache);

    rocksdb_filterpolicy_t *filter_policy = rocksdb_filterpolicy_create_bloom(BLOOM_FILTER_BITS_PER_KEY);
    rocksdb_block_based_options_set_filter_policy(table_options, filter_policy);

    rocksdb_block_based_options_set_block_size(table_options, config.block_size);
    rocksdb_block_based_options_set_block_restart_interval(table_options, config.block_restart_interval);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(table_options, 1);
    rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(table_options, 1);

    rocksdb_options_set_block_based_table_factory(options, table_options);
    rocksdb_block_based_options_destroy(table_options);

    rocksdb_options_set_target_file_size_base(options, config.max_file_size);

    db = rocksdb_open(options, config.db_path, &err);
    if (err != NULL) {
        ValkeyModule_Log(ctx, "warning", "infcache: failed to open RocksDB: %s", err);
        free(err);
        return VALKEYMODULE_ERR;
    }
    LOG(ctx, "debug", "infcache: RocksDB opened successfully with TTL support");

    roptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(roptions, 0);
    rocksdb_readoptions_set_fill_cache(roptions, 1);

    woptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(woptions, 0);
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
    if (config.db_path) {
        free(config.db_path);
        config.db_path = NULL;
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

int InfoCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, ERR_DB_NOT_INIT);
    }

    char *db_stats = rocksdb_property_value(db, "rocksdb.stats");

    if (db_stats) {
        ValkeyModule_ReplyWithVerbatimString(ctx, db_stats, strlen(db_stats));
        free(db_stats);
    } else {
        ValkeyModule_ReplyWithSimpleString(ctx, "No stats available");
    }

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

    ValkeyModule_Log(ctx, "notice", "infcache: module loaded successfully, db_path=%s", config.db_path);

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
