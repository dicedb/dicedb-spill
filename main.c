#include "valkeymodule.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

static rocksdb_t *db = NULL;
static rocksdb_options_t *options = NULL;
static rocksdb_readoptions_t *roptions = NULL;
static rocksdb_writeoptions_t *woptions = NULL;

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

void ParseModuleArgs(ValkeyModuleString **argv, int argc) {
    for (int i = 0; i < argc; i += 2) {
        if (i + 1 >= argc) break;
        
        size_t len;
        const char *key = ValkeyModule_StringPtrLen(argv[i], &len);
        const char *value = ValkeyModule_StringPtrLen(argv[i + 1], &len);
        
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
        config.db_path = strdup("./dicedb-l2");
    }
}

int InitRocksDB(ValkeyModuleCtx *ctx) {
    char *err = NULL;
    
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
    
    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_size(table_options, config.block_size);
    rocksdb_block_based_options_set_block_restart_interval(table_options, config.block_restart_interval);
    rocksdb_options_set_block_based_table_factory(options, table_options);
    
    rocksdb_options_set_target_file_size_base(options, config.max_file_size);
    
    db = rocksdb_open(options, config.db_path, &err);
    if (err != NULL) {
        ValkeyModule_Log(ctx, "warning", "Failed to open RocksDB: %s", err);
        free(err);
        return VALKEYMODULE_ERR;
    }
    
    roptions = rocksdb_readoptions_create();
    woptions = rocksdb_writeoptions_create();
    
    ValkeyModule_Log(ctx, "notice", "RocksDB initialized at path: %s", config.db_path);
    ValkeyModule_Log(ctx, "notice", "RocksDB config - write_buffer_size: %zu, max_open_files: %d, block_size: %zu", 
                     config.write_buffer_size, config.max_open_files, config.block_size);
    
    return VALKEYMODULE_OK;
}

void CleanupRocksDB() {
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

int PreevictionKeyNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);

    if (strcmp(event, "preeviction") == 0 && db != NULL) {
        size_t keylen;
        const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);
        
        ValkeyModuleCallReply *dump_reply = ValkeyModule_Call(ctx, "DUMP", "s", key);
        ValkeyModuleCallReply *ttl_reply = ValkeyModule_Call(ctx, "PTTL", "s", key);
        
        if (dump_reply && ValkeyModule_CallReplyType(dump_reply) == VALKEYMODULE_REPLY_STRING) {
            size_t dump_len;
            const char *dump_data = ValkeyModule_CallReplyStringPtr(dump_reply, &dump_len);
            
            if (dump_data && dump_len > 0) {
                long long pttl = -1;
                if (ttl_reply && ValkeyModule_CallReplyType(ttl_reply) == VALKEYMODULE_REPLY_INTEGER) {
                    pttl = ValkeyModule_CallReplyInteger(ttl_reply);
                }
                
                int64_t absolute_expire_ms = 0;
                if (pttl > 0) {
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    absolute_expire_ms = (int64_t)(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000 + pttl;
                }
                
                size_t total_len = sizeof(int64_t) + sizeof(uint32_t) + dump_len;
                char *combined_data = malloc(total_len);
                
                if (combined_data) {
                    char *ptr = combined_data;
                    
                    memcpy(ptr, &absolute_expire_ms, sizeof(int64_t));
                    ptr += sizeof(int64_t);
                    
                    uint32_t dump_len_32 = (uint32_t)dump_len;
                    memcpy(ptr, &dump_len_32, sizeof(uint32_t));
                    ptr += sizeof(uint32_t);
                    
                    memcpy(ptr, dump_data, dump_len);
                    
                    char *err = NULL;
                    rocksdb_put(db, woptions, keyname, keylen, combined_data, total_len, &err);
                    
                    if (err != NULL) {
                        ValkeyModule_Log(ctx, "warning", "Failed to store key %.*s in RocksDB: %s", 
                                       (int)keylen, keyname, err);
                        free(err);
                    } else {
                        ValkeyModule_Log(ctx, "notice", "Stored evicted key %.*s (%zu bytes, expire: %ld ms) in RocksDB", 
                                       (int)keylen, keyname, dump_len, absolute_expire_ms);
                    }
                    
                    free(combined_data);
                }
            }
        } else {
            ValkeyModule_Log(ctx, "warning", "Failed to DUMP key %.*s before eviction", 
                           (int)keylen, keyname);
        }
        
        if (dump_reply) {
            ValkeyModule_FreeCallReply(dump_reply);
        }
        if (ttl_reply) {
            ValkeyModule_FreeCallReply(ttl_reply);
        }
    }

    return 0;
}

int RestoreCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        return ValkeyModule_WrongArity(ctx);
    }
    
    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, "ERR RocksDB not initialized");
    }
    
    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(argv[1], &keylen);
    
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
    
    if (vallen < sizeof(int64_t) + sizeof(uint32_t)) {
        free(val);
        return ValkeyModule_ReplyWithError(ctx, "ERR Corrupted data in RocksDB");
    }
    
    char *ptr = val;
    
    int64_t absolute_expire_ms;
    memcpy(&absolute_expire_ms, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);
    
    uint32_t dump_len;
    memcpy(&dump_len, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    if (vallen != sizeof(int64_t) + sizeof(uint32_t) + dump_len) {
        free(val);
        return ValkeyModule_ReplyWithError(ctx, "ERR Data length mismatch in RocksDB");
    }
    
    long long ttl_ms = 0;
    if (absolute_expire_ms > 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        int64_t current_ms = (int64_t)(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
        ttl_ms = absolute_expire_ms - current_ms;
        
        if (ttl_ms <= 0) {
            free(val);
            
            char *del_err = NULL;
            rocksdb_delete(db, woptions, keyname, keylen, &del_err);
            if (del_err) {
                ValkeyModule_Log(ctx, "warning", "Failed to delete expired key from RocksDB: %s", del_err);
                free(del_err);
            }
            
            return ValkeyModule_ReplyWithError(ctx, "ERR Key has expired");
        }
    }
    
    ValkeyModuleString *dump_data = ValkeyModule_CreateString(ctx, ptr, dump_len);
    
    ValkeyModuleCallReply *reply;
    if (ttl_ms > 0) {
        char ttl_str[32];
        snprintf(ttl_str, sizeof(ttl_str), "%lld", ttl_ms);
        reply = ValkeyModule_Call(ctx, "RESTORE", "sslc", argv[1], ttl_str, dump_data, "!");
    } else {
        reply = ValkeyModule_Call(ctx, "RESTORE", "sslc", argv[1], "0", dump_data, "!");
    }
    
    free(val);
    ValkeyModule_FreeString(ctx, dump_data);
    
    if (reply && ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        const char *errmsg = ValkeyModule_CallReplyStringPtr(reply, NULL);
        ValkeyModule_ReplyWithError(ctx, errmsg);
    } else {
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        
        char *del_err = NULL;
        rocksdb_delete(db, woptions, keyname, keylen, &del_err);
        if (del_err) {
            ValkeyModule_Log(ctx, "warning", "Failed to delete key from RocksDB after restore: %s", del_err);
            free(del_err);
        }
    }
    
    if (reply) {
        ValkeyModule_FreeCallReply(reply);
    }
    
    return VALKEYMODULE_OK;
}

int InfoCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    
    if (!db) {
        return ValkeyModule_ReplyWithError(ctx, "ERR RocksDB not initialized");
    }
    
    char *stats = rocksdb_property_value(db, "rocksdb.stats");
    
    if (stats) {
        ValkeyModule_ReplyWithVerbatimString(ctx, stats, strlen(stats));
        free(stats);
    } else {
        ValkeyModule_ReplyWithSimpleString(ctx, "No stats available");
    }
    
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "infcache", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    
    ParseModuleArgs(argv, argc);
    
    if (InitRocksDB(ctx) != VALKEYMODULE_OK) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }
    
    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREEVICTION, PreevictionKeyNotification) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "Failed to subscribe to eviction events");
        CleanupRocksDB();
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
    
    ValkeyModule_Log(ctx, "notice", "Infcache module loaded successfully with RocksDB at %s", config.db_path);
    
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    VALKEYMODULE_NOT_USED(ctx);
    CleanupRocksDB();
    return VALKEYMODULE_OK;
}