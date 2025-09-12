#include "valkeymodule.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
        
        ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "DUMP", "s", key);
        
        if (reply && ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_STRING) {
            size_t vallen;
            const char *val = ValkeyModule_CallReplyStringPtr(reply, &vallen);
            
            if (val && vallen > 0) {
                char *err = NULL;
                rocksdb_put(db, woptions, keyname, keylen, val, vallen, &err);
                
                if (err != NULL) {
                    ValkeyModule_Log(ctx, "warning", "Failed to store key %.*s in RocksDB: %s", 
                                   (int)keylen, keyname, err);
                    free(err);
                } else {
                    ValkeyModule_Log(ctx, "notice", "Stored evicted key %.*s (%zu bytes) in RocksDB", 
                                   (int)keylen, keyname, vallen);
                }
            }
        } else {
            ValkeyModule_Log(ctx, "warning", "Failed to DUMP key %.*s before eviction", 
                           (int)keylen, keyname);
        }
        
        if (reply) {
            ValkeyModule_FreeCallReply(reply);
        }
    }

    return 0;
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