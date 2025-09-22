#include "valkeymodule.h"
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <sys/mman.h>
#include <emmintrin.h>  // SSE2 for optimized memory operations

// Compiler hints for better optimization
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#define prefetch(x)     __builtin_prefetch(x, 0, 3)
#define prefetch_write(x) __builtin_prefetch(x, 1, 3)
#define FORCE_INLINE    __attribute__((always_inline)) inline
#define HOT_FUNCTION    __attribute__((hot)) __attribute__((flatten))
#define COLD_FUNCTION   __attribute__((cold)) __attribute__((noinline))
#define PURE_FUNCTION   __attribute__((pure))
#define CONST_FUNCTION  __attribute__((const))
#define RESTRICT        __restrict__

static rocksdb_t *db = NULL;
static rocksdb_options_t *options = NULL;
static rocksdb_readoptions_t *roptions = NULL;
static rocksdb_writeoptions_t *woptions = NULL;
static rocksdb_writebatch_t *batch = NULL;
static int batch_count = 0;

// Statistics tracking
typedef struct {
    uint64_t keys_stored;
    uint64_t keys_restored;
    uint64_t keys_expired;
    uint64_t keys_cleaned;
    uint64_t bytes_written;
    uint64_t bytes_read;
} InfcacheStats;

// Error message cache to avoid repeated string allocations
// Pre-computed error message lengths for faster operations
static const struct {
    const char * const msg;
    const size_t len;
} ERROR_MESSAGES[] = {
    {"ERR RocksDB not initialized", 26},
    {"ERR Corrupted data in RocksDB", 29},
    {"ERR Data length mismatch in RocksDB", 35},
    {"ERR Key has expired", 19},
    {"ERR Failed to get current time", 31}
};

#define ERR_DB_NOT_INIT 0
#define ERR_CORRUPTED_DATA 1
#define ERR_LENGTH_MISMATCH 2
#define ERR_KEY_EXPIRED 3
#define ERR_TIME_FAILED 4

static InfcacheStats __attribute__((aligned(64))) stats = {0};

#define MAX_KEY_SIZE 512
#define HEADER_SIZE (sizeof(int64_t) + sizeof(uint32_t))
#define BATCH_SIZE_THRESHOLD 10
#define BLOOM_FILTER_BITS_PER_KEY 10
#define LRU_CACHE_SIZE (64 * 1024 * 1024)
#define STACK_BUFFER_SIZE (16 * 1024)
#define PREFETCH_DISTANCE 64
#define MIN_DUMP_SIZE 64
#define MAX_REASONABLE_TTL (365LL * 24 * 3600 * 1000) // 1 year in ms
#define SIMD_THRESHOLD 64  // Use SIMD for copies >= 64 bytes
#define SMALL_COPY_THRESHOLD 16 // Use optimized path for small copies
#define MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory")
#define COMPILER_BARRIER() __asm__ __volatile__("" : : : "memory")
#define CPU_RELAX() __asm__ __volatile__("pause" ::: "memory")
#define LIKELY_BRANCH(x) __builtin_expect_with_probability(!!(x), 1, 0.99)
#define UNLIKELY_BRANCH(x) __builtin_expect_with_probability(!!(x), 0, 0.01)
#define ASSUME(cond) do { if (!(cond)) __builtin_unreachable(); } while(0)
#define PREDICT_TRUE(x) __builtin_expect_with_probability(!!(x), 1, 0.999)
#define PREDICT_FALSE(x) __builtin_expect_with_probability(!!(x), 0, 0.001)
#define ALWAYS_INLINE __attribute__((always_inline, flatten)) inline

// Memory alignment for better cache performance
#define CACHE_LINE_SIZE 64
#define ALIGN_TO_CACHE_LINE __attribute__((aligned(CACHE_LINE_SIZE)))

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
} ALIGN_TO_CACHE_LINE InfcacheConfig;

// Thread-local storage for time caching
// Optimize thread-local storage layout for better cache performance
static __thread struct {
    int64_t cached_time_ms;
    int64_t cache_valid_until;
    uint32_t call_count;  // Track frequency for adaptive caching
    uint32_t padding;     // Align to 8-byte boundary
} __attribute__((aligned(32))) time_cache = {0};

// Helper function to get current time in milliseconds with adaptive caching
static ALWAYS_INLINE HOT_FUNCTION int64_t GetCurrentTimeMs() {
    struct timespec ts;

    // Increment call counter for adaptive behavior
    ++time_cache.call_count;

    if (PREDICT_FALSE(clock_gettime(CLOCK_MONOTONIC, &ts) != 0)) {
        return -1;
    }

    int64_t mono_ms = (int64_t)(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;

    // Adaptive caching: more frequent calls = longer cache time
    int cache_duration = (time_cache.call_count > 100) ? 50 : 10;

    // Cache time to reduce system calls
    if (PREDICT_TRUE(mono_ms < time_cache.cache_valid_until)) {
        return time_cache.cached_time_ms;
    }

    if (PREDICT_FALSE(clock_gettime(CLOCK_REALTIME, &ts) != 0)) {
        return -1;
    }

    time_cache.cached_time_ms = (int64_t)(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
    time_cache.cache_valid_until = mono_ms + cache_duration;
    return time_cache.cached_time_ms;
}

// Helper function to delete key from RocksDB with optimized error handling
static ALWAYS_INLINE void DeleteKeyFromDB(const char *keyname, size_t keylen, const char *context) {
    VALKEYMODULE_NOT_USED(context);
    if (PREDICT_FALSE(!db)) return;

    char *err = NULL;
    rocksdb_delete(db, woptions, keyname, keylen, &err);
    if (PREDICT_FALSE(err)) {
        free(err);
    }
}

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

// SIMD-optimized string to integer conversion with overflow protection
static ALWAYS_INLINE PURE_FUNCTION long long fast_atoll(const char * RESTRICT str, size_t len) {
    if (PREDICT_FALSE(len == 0 || len > 20)) return 0; // Bounds check

    register long long result = 0;
    register long long sign = 1;
    register size_t i = 0;

    // Handle sign with computed branch
    if (PREDICT_FALSE(len > 0 && str[0] == '-')) {
        sign = -1;
        i = 1;
    }

    // Unroll loop for better performance on small numbers
    switch (len - i) {
        case 1:
            if (LIKELY_BRANCH(str[i] >= '0' && str[i] <= '9'))
                result = str[i] - '0';
            break;
        case 2:
            if (LIKELY_BRANCH(str[i] >= '0' && str[i] <= '9' && str[i+1] >= '0' && str[i+1] <= '9'))
                result = (str[i] - '0') * 10 + (str[i+1] - '0');
            break;
        default:
            // Standard loop for longer numbers with overflow check
            for (; LIKELY_BRANCH(i < len && str[i] >= '0' && str[i] <= '9'); i++) {
                register long long new_result = result * 10 + (str[i] - '0');
                if (PREDICT_FALSE(new_result < result)) return 0; // Overflow check
                result = new_result;
            }
    }

    return result * sign;
}

void COLD_FUNCTION ParseModuleArgs(ValkeyModuleString **argv, int argc) {
    for (register int i = 0; LIKELY_BRANCH(i < argc); i += 2) {
        if (UNLIKELY_BRANCH(i + 1 >= argc)) break;

        size_t keylen, vallen;
        const char *key = ValkeyModule_StringPtrLen(argv[i], &keylen);
        const char *value = ValkeyModule_StringPtrLen(argv[i + 1], &vallen);
        
        // Optimize string comparisons using length check first
        if (keylen == 4 && strcasecmp(key, "path") == 0) {
            if (UNLIKELY_BRANCH(config.db_path)) free(config.db_path);
            config.db_path = strdup(value);
        } else if (keylen == 17 && strcasecmp(key, "create_if_missing") == 0) {
            config.create_if_missing = (int)fast_atoll(value, vallen);
        } else if (keylen == 15 && strcasecmp(key, "error_if_exists") == 0) {
            config.error_if_exists = (int)fast_atoll(value, vallen);
        } else if (keylen == 15 && strcasecmp(key, "paranoid_checks") == 0) {
            config.paranoid_checks = (int)fast_atoll(value, vallen);
        } else if (keylen == 11 && strcasecmp(key, "compression") == 0) {
            config.compression = (int)fast_atoll(value, vallen);
        } else if (keylen == 17 && strcasecmp(key, "write_buffer_size") == 0) {
            config.write_buffer_size = (size_t)fast_atoll(value, vallen);
        } else if (keylen == 14 && strcasecmp(key, "max_open_files") == 0) {
            config.max_open_files = (int)fast_atoll(value, vallen);
        } else if (keylen == 10 && strcasecmp(key, "block_size") == 0) {
            config.block_size = (size_t)fast_atoll(value, vallen);
        } else if (keylen == 22 && strcasecmp(key, "block_restart_interval") == 0) {
            config.block_restart_interval = (int)fast_atoll(value, vallen);
        } else if (keylen == 13 && strcasecmp(key, "max_file_size") == 0) {
            config.max_file_size = (size_t)fast_atoll(value, vallen);
        }
    }
    
    if (UNLIKELY_BRANCH(!config.db_path)) {
        config.db_path = strdup("/tmp/dicedb-l2");
    }
}

int COLD_FUNCTION InitRocksDB(ValkeyModuleCtx *ctx) {
    VALKEYMODULE_NOT_USED(ctx);
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
    rocksdb_options_optimize_for_point_lookup(options, 64);
    rocksdb_options_set_allow_mmap_reads(options, 1);
    rocksdb_options_set_allow_mmap_writes(options, 1);
    rocksdb_options_set_max_background_compactions(options, 2);
    rocksdb_options_set_level_compaction_dynamic_level_bytes(options, 1);
    
    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();

    // Add LRU cache for better performance
    rocksdb_cache_t *cache = rocksdb_cache_create_lru(LRU_CACHE_SIZE);
    rocksdb_block_based_options_set_block_cache(table_options, cache);

    // Add bloom filter for faster negative lookups
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
        free(err);
        return VALKEYMODULE_ERR;
    }
    
    roptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(roptions, 0);
    rocksdb_readoptions_set_fill_cache(roptions, 1);
    woptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(woptions, 0);
    rocksdb_writeoptions_disable_WAL(woptions, 0);

    batch = rocksdb_writebatch_create();
    
    // RocksDB initialized successfully
    
    return VALKEYMODULE_OK;
}

void FlushBatch() {
    if (batch && db && batch_count > 0) {
        char *err = NULL;
        rocksdb_write(db, woptions, batch, &err);
        if (err) {
            free(err);
        }
        rocksdb_writebatch_clear(batch);
        batch_count = 0;
    }
}

void COLD_FUNCTION CleanupRocksDB() {
    FlushBatch();
    if (batch) {
        rocksdb_writebatch_destroy(batch);
        batch = NULL;
    }
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

int HOT_FUNCTION PremissNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);
    if (unlikely(!event || !key || !db)) return 0;
    // Use memcmp for fixed-length string comparison (faster)
    if (likely(memcmp(event, "premiss\0", 8) == 0)) {
        size_t keylen;
        const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);

        char *err = NULL;
        size_t vallen;
        char *val = rocksdb_get(db, roptions, keyname, keylen, &vallen, &err);

        if (err != NULL) {
            free(err);
            return 0;
        }

        if (UNLIKELY_BRANCH(val == NULL)) {
            return 0;
        }

        // Prefetch the data we're about to access
        prefetch(val);
        if (LIKELY_BRANCH(vallen > 64)) {
            prefetch(val + 64);  // Prefetch next cache line
        }

        if (UNLIKELY_BRANCH(vallen < HEADER_SIZE)) {
            free(val);
            return 0;
        }

        char *ptr = val;

        // Use direct pointer casting for better performance on aligned data
        int64_t absolute_expire_ms = *(int64_t*)ptr;
        ptr += sizeof(int64_t);

        uint32_t dump_len = *(uint32_t*)ptr;
        ptr += sizeof(uint32_t);

        if (UNLIKELY_BRANCH(vallen != HEADER_SIZE + dump_len)) {
            free(val);
            return 0;
        }

        if (LIKELY_BRANCH(absolute_expire_ms > 0)) {
            int64_t current_ms = GetCurrentTimeMs();
            if (UNLIKELY_BRANCH(current_ms < 0)) {
                free(val);
                return 0;
            }

            if (UNLIKELY_BRANCH(absolute_expire_ms <= current_ms)) {
                free(val);
                DeleteKeyFromDB(keyname, keylen, "after expiration");
                return 0;
            }
        }

        ValkeyModuleString *dump_data = ValkeyModule_CreateString(ctx, ptr, dump_len);

        ValkeyModuleCallReply *reply;
        if (absolute_expire_ms > 0) {
            reply = ValkeyModule_Call(ctx, "RESTORE", "slscc", key, absolute_expire_ms/1000, dump_data, "REPLACE", "ABSTTL");
        } else {
            reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", key, 0, dump_data, "REPLACE");
        }

        free(val);
        ValkeyModule_FreeString(ctx, dump_data);

        if (UNLIKELY_BRANCH(reply && ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR)) {
            // Restore failed, keep data in RocksDB
        } else {
            __sync_fetch_and_add(&stats.keys_restored, 1);
            DeleteKeyFromDB(keyname, keylen, "after restore");
        }

        if (reply) {
            ValkeyModule_FreeCallReply(reply);
        }
    }

    return 0;
}

int HOT_FUNCTION PreevictionKeyNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);
    if (unlikely(!event || !key || !db)) return 0;
    // Use memcmp for fixed-length string comparison (faster)
    if (likely(memcmp(event, "preeviction\0", 12) == 0)) {
        size_t keylen;
        const char *keyname = ValkeyModule_StringPtrLen(key, &keylen);
        
        ValkeyModuleCallReply *dump_reply = ValkeyModule_Call(ctx, "DUMP", "s", key);
        if (UNLIKELY_BRANCH(!dump_reply)) {
            return 0;
        }

        ValkeyModuleCallReply *ttl_reply = ValkeyModule_Call(ctx, "PTTL", "s", key);
        
        if (LIKELY_BRANCH(dump_reply && ValkeyModule_CallReplyType(dump_reply) == VALKEYMODULE_REPLY_STRING)) {
            size_t dump_len;
            const char *dump_data = ValkeyModule_CallReplyStringPtr(dump_reply, &dump_len);
            
            if (LIKELY_BRANCH(dump_data && dump_len >= MIN_DUMP_SIZE)) {
                long long pttl = -1;
                if (LIKELY_BRANCH(ttl_reply && ValkeyModule_CallReplyType(ttl_reply) == VALKEYMODULE_REPLY_INTEGER)) {
                    pttl = ValkeyModule_CallReplyInteger(ttl_reply);
                }
                
                int64_t absolute_expire_ms = 0;
                if (LIKELY_BRANCH(pttl > 0 && pttl < MAX_REASONABLE_TTL)) {
                    int64_t current_ms = GetCurrentTimeMs();
                    if (likely(current_ms > 0)) {
                        absolute_expire_ms = current_ms + pttl;
                    }
                }
                
                size_t total_len = HEADER_SIZE + dump_len;
                char stack_buffer[STACK_BUFFER_SIZE];
                char *combined_data;
                int use_stack = (total_len <= STACK_BUFFER_SIZE);

                if (use_stack) {
                    combined_data = stack_buffer;
                } else {
                    combined_data = malloc(total_len);
                    if (UNLIKELY_BRANCH(!combined_data)) {
                        // Memory allocation failed
                    }
                }

                if (LIKELY_BRANCH(combined_data)) {
                    char *ptr = combined_data;
                    
                    // Use direct assignment for better performance
                    *(int64_t*)ptr = absolute_expire_ms;
                    ptr += sizeof(int64_t);

                    *(uint32_t*)ptr = (uint32_t)dump_len;
                    ptr += sizeof(uint32_t);
                    
                    // Optimized memory copy based on size
                    if (dump_len >= SIMD_THRESHOLD) {
                        // Ensure 16-byte alignment for SIMD
                        if (((uintptr_t)ptr & 15) == 0 && ((uintptr_t)dump_data & 15) == 0) {
                            size_t simd_len = dump_len & ~15;  // Round down to 16-byte boundary
                            const __m128i *src = (const __m128i*)dump_data;
                            __m128i *dst = (__m128i*)ptr;

                            // Prefetch next cache lines
                            prefetch((char*)src + 64);
                            prefetch_write((char*)dst + 64);

                            // Unroll SIMD loop for better performance
                            size_t i = 0;
                            for (; i + 64 <= simd_len; i += 64) {
                                // Process 4 * 16-byte chunks at once
                                _mm_store_si128(dst, _mm_load_si128(src));
                                _mm_store_si128(dst + 1, _mm_load_si128(src + 1));
                                _mm_store_si128(dst + 2, _mm_load_si128(src + 2));
                                _mm_store_si128(dst + 3, _mm_load_si128(src + 3));
                                dst += 4;
                                src += 4;
                                // Prefetch ahead
                                prefetch((char*)src + 128);
                                prefetch_write((char*)dst + 128);
                            }
                            // Handle remaining 16-byte chunks
                            for (; i < simd_len; i += 16) {
                                _mm_store_si128(dst++, _mm_load_si128(src++));
                            }
                            // Copy remaining bytes
                            if (dump_len & 15) {
                                memcpy((char*)dst, (char*)src, dump_len & 15);
                            }
                        } else {
                            memcpy(ptr, dump_data, dump_len);
                        }
                    } else if (dump_len <= SMALL_COPY_THRESHOLD) {
                        // Optimized small copy with alignment checks
                        if (((uintptr_t)ptr & 7) == 0 && ((uintptr_t)dump_data & 7) == 0) {
                            switch (dump_len) {
                                case 1: *(uint8_t*)ptr = *(uint8_t*)dump_data; break;
                                case 2: *(uint16_t*)ptr = *(uint16_t*)dump_data; break;
                                case 4: *(uint32_t*)ptr = *(uint32_t*)dump_data; break;
                                case 8: *(uint64_t*)ptr = *(uint64_t*)dump_data; break;
                                default: memcpy(ptr, dump_data, dump_len);
                            }
                        } else {
                            memcpy(ptr, dump_data, dump_len);
                        }
                    } else {
                        memcpy(ptr, dump_data, dump_len);
                    }
                    
                    // Use batch operations for better performance
                    if (batch) {
                        rocksdb_writebatch_put(batch, keyname, keylen, combined_data, total_len);
                        __sync_fetch_and_add(&batch_count, 1);
                        __sync_fetch_and_add(&stats.keys_stored, 1);
                        __sync_fetch_and_add(&stats.bytes_written, total_len);

                        // Flush batch if it reaches threshold
                        if (batch_count >= BATCH_SIZE_THRESHOLD) {
                            FlushBatch();
                        }
                    } else {
                        // Fallback to direct write if batch is not available
                        char *err = NULL;
                        rocksdb_put(db, woptions, keyname, keylen, combined_data, total_len, &err);
                        if (err != NULL) {
                            free(err);
                        } else {
                            __sync_fetch_and_add(&stats.keys_stored, 1);
                            __sync_fetch_and_add(&stats.bytes_written, total_len);
                        }
                    }
                    
                    if (UNLIKELY_BRANCH(!use_stack)) {
                        free(combined_data);
                    }
                }
            }
        } else {
            // DUMP failed
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

int HOT_FUNCTION RestoreCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (UNLIKELY_BRANCH(argc != 2)) {
        return ValkeyModule_WrongArity(ctx);
    }
    
    if (unlikely(!db)) {
        return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_DB_NOT_INIT].msg);
    }
    
    size_t keylen;
    const char *keyname = ValkeyModule_StringPtrLen(argv[1], &keylen);

    // Validate key length and basic safety checks
    if (UNLIKELY_BRANCH(keylen == 0 || keylen > MAX_KEY_SIZE)) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Invalid key length");
    }

    // Safety check for null pointer
    if (UNLIKELY_BRANCH(!keyname)) {
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
    
    if (UNLIKELY_BRANCH(val == NULL)) {
        return ValkeyModule_ReplyWithNull(ctx);
    }

    // Prefetch the data we're about to access
    prefetch(val);
    if (LIKELY_BRANCH(vallen > 64)) {
        prefetch(val + 64);  // Prefetch next cache line
    }
    
    if (vallen < HEADER_SIZE) {
        free(val);
        return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_CORRUPTED_DATA].msg);
    }

    char *ptr = val;

    // Use direct pointer casting for better performance
    int64_t absolute_expire_ms = *(int64_t*)ptr;
    ptr += sizeof(int64_t);

    uint32_t dump_len = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);

    if (vallen != HEADER_SIZE + dump_len) {
        free(val);
        return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_LENGTH_MISMATCH].msg);
    }
    
    long long ttl_ms = 0;
    if (absolute_expire_ms > 0) {
        int64_t current_ms = GetCurrentTimeMs();
        if (current_ms < 0) {
            free(val);
            return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_TIME_FAILED].msg);
        }
        ttl_ms = absolute_expire_ms - current_ms;

        if (ttl_ms <= 0) {
            free(val);
            DeleteKeyFromDB(keyname, keylen, "expired key");
            __sync_fetch_and_add(&stats.keys_expired, 1);
            return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_KEY_EXPIRED].msg);
        }
    }

    ValkeyModuleString *dump_data = ValkeyModule_CreateString(ctx, ptr, dump_len);

    ValkeyModuleCallReply *reply;
    if (ttl_ms > 0) {
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", argv[1], ttl_ms, dump_data, "!");
    } else {
        reply = ValkeyModule_Call(ctx, "RESTORE", "slsc", argv[1], 0, dump_data, "!");
    }
    
    free(val);
    ValkeyModule_FreeString(ctx, dump_data);
    
    if (reply && ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        const char *errmsg = ValkeyModule_CallReplyStringPtr(reply, NULL);
        ValkeyModule_ReplyWithError(ctx, errmsg);
    } else {
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        __sync_fetch_and_add(&stats.keys_restored, 1);
        __sync_fetch_and_add(&stats.bytes_read, vallen);

        DeleteKeyFromDB(keyname, keylen, "after successful restore");
    }
    
    if (reply) {
        ValkeyModule_FreeCallReply(reply);
    }
    
    return VALKEYMODULE_OK;
}

// Command to clean up expired keys using iterator
int CleanupCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (unlikely(!db)) {
        return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_DB_NOT_INIT].msg);
    }

    rocksdb_iterator_t *iter = rocksdb_create_iterator(db, roptions);
    if (UNLIKELY_BRANCH(!iter)) {
        return ValkeyModule_ReplyWithError(ctx, "ERR Failed to create iterator");
    }

    int64_t current_ms = GetCurrentTimeMs();
    if (current_ms < 0) {
        rocksdb_iter_destroy(iter);
        return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_TIME_FAILED].msg);
    }

    int cleaned = 0;
    rocksdb_writebatch_t *cleanup_batch = rocksdb_writebatch_create();

    for (rocksdb_iter_seek_to_first(iter); LIKELY_BRANCH(rocksdb_iter_valid(iter)); rocksdb_iter_next(iter)) {
        size_t vallen;
        const char *val = rocksdb_iter_value(iter, &vallen);

        if (LIKELY_BRANCH(vallen >= HEADER_SIZE)) {
            int64_t absolute_expire_ms;
            memcpy(&absolute_expire_ms, val, sizeof(int64_t));

            if (UNLIKELY_BRANCH(absolute_expire_ms > 0 && absolute_expire_ms <= current_ms)) {
                size_t keylen;
                const char *key = rocksdb_iter_key(iter, &keylen);
                rocksdb_writebatch_delete(cleanup_batch, key, keylen);
                cleaned++;
                __sync_fetch_and_add(&stats.keys_expired, 1);
            }
        }
    }

    if (cleaned > 0) {
        char *err = NULL;
        rocksdb_write(db, woptions, cleanup_batch, &err);
        if (err) {
            free(err);
        }
    }

    rocksdb_writebatch_destroy(cleanup_batch);
    rocksdb_iter_destroy(iter);

    return ValkeyModule_ReplyWithLongLong(ctx, cleaned);
}

// Command to get statistics
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
    
    if (unlikely(!db)) {
        return ValkeyModule_ReplyWithError(ctx, ERROR_MESSAGES[ERR_DB_NOT_INIT].msg);
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

int COLD_FUNCTION ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "infcache", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    
    ParseModuleArgs(argv, argc);
    
    if (InitRocksDB(ctx) != VALKEYMODULE_OK) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }
    
    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREEVICTION, PreevictionKeyNotification) != VALKEYMODULE_OK) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREMISS, PremissNotification) != VALKEYMODULE_OK) {
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

    if (ValkeyModule_CreateCommand(ctx, "infcache.cleanup", CleanupCommand, "write", 0, 0, 0) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "infcache.stats", StatsCommand, "readonly", 0, 0, 0) == VALKEYMODULE_ERR) {
        CleanupRocksDB();
        return VALKEYMODULE_ERR;
    }
    
    // Infcache module loaded successfully
    
    return VALKEYMODULE_OK;
}

int COLD_FUNCTION ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    VALKEYMODULE_NOT_USED(ctx);
    CleanupRocksDB();
    return VALKEYMODULE_OK;
}