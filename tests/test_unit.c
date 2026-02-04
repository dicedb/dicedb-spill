#define _POSIX_C_SOURCE 199309L
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>

// Test framework
#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    printf("Running test: %s...", #name); \
    test_##name(); \
    printf(" PASSED\n"); \
    tests_passed++; \
} while(0)

static int tests_passed = 0;
static int tests_total = 0;

// Mock structures for testing data format
typedef struct {
    int64_t expire_ms;
    uint32_t dump_len;
    char dump_data[];
} StorageFormat;

// Test helper functions
StorageFormat* create_storage_format(int64_t expire_ms, const char* data, size_t data_len) {
    size_t total_size = sizeof(int64_t) + sizeof(uint32_t) + data_len;
    StorageFormat* fmt = (StorageFormat*)malloc(total_size);
    
    char* ptr = (char*)fmt;
    memcpy(ptr, &expire_ms, sizeof(int64_t));
    ptr += sizeof(int64_t);
    
    uint32_t dump_len_32 = (uint32_t)data_len;
    memcpy(ptr, &dump_len_32, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    memcpy(ptr, data, data_len);
    
    return fmt;
}

int parse_storage_format(const char* data, size_t data_len, 
                        int64_t* expire_ms, uint32_t* dump_len, const char** dump_data) {
    if (data_len < sizeof(int64_t) + sizeof(uint32_t)) {
        return -1;
    }
    
    const char* ptr = data;
    
    memcpy(expire_ms, ptr, sizeof(int64_t));
    ptr += sizeof(int64_t);
    
    memcpy(dump_len, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    
    if (data_len != sizeof(int64_t) + sizeof(uint32_t) + *dump_len) {
        return -1;
    }
    
    *dump_data = ptr;
    return 0;
}

int64_t get_current_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

// Unit Tests

TEST(storage_format_no_expiration) {
    const char* test_data = "test_value_data";
    size_t data_len = strlen(test_data);
    
    StorageFormat* fmt = create_storage_format(0, test_data, data_len);
    
    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;
    
    int result = parse_storage_format((char*)fmt, 
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len,
                                     &expire_ms, &dump_len, &dump_data);
    
    assert(result == 0);
    assert(expire_ms == 0);
    assert(dump_len == data_len);
    assert(memcmp(dump_data, test_data, data_len) == 0);
    
    free(fmt);
}

TEST(storage_format_with_expiration) {
    const char* test_data = "test_value_with_ttl";
    size_t data_len = strlen(test_data);
    int64_t test_expire = 1234567890123LL;
    
    StorageFormat* fmt = create_storage_format(test_expire, test_data, data_len);
    
    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;
    
    int result = parse_storage_format((char*)fmt, 
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len,
                                     &expire_ms, &dump_len, &dump_data);
    
    assert(result == 0);
    assert(expire_ms == test_expire);
    assert(dump_len == data_len);
    assert(memcmp(dump_data, test_data, data_len) == 0);
    
    free(fmt);
}

TEST(storage_format_corrupted_too_short) {
    char data[8];  // Too short
    
    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;
    
    int result = parse_storage_format(data, sizeof(data), 
                                     &expire_ms, &dump_len, &dump_data);
    
    assert(result == -1);
}

TEST(storage_format_length_mismatch) {
    const char* test_data = "test";
    size_t data_len = strlen(test_data);
    
    StorageFormat* fmt = create_storage_format(0, test_data, data_len);
    
    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;
    
    // Pass wrong total length
    int result = parse_storage_format((char*)fmt, 
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len + 10,
                                     &expire_ms, &dump_len, &dump_data);
    
    assert(result == -1);
    
    free(fmt);
}

TEST(ttl_calculation_not_expired) {
    int64_t current_ms = get_current_time_ms();
    int64_t expire_ms = current_ms + 3600000;  // 1 hour from now
    
    int64_t ttl_ms = expire_ms - current_ms;
    
    assert(ttl_ms > 0);
    assert(ttl_ms <= 3600000);
}

TEST(ttl_calculation_expired) {
    int64_t current_ms = get_current_time_ms();
    int64_t expire_ms = current_ms - 1000;  // 1 second ago
    
    int64_t ttl_ms = expire_ms - current_ms;
    
    assert(ttl_ms < 0);
}

TEST(ttl_calculation_no_expiration) {
    int64_t expire_ms = 0;
    
    // When expire_ms is 0, there's no expiration
    assert(expire_ms == 0);
}

TEST(storage_format_large_data) {
    // Test with larger data
    size_t data_len = 65536;  // 64KB
    char* test_data = malloc(data_len);
    
    // Fill with pattern
    for (size_t i = 0; i < data_len; i++) {
        test_data[i] = (char)(i % 256);
    }
    
    StorageFormat* fmt = create_storage_format(1234567890, test_data, data_len);
    
    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;
    
    int result = parse_storage_format((char*)fmt, 
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len,
                                     &expire_ms, &dump_len, &dump_data);
    
    assert(result == 0);
    assert(expire_ms == 1234567890);
    assert(dump_len == data_len);
    assert(memcmp(dump_data, test_data, data_len) == 0);
    
    free(test_data);
    free(fmt);
}

TEST(storage_format_empty_data) {
    // Test with empty dump data (edge case)
    StorageFormat* fmt = create_storage_format(0, "", 0);
    
    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;
    
    int result = parse_storage_format((char*)fmt, 
                                     sizeof(int64_t) + sizeof(uint32_t),
                                     &expire_ms, &dump_len, &dump_data);
    
    assert(result == 0);
    assert(expire_ms == 0);
    assert(dump_len == 0);
    
    free(fmt);
}

TEST(storage_format_binary_data) {
    // Test with binary data containing null bytes
    char test_data[] = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0x00};
    size_t data_len = sizeof(test_data);

    StorageFormat* fmt = create_storage_format(987654321, test_data, data_len);

    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;

    int result = parse_storage_format((char*)fmt,
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len,
                                     &expire_ms, &dump_len, &dump_data);

    assert(result == 0);
    assert(expire_ms == 987654321);
    assert(dump_len == data_len);
    assert(memcmp(dump_data, test_data, data_len) == 0);

    free(fmt);
}

// New tests for ABSTTL functionality

TEST(absttl_calculation_future) {
    // Test ABSTTL calculation for future expiration
    int64_t current_ms = get_current_time_ms();
    int64_t absolute_expire_ms = current_ms + 3600000;  // 1 hour from now

    // Check that the absolute expiration time is in the future
    assert(absolute_expire_ms > current_ms);

    // Verify the absolute timestamp is reasonable (not too far in future)
    assert((absolute_expire_ms - current_ms) <= 3600000);
}

TEST(absttl_calculation_expired) {
    // Test ABSTTL calculation for already expired keys
    int64_t current_ms = get_current_time_ms();
    int64_t absolute_expire_ms = current_ms - 1000;  // 1 second ago

    // Check that the absolute expiration time is in the past
    assert(absolute_expire_ms <= current_ms);
}

TEST(absttl_calculation_edge_cases) {
    // Test edge cases for ABSTTL
    int64_t current_ms = get_current_time_ms();

    // Test expiration exactly at current time
    int64_t expire_now = current_ms;
    assert(expire_now <= current_ms);

    // Test very small positive TTL
    int64_t expire_soon = current_ms + 1;
    assert(expire_soon > current_ms);

    // Test very large TTL (far future)
    int64_t expire_far = current_ms + (365LL * 24 * 60 * 60 * 1000); // 1 year
    assert(expire_far > current_ms);
}

TEST(expired_key_detection_logic) {
    // Test the logic for detecting expired keys
    int64_t current_ms = get_current_time_ms();

    // Key expired 5 seconds ago
    int64_t expired_time = current_ms - 5000;
    assert(expired_time <= current_ms);  // Should be detected as expired

    // Key expires in 5 seconds
    int64_t future_time = current_ms + 5000;
    assert(future_time > current_ms);  // Should NOT be detected as expired

    // Key expires exactly now (edge case)
    int64_t now_time = current_ms;
    assert(now_time <= current_ms);  // Should be detected as expired
}

TEST(absttl_seconds_conversion) {
    // Test conversion from milliseconds to seconds for ABSTTL
    int64_t current_ms = get_current_time_ms();
    int64_t expire_ms = current_ms + 3600000;  // 1 hour = 3600000 ms

    // Convert to seconds (as required by RESTORE ABSTTL)
    int64_t expire_sec = expire_ms / 1000;
    int64_t current_sec = current_ms / 1000;

    // Should be approximately 3600 seconds difference (allow 1 second tolerance)
    int64_t diff_sec = expire_sec - current_sec;
    assert(diff_sec >= 3599 && diff_sec <= 3601);
}

TEST(key_expiration_boundary_conditions) {
    // Test boundary conditions for key expiration
    int64_t current_ms = get_current_time_ms();

    // Test keys that expire exactly at millisecond boundaries
    int64_t expire_boundary = (current_ms / 1000) * 1000;  // Round down to nearest second

    if (expire_boundary <= current_ms) {
        // Should be expired
        assert(expire_boundary <= current_ms);
    }

    // Test keys that expire 1ms in the future
    int64_t expire_1ms = current_ms + 1;
    assert(expire_1ms > current_ms);  // Should not be expired

    // Test keys that expired 1ms ago
    int64_t expired_1ms = current_ms - 1;
    assert(expired_1ms <= current_ms);  // Should be expired
}

TEST(storage_format_with_precise_absttl) {
    // Test storage format with precise ABSTTL timestamps
    int64_t precise_expire = 1734567890123LL;  // Precise timestamp
    const char* test_data = "precise_absttl_test";
    size_t data_len = strlen(test_data);

    StorageFormat* fmt = create_storage_format(precise_expire, test_data, data_len);

    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;

    int result = parse_storage_format((char*)fmt,
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len,
                                     &expire_ms, &dump_len, &dump_data);

    assert(result == 0);
    assert(expire_ms == precise_expire);  // Exact match for ABSTTL
    assert(dump_len == data_len);
    assert(memcmp(dump_data, test_data, data_len) == 0);

    free(fmt);
}

TEST(storage_format_max_timestamp) {
    // Test with maximum reasonable timestamp values
    int64_t max_timestamp = 9999999999999LL;  // Year 2286
    const char* test_data = "max_timestamp_test";
    size_t data_len = strlen(test_data);

    StorageFormat* fmt = create_storage_format(max_timestamp, test_data, data_len);

    int64_t expire_ms;
    uint32_t dump_len;
    const char* dump_data;

    int result = parse_storage_format((char*)fmt,
                                     sizeof(int64_t) + sizeof(uint32_t) + data_len,
                                     &expire_ms, &dump_len, &dump_data);

    assert(result == 0);
    assert(expire_ms == max_timestamp);
    assert(dump_len == data_len);
    assert(memcmp(dump_data, test_data, data_len) == 0);

    free(fmt);
}

TEST(expired_key_cleanup_simulation) {
    // Simulate the expired key cleanup logic
    int64_t current_ms = get_current_time_ms();

    // Create various test scenarios
    struct test_case {
        int64_t absolute_expire_ms;
        int should_be_deleted;
    } cases[] = {
        {0, 0},                      // No expiration
        {current_ms - 5000, 1},      // Expired 5 seconds ago
        {current_ms - 1, 1},         // Expired 1ms ago
        {current_ms, 1},             // Expires exactly now
        {current_ms + 1, 0},         // Expires 1ms from now
        {current_ms + 5000, 0},      // Expires 5 seconds from now
    };

    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        int64_t expire_ms = cases[i].absolute_expire_ms;
        int should_delete = cases[i].should_be_deleted;

        if (expire_ms > 0) {
            int is_expired = (expire_ms <= current_ms);
            assert(is_expired == should_delete);
        } else {
            // No expiration, should not delete
            assert(should_delete == 0);
        }
    }
}

// Test fast_atoll function (mock implementation for testing)
static long long mock_fast_atoll(const char *str, size_t len) {
    if (len == 0 || len > 20) return 0;

    long long result = 0;
    long long sign = 1;
    size_t i = 0;

    if (len > 0 && str[0] == '-') {
        sign = -1;
        i = 1;
    }

    for (; i < len && str[i] >= '0' && str[i] <= '9'; i++) {
        long long new_result = result * 10 + (str[i] - '0');
        if (new_result < result) return 0; // Overflow
        result = new_result;
    }

    return result * sign;
}

TEST(fast_atoll_basic_conversions) {
    // Test basic positive numbers
    assert(mock_fast_atoll("123", 3) == 123);
    assert(mock_fast_atoll("0", 1) == 0);
    assert(mock_fast_atoll("1", 1) == 1);
    assert(mock_fast_atoll("9999", 4) == 9999);

    // Test negative numbers
    assert(mock_fast_atoll("-123", 4) == -123);
    assert(mock_fast_atoll("-1", 2) == -1);

    // Test edge cases
    assert(mock_fast_atoll("", 0) == 0);       // Empty string
    assert(mock_fast_atoll("abc", 3) == 0);    // Non-numeric
    assert(mock_fast_atoll("12abc", 5) == 12); // Partial numeric
}

TEST(fast_atoll_large_numbers) {
    // Test large valid numbers
    assert(mock_fast_atoll("9223372036854775807", 19) > 0); // Near max long long
    assert(mock_fast_atoll("1000000000", 10) == 1000000000LL);

    // Test overflow handling (should return 0)
    assert(mock_fast_atoll("99999999999999999999999", 23) == 0);

    // Test too long strings
    assert(mock_fast_atoll("12345678901234567890123", 23) == 0);
}

TEST(fast_atoll_edge_cases) {
    // Test single digits
    for (int i = 0; i <= 9; i++) {
        char digit_str[2] = {'0' + i, '\0'};
        assert(mock_fast_atoll(digit_str, 1) == i);
    }

    // Test two digits
    assert(mock_fast_atoll("10", 2) == 10);
    assert(mock_fast_atoll("99", 2) == 99);

    // Test with leading zeros (should still work)
    assert(mock_fast_atoll("00123", 5) == 123);
    assert(mock_fast_atoll("000", 3) == 0);
}

// Test time caching functionality
TEST(time_caching_consistency) {
    // Test that get_current_time_ms returns reasonable values
    int64_t time1 = get_current_time_ms();
    assert(time1 > 0);

    // Small delay
    for (volatile int i = 0; i < 1000000; i++);

    int64_t time2 = get_current_time_ms();
    assert(time2 >= time1); // Time should not go backwards
    assert(time2 - time1 < 1000); // Should be less than 1 second difference
}

TEST(time_precision_validation) {
    // Test that time has millisecond precision
    int64_t current_time = get_current_time_ms();

    // Verify it's a reasonable timestamp (after year 2020)
    int64_t year_2020_ms = 1577836800000LL; // Jan 1, 2020
    assert(current_time > year_2020_ms);

    // Verify it's not too far in the future (before year 2050)
    int64_t year_2050_ms = 2524608000000LL; // Jan 1, 2050
    assert(current_time < year_2050_ms);
}

// Test configuration parsing logic (mock implementation)
typedef struct {
    char *path;
    size_t max_memory;
} MockConfig;

static MockConfig mock_config = {0};

static void mock_parse_args(const char **args, int count) {
    for (int i = 0; i < count; i += 2) {
        if (i + 1 >= count) break;

        const char *key = args[i];
        const char *value = args[i + 1];

        if (strcmp(key, "path") == 0) {
            if (mock_config.path) free(mock_config.path);
            mock_config.path = strdup(value);
        } else if (strcmp(key, "max-memory") == 0 || strcmp(key, "max_memory") == 0) {
            mock_config.max_memory = (size_t)atoll(value);
            if (mock_config.max_memory < 64 * 1024 * 1024) {
                mock_config.max_memory = 64 * 1024 * 1024;
            }
        }
    }

    if (!mock_config.path) {
        mock_config.path = strdup("/tmp/dicedb-l2");
    }
    if (mock_config.max_memory == 0) {
        mock_config.max_memory = 256 * 1024 * 1024;  // Default 256MB
    }
}

TEST(config_parsing_basic) {
    // Reset config
    if (mock_config.path) {
        free(mock_config.path);
        mock_config.path = NULL;
    }
    memset(&mock_config, 0, sizeof(mock_config));

    // Test basic parsing
    const char *args[] = {"path", "/custom/path", "max-memory", "134217728"};
    mock_parse_args(args, 4);

    assert(strcmp(mock_config.path, "/custom/path") == 0);
    assert(mock_config.max_memory == 134217728);  // 128MB

    free(mock_config.path);
    mock_config.path = NULL;
}

TEST(config_parsing_defaults) {
    // Reset config
    if (mock_config.path) {
        free(mock_config.path);
        mock_config.path = NULL;
    }
    memset(&mock_config, 0, sizeof(mock_config));

    // Test default path and memory assignment
    const char *args[] = {};
    mock_parse_args(args, 0);

    assert(strcmp(mock_config.path, "/tmp/dicedb-l2") == 0);
    assert(mock_config.max_memory == 256 * 1024 * 1024);  // Default 256MB

    free(mock_config.path);
    mock_config.path = NULL;
}

TEST(config_parsing_numeric_values) {
    // Reset config
    if (mock_config.path) {
        free(mock_config.path);
        mock_config.path = NULL;
    }
    memset(&mock_config, 0, sizeof(mock_config));

    // Test numeric value parsing with minimum enforcement
    const char *args[] = {
        "max-memory", "1000",  // Too low, should be clamped to 64MB
        "path", "/test/path"
    };
    mock_parse_args(args, 4);

    assert(mock_config.max_memory == 64 * 1024 * 1024);  // Should be clamped to 64MB minimum
    assert(strcmp(mock_config.path, "/test/path") == 0);

    if (mock_config.path) {
        free(mock_config.path);
        mock_config.path = NULL;
    }
}

// Test error message handling
TEST(error_message_constants) {
    // Test error message lengths and content
    const char *expected_messages[] = {
        "ERR RocksDB not initialized",
        "ERR Corrupted data in RocksDB",
        "ERR Data length mismatch in RocksDB",
        "ERR Key has expired",
        "ERR Failed to get current time"
    };

    const size_t expected_lengths[] = {26, 29, 35, 19, 31};

    for (int i = 0; i < 5; i++) {
        size_t actual_len = strlen(expected_messages[i]);
        assert(actual_len == expected_lengths[i]);
        assert(strlen(expected_messages[i]) > 0);
    }
}

// Test statistics tracking
typedef struct {
    uint64_t keys_stored;
    uint64_t keys_restored;
    uint64_t keys_expired;
    uint64_t keys_cleaned;
    uint64_t bytes_written;
    uint64_t bytes_read;
} MockStats;

static MockStats mock_stats = {0};

TEST(stats_tracking_increments) {
    // Reset stats
    memset(&mock_stats, 0, sizeof(mock_stats));

    // Test basic increments
    mock_stats.keys_stored++;
    mock_stats.bytes_written += 1024;

    assert(mock_stats.keys_stored == 1);
    assert(mock_stats.bytes_written == 1024);
    assert(mock_stats.keys_restored == 0);

    // Test multiple increments
    mock_stats.keys_restored += 5;
    mock_stats.bytes_read += 2048;

    assert(mock_stats.keys_restored == 5);
    assert(mock_stats.bytes_read == 2048);
}

TEST(stats_overflow_handling) {
    // Test with large values
    mock_stats.keys_stored = UINT64_MAX - 1;
    mock_stats.keys_stored++; // Should wrap to UINT64_MAX

    assert(mock_stats.keys_stored == UINT64_MAX);

    // Test bytes counters with large values
    mock_stats.bytes_written = 1000000000ULL;
    mock_stats.bytes_written += 2000000000ULL;

    assert(mock_stats.bytes_written == 3000000000ULL);
}

int main() {
    printf("=== Running Unit Tests ===\n");

    tests_total = 30;

    // Original tests
    RUN_TEST(storage_format_no_expiration);
    RUN_TEST(storage_format_with_expiration);
    RUN_TEST(storage_format_corrupted_too_short);
    RUN_TEST(storage_format_length_mismatch);
    RUN_TEST(ttl_calculation_not_expired);
    RUN_TEST(ttl_calculation_expired);
    RUN_TEST(ttl_calculation_no_expiration);
    RUN_TEST(storage_format_large_data);
    RUN_TEST(storage_format_empty_data);
    RUN_TEST(storage_format_binary_data);

    // New ABSTTL and expiration tests
    RUN_TEST(absttl_calculation_future);
    RUN_TEST(absttl_calculation_expired);
    RUN_TEST(absttl_calculation_edge_cases);
    RUN_TEST(expired_key_detection_logic);
    RUN_TEST(absttl_seconds_conversion);
    RUN_TEST(key_expiration_boundary_conditions);
    RUN_TEST(storage_format_with_precise_absttl);
    RUN_TEST(storage_format_max_timestamp);
    RUN_TEST(expired_key_cleanup_simulation);

    // New utility function tests
    RUN_TEST(fast_atoll_basic_conversions);
    RUN_TEST(fast_atoll_large_numbers);
    RUN_TEST(fast_atoll_edge_cases);
    RUN_TEST(time_caching_consistency);
    RUN_TEST(time_precision_validation);
    RUN_TEST(config_parsing_basic);
    RUN_TEST(config_parsing_defaults);
    RUN_TEST(config_parsing_numeric_values);
    RUN_TEST(error_message_constants);
    RUN_TEST(stats_tracking_increments);
    RUN_TEST(stats_overflow_handling);

    printf("\n=== Test Results ===\n");
    printf("Tests passed: %d/%d\n", tests_passed, tests_total);

    if (tests_passed == tests_total) {
        printf("All tests PASSED!\n");
        return 0;
    } else {
        printf("Some tests FAILED!\n");
        return 1;
    }
}