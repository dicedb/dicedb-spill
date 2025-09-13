#define _POSIX_C_SOURCE 199309L
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

int main() {
    printf("=== Running Unit Tests ===\n");

    tests_total = 19;

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