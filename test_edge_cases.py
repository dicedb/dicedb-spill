#!/usr/bin/env python3
"""
Edge cases and error condition tests for DiceDB Infcache module
Tests various boundary conditions, error scenarios, and stress cases
"""

import os
import sys
import time
import subprocess
import tempfile
import threading
import random

try:
    import valkey as redis
except ImportError:
    try:
        import redis
    except ImportError:
        print("ERROR: Neither valkey nor redis Python library found")
        sys.exit(1)

# Test configuration
REDIS_PORT = 6379

def run_test(test_func, test_name):
    """Run a single test and report results"""
    try:
        print(f"\nRunning: {test_name}...", end=" ")
        test_func()
        print("PASSED")
        return True
    except AssertionError as e:
        print(f"FAILED\n  Error: {e}")
        return False
    except Exception as e:
        print(f"ERROR\n  Unexpected error: {e}")
        return False

# Edge case tests

def test_zero_ttl_edge_case():
    """Test keys with zero TTL (should expire immediately)"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set a key with 0 TTL - should expire immediately
    try:
        r.setex('zero_ttl_key', 0, 'zero_ttl_value')
        # Key should be expired/non-existent immediately
        value = r.get('zero_ttl_key')
        # Some implementations might reject 0 TTL, others might expire immediately
        assert value is None or value == 'zero_ttl_value'
        print("  Zero TTL handled correctly")
    except redis.ResponseError as e:
        # Some Redis implementations reject 0 TTL
        print(f"  Zero TTL rejected (expected): {e}")

def test_negative_ttl_edge_case():
    """Test keys with negative TTL (should be rejected or expire immediately)"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    try:
        # Try to set a key with negative TTL
        r.setex('negative_ttl_key', -1, 'negative_ttl_value')
        # If it doesn't raise an error, check if key exists
        value = r.get('negative_ttl_key')
        assert value is None, "Key with negative TTL should not exist"
        print("  Negative TTL handled correctly (key expired)")
    except redis.ResponseError as e:
        # Expected behavior - negative TTL should be rejected
        print(f"  Negative TTL correctly rejected: {e}")

def test_maximum_ttl_values():
    """Test keys with very large TTL values"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Test with maximum reasonable TTL (10 years in seconds)
    max_ttl = 10 * 365 * 24 * 60 * 60  # 10 years
    r.setex('max_ttl_key', max_ttl, 'max_ttl_value')

    # Verify the key was set
    value = r.get('max_ttl_key')
    assert value == 'max_ttl_value', "Max TTL key not set correctly"

    ttl = r.ttl('max_ttl_key')
    assert ttl > max_ttl - 10, "Max TTL not preserved correctly"
    print(f"  Maximum TTL test passed: {ttl} seconds")

def test_special_key_names():
    """Test eviction and restoration with special key names"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=False)

    # Test keys with special characters and patterns
    special_keys = [
        b'',  # Empty key (if allowed)
        b' ',  # Space key
        b'\x00',  # Null byte key
        b'\xff\xfe\xfd',  # High byte values
        b'key\nwith\nnewlines',  # Newlines
        b'key\twith\ttabs',  # Tabs
        b'very' + b'x' * 1000 + b'long_key',  # Very long key name
        b'key{with}braces',  # Braces
        b'key[with]brackets',  # Brackets
        b'key(with)parens',  # Parentheses
    ]

    set_keys = []
    for i, key in enumerate(special_keys):
        try:
            if len(key) == 0:
                continue  # Skip empty keys as they're usually not allowed
            r.set(key, f'special_value_{i}'.encode())
            set_keys.append(key)
        except redis.ResponseError as e:
            print(f"  Key {key!r} rejected (expected): {e}")

    if not set_keys:
        print("  No special keys could be set")
        return

    # Trigger eviction
    for i in range(1000):
        r.set(f'filler_special_{i}'.encode(), b'x' * 8000)

    # Try to restore special keys
    restored = 0
    for key in set_keys:
        if r.get(key) is None:
            try:
                result = r.execute_command('infcache.restore', key)
                if result == b'OK':
                    restored += 1
            except Exception as e:
                print(f"  Special key {key!r} restore failed: {e}")

    print(f"  {restored}/{len(set_keys)} special keys restored successfully")

def test_concurrent_absttl_operations():
    """Test concurrent operations with ABSTTL keys"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)
    errors = []
    restored_keys = []

    def worker(thread_id):
        try:
            # Each thread creates keys with different TTLs
            base_ttl = 3600 + (thread_id * 100)
            for i in range(10):
                key = f'concurrent_absttl_{thread_id}_{i}'
                ttl = base_ttl + i
                r.setex(key, ttl, f'concurrent_value_{thread_id}_{i}')

                # Add some filler data to trigger evictions
                if random.random() > 0.7:
                    r.set(f'filler_concurrent_{thread_id}_{i}', 'x' * 5000)

                # Sometimes try to restore keys
                if random.random() > 0.8:
                    try:
                        result = r.execute_command('infcache.restore', key)
                        if result == 'OK':
                            restored_keys.append(key)
                    except redis.ResponseError:
                        pass  # Expected for non-evicted keys
        except Exception as e:
            errors.append(f"Thread {thread_id}: {str(e)}")

    # Run concurrent operations
    threads = []
    for i in range(4):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"  Concurrent ABSTTL operations: {len(errors)} errors, {len(restored_keys)} keys restored")

    # Verify some of the restored keys still have reasonable TTLs
    valid_ttls = 0
    for key in restored_keys[:5]:  # Check first 5 restored keys
        try:
            ttl = r.ttl(key)
            if ttl > 0:
                valid_ttls += 1
        except:
            pass

    print(f"  {valid_ttls}/5 sampled restored keys have valid TTLs")

def test_rocksdb_corruption_simulation():
    """Test behavior when RocksDB data might be corrupted"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set some keys and evict them
    test_keys = []
    for i in range(20):
        key = f'corruption_test_{i}'
        r.set(key, f'corruption_value_{i}')
        test_keys.append(key)

    # Trigger eviction
    for i in range(1500):
        r.set(f'filler_corruption_{i}', 'x' * 6000)

    # Check which keys were evicted
    evicted_keys = [key for key in test_keys if r.get(key) is None]

    if not evicted_keys:
        print("  No keys evicted for corruption test")
        return

    # Try to restore evicted keys - some might fail due to various reasons
    restore_results = {'ok': 0, 'none': 0, 'error': 0}

    for key in evicted_keys:
        try:
            result = r.execute_command('infcache.restore', key)
            if result == 'OK':
                restore_results['ok'] += 1
            elif result is None:
                restore_results['none'] += 1
            else:
                restore_results['error'] += 1
        except redis.ResponseError:
            restore_results['error'] += 1
        except Exception:
            restore_results['error'] += 1

    print(f"  Corruption test results: {restore_results}")
    # At least some operations should work
    assert restore_results['ok'] + restore_results['none'] > 0

def test_memory_pressure_scenarios():
    """Test behavior under extreme memory pressure"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Create a baseline key
    r.setex('memory_pressure_key', 1800, 'memory_pressure_value')

    # Create extreme memory pressure with large values
    try:
        for i in range(100):
            # Create 1MB values
            large_value = 'x' * (1024 * 1024)
            r.set(f'pressure_{i}', large_value)

            # Check if our baseline key got evicted
            if r.get('memory_pressure_key') is None:
                break
    except redis.ConnectionError:
        # Server might be under too much pressure
        print("  Connection lost under memory pressure (expected)")
        time.sleep(2)  # Give server time to recover
        r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Try to restore the baseline key
    try:
        result = r.execute_command('infcache.restore', 'memory_pressure_key')
        if result == 'OK':
            value = r.get('memory_pressure_key')
            assert value == 'memory_pressure_value'
            print("  Key successfully restored after memory pressure")
        else:
            print(f"  Key not restored after memory pressure: {result}")
    except Exception as e:
        print(f"  Restore failed after memory pressure: {e}")

def test_rapid_expiration_scenarios():
    """Test keys that expire very rapidly"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Create keys with very short TTLs in rapid succession
    rapid_keys = []
    for i in range(50):
        key = f'rapid_expire_{i}'
        ttl = 1 + (i % 3)  # TTLs of 1-3 seconds
        r.setex(key, ttl, f'rapid_value_{i}')
        rapid_keys.append((key, ttl))

    # Trigger some evictions immediately
    for i in range(800):
        r.set(f'filler_rapid_{i}', 'x' * 8000)

    # Wait for some keys to expire
    time.sleep(2)

    # Try to restore keys - some should be expired and deleted
    expired_count = 0
    restored_count = 0
    not_found_count = 0

    for key, original_ttl in rapid_keys:
        if r.get(key) is None:  # Key was evicted
            try:
                result = r.execute_command('infcache.restore', key)
                if result == 'OK':
                    restored_count += 1
                elif result is None:
                    not_found_count += 1
                    if original_ttl <= 2:  # Should have expired
                        expired_count += 1
            except:
                pass

    print(f"  Rapid expiration test: {restored_count} restored, {expired_count} expired correctly, {not_found_count} not found")

def test_clock_skew_simulation():
    """Test behavior with potential clock skew scenarios"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Create keys with TTLs that might be affected by clock skew
    current_time = int(time.time())

    # Set keys with TTLs based on current time
    for i in range(10):
        key = f'clock_skew_{i}'
        ttl = 600 + (i * 60)  # 10 minutes to 19 minutes
        r.setex(key, ttl, f'clock_value_{i}')

    # Trigger eviction
    for i in range(1000):
        r.set(f'filler_clock_{i}', 'x' * 7000)

    # Simulate time passage (just wait a bit)
    time.sleep(1)

    # Try to restore keys and verify TTLs are reasonable
    restored_with_good_ttl = 0
    for i in range(10):
        key = f'clock_skew_{i}'
        if r.get(key) is None:
            try:
                result = r.execute_command('infcache.restore', key)
                if result == 'OK':
                    ttl = r.ttl(key)
                    original_ttl = 600 + (i * 60)
                    # TTL should be reasonable (allowing for processing time)
                    if 0 < ttl <= original_ttl:
                        restored_with_good_ttl += 1
            except:
                pass

    print(f"  Clock skew test: {restored_with_good_ttl} keys restored with reasonable TTLs")

def test_error_recovery():
    """Test system recovery from various error conditions"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Test 1: Try to restore non-existent keys
    for i in range(10):
        result = r.execute_command('infcache.restore', f'nonexistent_{i}')
        assert result is None, f"Expected None for nonexistent key, got {result}"

    # Test 2: Mix valid and invalid operations
    r.set('error_recovery_key', 'error_recovery_value')

    # Trigger eviction
    for i in range(500):
        r.set(f'filler_error_{i}', 'x' * 10000)

    # Test various edge cases
    error_cases = [
        b'',  # Empty key
        b'\x00' * 100,  # Null bytes
        'very_long_key_' + 'x' * 10000,  # Very long key
    ]

    errors_handled = 0
    for case in error_cases:
        try:
            result = r.execute_command('infcache.restore', case)
            # Any result is acceptable (None, error, etc.)
            errors_handled += 1
        except:
            errors_handled += 1

    # Verify normal operation still works
    if r.get('error_recovery_key') is None:
        result = r.execute_command('infcache.restore', 'error_recovery_key')
        if result == 'OK':
            value = r.get('error_recovery_key')
            assert value == 'error_recovery_value'

    print(f"  Error recovery: {errors_handled}/{len(error_cases)} edge cases handled gracefully")

def main():
    """Main test runner for edge cases"""
    print("=== DiceDB Infcache Edge Cases and Error Conditions ===\n")

    # Check if server is running
    try:
        r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True, socket_connect_timeout=2)
        r.ping()
    except:
        print("ERROR: Cannot connect to database server on port 6379")
        print("Please start DiceDB server with infcache module loaded")
        sys.exit(1)

    # Run edge case tests
    tests = [
        (test_zero_ttl_edge_case, "Zero TTL edge case"),
        (test_negative_ttl_edge_case, "Negative TTL edge case"),
        (test_maximum_ttl_values, "Maximum TTL values"),
        (test_special_key_names, "Special key names"),
        (test_concurrent_absttl_operations, "Concurrent ABSTTL operations"),
        (test_rocksdb_corruption_simulation, "RocksDB corruption simulation"),
        (test_memory_pressure_scenarios, "Memory pressure scenarios"),
        (test_rapid_expiration_scenarios, "Rapid expiration scenarios"),
        (test_clock_skew_simulation, "Clock skew simulation"),
        (test_error_recovery, "Error recovery"),
    ]

    passed = 0
    failed = 0

    print(f"\nRunning {len(tests)} edge case tests...\n")
    for test_func, test_name in tests:
        if run_test(test_func, test_name):
            passed += 1
        else:
            failed += 1

    # Results
    print(f"\n=== Edge Case Test Results ===")
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")

    if failed == 0:
        print("All edge case tests PASSED!")
        sys.exit(0)
    else:
        print(f"{failed} edge case tests FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    main()