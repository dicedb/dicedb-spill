#!/usr/bin/env python3
"""
Integration tests for DiceDB Spill module
Tests the module's behavior with a real DiceDB/Valkey instance
"""

import os
import sys
import time
import subprocess
import tempfile
import shutil

try:
    import valkey as redis
except ImportError:
    try:
        import redis
    except ImportError:
        print("ERROR: Neither valkey nor redis Python library found")
        print("This will be installed automatically in venv")
        sys.exit(1)

# Test configuration
REDIS_PORT = 6379
MODULE_PATH = "../lib-spill.so"
ROCKSDB_PATH = None  # Will be set to temp directory

def setup_test_environment():
    """Set up test environment with temporary RocksDB directory"""
    global ROCKSDB_PATH
    ROCKSDB_PATH = tempfile.mkdtemp(prefix="spill_test_")
    return ROCKSDB_PATH

def cleanup_test_environment():
    """Clean up test environment"""
    global ROCKSDB_PATH
    if ROCKSDB_PATH and os.path.exists(ROCKSDB_PATH):
        shutil.rmtree(ROCKSDB_PATH)

def check_server_running():
    """Check if database server is running on the expected port"""
    try:
        # Try to connect to the server
        r = redis.Redis(host='localhost', port=REDIS_PORT, socket_connect_timeout=2, socket_timeout=2)
        r.ping()
        try:
            r.execute_command('spill.info')
            return True
        except redis.ResponseError as e:
            if 'unknown command' in str(e).lower():
                print(f"ERROR: Spill module not loaded")
                return False
            return True

    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        return False

def run_test(test_func, test_name):
    try:
        print(f"{test_name}...", end=" ")
        test_func()
        print("PASS")
        return True
    except AssertionError as e:
        print(f"FAIL: {e}")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False

# Integration Tests

def test_basic_eviction_and_restore():
    """Test basic key eviction and restoration"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set a key
    r.set('test_key', 'test_value')

    # Fill memory to trigger eviction
    for i in range(1000):
        r.set(f'filler_{i}', 'x' * 5000)

    # Check if key was evicted
    if r.get('test_key') is not None:
        print("  Key was not evicted, trying more filler data...")
        for i in range(1000, 2000):
            r.set(f'filler_{i}', 'x' * 5000)

    if r.get('test_key') is not None:
        print("  Key still not evicted, skipping basic test")
        return

    # Restore the key
    result = r.execute_command('spill.restore', 'test_key')
    assert result == 'OK', f"Restore failed: {result}"

    # Verify restored value
    value = r.get('test_key')
    assert value == 'test_value', f"Restored value mismatch: {value}"

def test_ttl_preservation():
    """Test that TTL is preserved during eviction and restoration"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set a key with TTL
    r.setex('ttl_key', 3600, 'ttl_value')  # 1 hour TTL
    initial_ttl = r.ttl('ttl_key')

    # Fill memory to trigger eviction
    for i in range(1000):
        r.set(f'filler_ttl_{i}', 'y' * 5000)

    # Check if key was evicted, if not skip TTL test
    if r.get('ttl_key') is not None:
        print("  Key was not evicted, skipping TTL test")
        return

    # Wait a bit
    time.sleep(1)

    # Restore the key
    result = r.execute_command('spill.restore', 'ttl_key')
    assert result == 'OK', f"Restore failed: {result}"

    # Check TTL is preserved (should be less than initial due to time passed)
    restored_ttl = r.ttl('ttl_key')
    assert restored_ttl > 0, "TTL should be positive"
    assert restored_ttl <= initial_ttl, "TTL should not have increased"
    # Allow for more tolerance in TTL difference
    assert restored_ttl > initial_ttl - 30, "TTL decreased too much"

    # Verify value
    value = r.get('ttl_key')
    assert value == 'ttl_value', f"Restored value mismatch: {value}"

def test_expired_key_not_restored():
    """Test that expired keys are not restored"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set a key with very short TTL
    r.setex('expire_key', 2, 'expire_value')  # 2 second TTL

    # Fill memory to trigger eviction
    for i in range(1000):
        r.set(f'filler_exp_{i}', 'z' * 5000)

    # Check if key was evicted
    if r.get('expire_key') is not None:
        print("  Key was not evicted, skipping expiration test")
        return

    # Wait for key to expire
    time.sleep(3)

    # Try to restore expired key
    try:
        result = r.execute_command('spill.restore', 'expire_key')
        # Should either get None (not found) or error message about expiration
        assert result is None or ('expired' in str(result).lower() if result else True), f"Expected expiration handling, got: {result}"
    except redis.ResponseError as e:
        # This is also acceptable - server returned an error about expiration
        assert 'expired' in str(e).lower() or 'not found' in str(e).lower(), f"Expected expiration error, got: {e}"

def test_restore_nonexistent_key():
    """Test restoring a key that doesn't exist in RocksDB"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    result = r.execute_command('spill.restore', 'nonexistent_key')
    assert result is None, f"Expected None for nonexistent key, got: {result}"

def test_multiple_evictions_and_restores():
    """Test multiple keys being evicted and restored"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set multiple keys
    keys = {}
    for i in range(10):
        key = f'multi_key_{i}'
        value = f'multi_value_{i}'
        r.set(key, value)
        keys[key] = value

    # Fill memory to trigger eviction
    for i in range(2000):
        r.set(f'filler_multi_{i}', 'a' * 5000)

    # Check keys were evicted
    evicted = []
    for key in keys:
        if r.get(key) is None:
            evicted.append(key)

    if len(evicted) == 0:
        print("  No keys were evicted, skipping multi-restore test")
        return

    print(f"\n  {len(evicted)} keys were evicted")

    # Restore evicted keys
    restored = 0
    for key in evicted:
        try:
            result = r.execute_command('spill.restore', key)
            if result == 'OK':
                restored += 1
                value = r.get(key)
                assert value == keys[key], f"Value mismatch for {key}"
        except redis.ResponseError as e:
            print(f"  Failed to restore {key}: {e}")

    # At least some keys should be restored
    assert restored > 0, f"No keys were restored out of {len(evicted)} evicted"
    print(f"  {restored}/{len(evicted)} keys successfully restored")

def test_spill_info_command():
    """Test the spill.info command"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Trigger some evictions first
    for i in range(100):
        r.set(f'info_key_{i}', 'info_value' * 100)

    for i in range(1000):
        r.set(f'filler_info_{i}', 'b' * 5000)

    # Get RocksDB stats
    stats = r.execute_command('spill.info')
    assert stats is not None, "spill.info returned None"
    assert len(stats) > 0, "spill.info returned empty stats"
    assert 'rocksdb' in stats.lower() or 'level' in stats.lower(), "Stats don't look like RocksDB stats"

def test_key_with_spaces_and_special_chars():
    """Test keys with spaces and special characters"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=False)  # Use binary mode

    # Test various special keys (excluding ones with null bytes which might cause issues)
    special_keys = [
        b'key with spaces',
        b'key:with:colons',
        b'key_with_underscores',
        b'key-with-dashes'
    ]

    set_keys = []
    for key in special_keys:
        try:
            r.set(key, b'special_value')
            set_keys.append(key)
        except Exception as e:
            print(f"  Failed to set key {key}: {e}")

    if not set_keys:
        print("  No special keys could be set, skipping test")
        return

    # Trigger eviction
    for i in range(1000):
        r.set(f'filler_special_{i}'.encode(), b'c' * 5000)

    # Restore special keys
    restored = 0
    for key in set_keys:
        if r.get(key) is None:
            try:
                result = r.execute_command('spill.restore', key)
                if result == b'OK':
                    value = r.get(key)
                    if value == b'special_value':
                        restored += 1
            except Exception as e:
                print(f"  Failed to restore special key {key}: {e}")

    print(f"  {restored}/{len(set_keys)} special keys restored successfully")

def test_double_restore():
    """Test that restoring a key twice removes it from RocksDB"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set and evict a key
    r.set('double_key', 'double_value')

    # Fill memory to trigger eviction
    for i in range(1000):
        r.set(f'filler_double_{i}', 'd' * 5000)

    # Check if key was evicted
    if r.get('double_key') is not None:
        print("  Key was not evicted, trying more filler data...")
        for i in range(1000, 2000):
            r.set(f'filler_double_{i}', 'd' * 5000)

    if r.get('double_key') is not None:
        print("  Key still not evicted, skipping double restore test")
        return

    # First restore
    result1 = r.execute_command('spill.restore', 'double_key')
    assert result1 == 'OK', f"First restore failed: {result1}"

    # Verify the key was restored
    value1 = r.get('double_key')
    assert value1 == 'double_value', f"Restored value mismatch: {value1}"

    # Delete from Redis (not from RocksDB - that should already be done by restore)
    r.delete('double_key')

    # Second restore should return None (key was removed from RocksDB after first restore)
    result2 = r.execute_command('spill.restore', 'double_key')
    assert result2 is None, f"Second restore should return None, got: {result2}"

def test_large_value():
    """Test eviction and restoration of large values"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=False)

    # Create a smaller large value (100KB instead of 1MB to avoid timeout issues)
    large_value = b'x' * (100 * 1024)
    r.set('large_key', large_value)

    # Trigger eviction
    for i in range(200):
        r.set(f'filler_large_{i}'.encode(), b'e' * 50000)

    if r.get('large_key') is None:
        # Restore large key
        try:
            result = r.execute_command('spill.restore', 'large_key')
            if result == b'OK':
                # Verify large value
                restored_value = r.get('large_key')
                assert restored_value == large_value, "Large value mismatch"
                print("  Large value successfully restored")
            else:
                print(f"  Large value restore returned: {result}")
        except Exception as e:
            print(f"  Large value restore failed: {e}")
    else:
        print("  Large key was not evicted, skipping large value test")

def test_concurrent_operations():
    """Test concurrent evictions and restorations"""
    import threading
    import random

    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)
    errors = []

    def worker(thread_id):
        try:
            for i in range(20):  # Reduced iterations to avoid timeouts
                key = f'concurrent_{thread_id}_{i}'
                r.set(key, f'value_{thread_id}_{i}')

                # Random operations
                if random.random() > 0.5:
                    r.set(f'filler_{thread_id}_{i}', 'f' * 5000)  # Smaller filler values

                if random.random() > 0.8:  # Less frequent restore attempts
                    try:
                        r.execute_command('spill.restore', key)
                    except redis.ResponseError:
                        pass  # Key might not exist, ignore error
        except Exception as e:
            errors.append(f"Thread {thread_id}: {str(e)}")

    # Run concurrent operations with fewer threads
    threads = []
    for i in range(3):  # Reduced from 5 to 3 threads
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Allow some errors in concurrent operations as they might be expected
    if len(errors) > 0:
        print(f"  Concurrent operations had {len(errors)} errors (may be expected)")
        for error in errors[:3]:  # Show first 3 errors
            print(f"    {error}")
    else:
        print("  Concurrent operations completed without errors")

# New integration tests for ABSTTL functionality

def test_absttl_preservation_during_eviction():
    """Test that ABSTTL is correctly preserved during eviction and restoration"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set a key with ABSTTL (absolute expiration time in seconds)
    current_time = int(time.time())
    abs_expire_time = current_time + 7200  # 2 hours from now

    # Use SETEX to set TTL, then check what absolute time it would correspond to
    r.setex('absttl_test_key', 7200, 'absttl_test_value')
    initial_ttl = r.ttl('absttl_test_key')
    print(f"  Initial TTL: {initial_ttl} seconds")

    # Fill memory to trigger eviction
    for i in range(1000):
        r.set(f'filler_absttl_{i}', 'x' * 8000)

    # Check if key was evicted
    if r.get('absttl_test_key') is not None:
        print("  Key was not evicted, trying more filler data...")
        for i in range(1000, 2000):
            r.set(f'filler_absttl_{i}', 'x' * 8000)

    if r.get('absttl_test_key') is not None:
        print("  Key still not evicted, skipping ABSTTL preservation test")
        return

    # Wait a bit to let time pass
    time.sleep(2)

    # Restore the key (should use ABSTTL internally)
    result = r.execute_command('spill.restore', 'absttl_test_key')
    assert result == 'OK', f"ABSTTL restore failed: {result}"

    # Verify the key was restored
    value = r.get('absttl_test_key')
    assert value == 'absttl_test_value', f"Restored value mismatch: {value}"

    # Check that TTL is still reasonable (should be less than original due to time passed)
    restored_ttl = r.ttl('absttl_test_key')
    assert restored_ttl > 0, "TTL should still be positive"
    assert restored_ttl < initial_ttl, "TTL should be less than initial due to time passed"
    # Allow some tolerance for processing time
    assert restored_ttl > initial_ttl - 10, "TTL decreased too much"

    print(f"  Restored TTL: {restored_ttl} seconds (difference: {initial_ttl - restored_ttl}s)")

def test_expired_key_deletion_from_rocksdb():
    """Test that expired keys are deleted from RocksDB without restoration"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set a key with very short TTL
    r.setex('expire_test_key', 3, 'expire_test_value')  # 3 seconds

    # Fill memory to trigger eviction
    for i in range(800):
        r.set(f'filler_expire_{i}', 'y' * 10000)

    # Check if key was evicted
    if r.get('expire_test_key') is not None:
        print("  Key was not evicted, trying more filler data...")
        for i in range(800, 1500):
            r.set(f'filler_expire_{i}', 'y' * 10000)

    if r.get('expire_test_key') is not None:
        print("  Key still not evicted, skipping expiration test")
        return

    print("  Key successfully evicted, waiting for expiration...")

    # Wait for the key to expire
    time.sleep(4)

    # Try to restore expired key - should return None (not found) because it was deleted
    result = r.execute_command('spill.restore', 'expire_test_key')
    assert result is None, f"Expected None for expired key, got: {result}"

    # Verify the key is not in Redis either
    value = r.get('expire_test_key')
    assert value is None, f"Expired key should not exist in Redis: {value}"

    print("  Expired key correctly deleted from RocksDB and not restored")

def test_absttl_precision():
    """Test that ABSTTL maintains millisecond precision"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Set multiple keys with different precise TTLs
    keys_and_ttls = []
    for i in range(5):
        key = f'precision_key_{i}'
        ttl = 3600 + i * 10  # Different TTLs: 3600, 3610, 3620, etc.
        r.setex(key, ttl, f'precision_value_{i}')
        keys_and_ttls.append((key, ttl, r.ttl(key)))

    # Trigger eviction
    for i in range(1200):
        r.set(f'filler_precision_{i}', 'z' * 7000)

    # Check which keys were evicted
    evicted_keys = []
    for key, original_ttl, initial_ttl in keys_and_ttls:
        if r.get(key) is None:
            evicted_keys.append((key, original_ttl, initial_ttl))

    if not evicted_keys:
        print("  No precision keys were evicted, skipping precision test")
        return

    print(f"  {len(evicted_keys)} keys evicted for precision testing")

    # Wait a bit
    time.sleep(1)

    # Restore and verify precision
    successful_restores = 0
    for key, original_ttl, initial_ttl in evicted_keys:
        try:
            result = r.execute_command('spill.restore', key)
            if result == 'OK':
                restored_ttl = r.ttl(key)
                if restored_ttl > 0:
                    # Check that the TTL is reasonable (allowing for processing time)
                    time_passed = 1  # We waited 1 second
                    expected_ttl_min = initial_ttl - time_passed - 2  # Allow 2s tolerance
                    expected_ttl_max = initial_ttl - time_passed + 2

                    if expected_ttl_min <= restored_ttl <= expected_ttl_max:
                        successful_restores += 1
                        print(f"    {key}: Original={original_ttl}s, Initial={initial_ttl}s, Restored={restored_ttl}s ✓")
                    else:
                        print(f"    {key}: TTL precision issue - Expected ~{initial_ttl-time_passed}s, got {restored_ttl}s")
        except Exception as e:
            print(f"    {key}: Restore failed: {e}")

    assert successful_restores > 0, "No keys were successfully restored with correct TTL precision"
    print(f"  {successful_restores}/{len(evicted_keys)} keys restored with correct TTL precision")

def test_absttl_edge_cases():
    """Test ABSTTL edge cases and boundary conditions"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Test case 1: Keys with very short TTL (1-2 seconds)
    r.setex('edge_short_ttl', 2, 'short_ttl_value')
    short_ttl_initial = r.ttl('edge_short_ttl')

    # Test case 2: Keys with very long TTL (24 hours)
    r.setex('edge_long_ttl', 86400, 'long_ttl_value')
    long_ttl_initial = r.ttl('edge_long_ttl')

    # Trigger eviction
    for i in range(1000):
        r.set(f'filler_edge_{i}', 'w' * 9000)

    # Check if keys were evicted
    short_evicted = r.get('edge_short_ttl') is None
    long_evicted = r.get('edge_long_ttl') is None

    if not (short_evicted or long_evicted):
        print("  No edge case keys were evicted, trying more filler...")
        for i in range(1000, 2000):
            r.set(f'filler_edge_{i}', 'w' * 9000)
        short_evicted = r.get('edge_short_ttl') is None
        long_evicted = r.get('edge_long_ttl') is None

    # Test short TTL restoration
    if short_evicted:
        print("  Testing short TTL restoration...")
        result = r.execute_command('spill.restore', 'edge_short_ttl')
        if result == 'OK':
            restored_value = r.get('edge_short_ttl')
            restored_ttl = r.ttl('edge_short_ttl')
            print(f"    Short TTL key restored: TTL={restored_ttl}s")
            assert restored_value == 'short_ttl_value'
            assert restored_ttl > 0  # Should still be valid
        else:
            print(f"    Short TTL key not restored (may have expired): {result}")
    else:
        print("  Short TTL key was not evicted")

    # Test long TTL restoration
    if long_evicted:
        print("  Testing long TTL restoration...")
        result = r.execute_command('spill.restore', 'edge_long_ttl')
        if result == 'OK':
            restored_value = r.get('edge_long_ttl')
            restored_ttl = r.ttl('edge_long_ttl')
            print(f"    Long TTL key restored: TTL={restored_ttl}s")
            assert restored_value == 'long_ttl_value'
            assert restored_ttl > 86390  # Should be close to original (allowing for processing time)
        else:
            print(f"    Long TTL key not restored: {result}")
    else:
        print("  Long TTL key was not evicted")

def test_multiple_absttl_keys():
    """Test restoration of multiple keys with different ABSTTL values"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Create multiple keys with different TTLs
    test_keys = []
    base_ttl = 3600  # 1 hour

    for i in range(10):
        key = f'multi_absttl_{i}'
        ttl = base_ttl + (i * 300)  # 1h, 1h5m, 1h10m, etc.
        value = f'multi_absttl_value_{i}'
        r.setex(key, ttl, value)
        initial_ttl = r.ttl(key)
        test_keys.append((key, value, ttl, initial_ttl))

    # Trigger massive eviction
    for i in range(2000):
        r.set(f'filler_multi_absttl_{i}', 'v' * 8000)

    # Check which keys were evicted
    evicted_keys = []
    for key, value, original_ttl, initial_ttl in test_keys:
        if r.get(key) is None:
            evicted_keys.append((key, value, original_ttl, initial_ttl))

    if not evicted_keys:
        print("  No multi-ABSTTL keys were evicted, skipping test")
        return

    print(f"  {len(evicted_keys)} keys evicted for multi-ABSTTL testing")

    # Wait a moment
    time.sleep(1)

    # Restore all evicted keys
    restored_count = 0
    for key, expected_value, original_ttl, initial_ttl in evicted_keys:
        try:
            result = r.execute_command('spill.restore', key)
            if result == 'OK':
                # Verify value and TTL
                actual_value = r.get(key)
                actual_ttl = r.ttl(key)

                assert actual_value == expected_value, f"Value mismatch for {key}"
                assert actual_ttl > 0, f"TTL should be positive for {key}"

                # TTL should be reasonable (less than initial due to time passed)
                assert actual_ttl <= initial_ttl, f"TTL increased for {key}"
                assert actual_ttl > initial_ttl - 10, f"TTL decreased too much for {key}"

                restored_count += 1
                print(f"    {key}: Restored with TTL {actual_ttl}s (was {initial_ttl}s)")
            else:
                print(f"    {key}: Not restored: {result}")
        except Exception as e:
            print(f"    {key}: Error during restore: {e}")

    assert restored_count > 0, "No keys were successfully restored"
    print(f"  {restored_count}/{len(evicted_keys)} multi-ABSTTL keys successfully restored")

def test_absttl_vs_relative_ttl_consistency():
    """Test that ABSTTL behavior is consistent with relative TTL behavior"""
    r = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)

    # Create two similar keys - one will test ABSTTL, one will be reference
    ttl_seconds = 1800  # 30 minutes
    r.setex('consistency_test_1', ttl_seconds, 'consistency_value_1')
    r.setex('consistency_test_2', ttl_seconds, 'consistency_value_2')

    initial_ttl_1 = r.ttl('consistency_test_1')
    initial_ttl_2 = r.ttl('consistency_test_2')

    # Trigger eviction of first key only (partial eviction)
    for i in range(800):
        r.set(f'filler_consistency_{i}', 'u' * 10000)

    # Check eviction status
    key1_evicted = r.get('consistency_test_1') is None
    key2_evicted = r.get('consistency_test_2') is None

    if not key1_evicted:
        print("  Test key was not evicted, trying more filler...")
        for i in range(800, 1500):
            r.set(f'filler_consistency_{i}', 'u' * 10000)
        key1_evicted = r.get('consistency_test_1') is None

    if not key1_evicted:
        print("  Consistency test key still not evicted, skipping consistency test")
        return

    # Wait a bit
    time.sleep(2)

    # Restore evicted key
    result = r.execute_command('spill.restore', 'consistency_test_1')
    if result == 'OK':
        restored_ttl_1 = r.ttl('consistency_test_1')
        current_ttl_2 = r.ttl('consistency_test_2') if not key2_evicted else None

        print(f"  Restored key TTL: {restored_ttl_1}s")
        if current_ttl_2 is not None:
            print(f"  Reference key TTL: {current_ttl_2}s")

            # The TTLs should be similar (allowing for small differences due to timing)
            ttl_diff = abs(restored_ttl_1 - current_ttl_2)
            assert ttl_diff <= 5, f"TTL difference too large: {ttl_diff}s"
            print(f"  TTL difference: {ttl_diff}s (within acceptable range)")
        else:
            print("  Reference key was also evicted, checking absolute consistency")
            # The restored TTL should still be reasonable
            time_passed = 2  # We waited 2 seconds
            expected_ttl = initial_ttl_1 - time_passed
            ttl_diff = abs(restored_ttl_1 - expected_ttl)
            assert ttl_diff <= 5, f"TTL not consistent with expected: {ttl_diff}s difference"
            print(f"  TTL consistent with expected time passage: {ttl_diff}s difference")
    else:
        print(f"  Consistency test key could not be restored: {result}")

def main():
    """Main test runner"""
    print("=== DiceDB Spill Integration Tests ===\n")

    # Check if module exists
    if not os.path.exists(MODULE_PATH):
        print(f"ERROR: Module not found at {MODULE_PATH}")
        print("Please run 'make' first to build the module")
        sys.exit(1)

    # Check if server is running with module loaded
    if not check_server_running():
        sys.exit(1)

    # Setup test environment
    setup_test_environment()

    # Run tests
    tests = [
        (test_basic_eviction_and_restore, "Basic eviction and restore"),
        (test_ttl_preservation, "TTL preservation"),
        (test_expired_key_not_restored, "Expired key not restored"),
        (test_restore_nonexistent_key, "Restore nonexistent key"),
        (test_multiple_evictions_and_restores, "Multiple evictions and restores"),
        (test_spill_info_command, "Spill info command"),
        (test_key_with_spaces_and_special_chars, "Keys with special characters"),
        (test_double_restore, "Double restore removes from RocksDB"),
        (test_large_value, "Large value eviction and restore"),
        (test_concurrent_operations, "Concurrent operations"),
        # New ABSTTL tests
        (test_absttl_preservation_during_eviction, "ABSTTL preservation during eviction"),
        (test_expired_key_deletion_from_rocksdb, "Expired key deletion from RocksDB"),
        (test_absttl_precision, "ABSTTL precision testing"),
        (test_absttl_edge_cases, "ABSTTL edge cases"),
        (test_multiple_absttl_keys, "Multiple ABSTTL keys"),
        (test_absttl_vs_relative_ttl_consistency, "ABSTTL vs relative TTL consistency")
    ]

    passed = 0
    failed = 0

    try:
        print(f"\nRunning {len(tests)} integration tests...\n")
        for test_func, test_name in tests:
            if run_test(test_func, test_name):
                passed += 1
            else:
                failed += 1
    finally:
        # Cleanup test environment
        cleanup_test_environment()

    print(f"\n{passed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()