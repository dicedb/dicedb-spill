#!/usr/bin/env python3
"""
DiceDB Spill Module - Commands and Cleanup Thread Tests

Tests for:
1. SPILL.RESTORE command behavior as documented
2. SPILL.INFO showing cleanup_interval
3. SPILL.CLEANUP command
4. SPILL.STATS command
5. Automatic periodic cleanup thread functionality
6. TTL preservation
7. Different data types support
"""

import redis
import time
import unittest
import re


class CommandsAndCleanupTest(unittest.TestCase):
    """Tests for documented command behaviors and cleanup thread"""

    @classmethod
    def setUpClass(cls):
        """Set up test environment and connection"""
        cls.client = redis.Redis(host='localhost', port=6379, decode_responses=True)

        # Test connection
        try:
            cls.client.ping()
        except redis.ConnectionError:
            raise Exception("Cannot connect to DiceDB on port 6379. Make sure server is running.")

    def setUp(self):
        """Set up each test"""
        self.client.flushdb()
        time.sleep(0.1)

    def tearDown(self):
        """Clean up after each test"""
        self.client.flushdb()
        time.sleep(0.1)

    # ========================================================================
    # SPILL.RESTORE Command Tests
    # ========================================================================

    def test_restore_evicted_key(self):
        """Test SPILL.RESTORE returns OK when restoring evicted key"""
        # Set a key
        self.client.set('mykey', 'myvalue')

        # Evict the key
        result = self.client.execute_command('EVICT', 'mykey')
        self.assertIn('mykey', result)

        # Key should not be in memory
        keys = self.client.keys('*')
        self.assertNotIn('mykey', keys)

        # Restore the key
        result = self.client.execute_command('SPILL.RESTORE', 'mykey')
        self.assertEqual(result, 'OK')

        # Verify key is back
        value = self.client.get('mykey')
        self.assertEqual(value, 'myvalue')

    def test_restore_nonexistent_key(self):
        """Test SPILL.RESTORE returns nil for non-existent key"""
        result = self.client.execute_command('SPILL.RESTORE', 'nonexistent')
        self.assertIsNone(result)

    def test_restore_key_in_memory_does_not_replace(self):
        """Test SPILL.RESTORE returns nil and doesn't replace key already in memory"""
        # Set and evict a key
        self.client.set('key1', 'original')
        self.client.execute_command('EVICT', 'key1')

        # Create a new key with the same name
        self.client.set('key1', 'new_value')

        # Try to restore - should return nil
        result = self.client.execute_command('SPILL.RESTORE', 'key1')
        self.assertIsNone(result)

        # Value should still be the new one, not replaced
        value = self.client.get('key1')
        self.assertEqual(value, 'new_value')

    def test_restore_removes_key_from_rocksdb(self):
        """Test that restored key is removed from RocksDB"""
        # Set and evict
        self.client.set('key1', 'value1')
        self.client.execute_command('EVICT', 'key1')

        # Check estimated_keys in RocksDB
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)
        estimated_keys_before = int(re.search(r'estimated_keys:(\d+)', info_str).group(1))
        self.assertGreater(estimated_keys_before, 0)

        # Restore the key
        self.client.execute_command('SPILL.RESTORE', 'key1')

        # Check estimated_keys again - should be decreased
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)
        estimated_keys_after = int(re.search(r'estimated_keys:(\d+)', info_str).group(1))
        self.assertEqual(estimated_keys_after, estimated_keys_before - 1)

    def test_restore_wrong_number_of_arguments(self):
        """Test SPILL.RESTORE with wrong number of arguments"""
        with self.assertRaises(redis.ResponseError) as context:
            self.client.execute_command('SPILL.RESTORE')
        self.assertIn('wrong number of arguments', str(context.exception).lower())

    def test_restore_expired_key(self):
        """Test SPILL.RESTORE returns error for expired key"""
        # Set key with 1 second TTL
        self.client.setex('expkey', 1, 'value')

        # Evict it
        self.client.execute_command('EVICT', 'expkey')

        # Wait for expiration
        time.sleep(2)

        # Try to restore - should get expiration error
        with self.assertRaises(redis.ResponseError) as context:
            self.client.execute_command('SPILL.RESTORE', 'expkey')
        self.assertIn('expired', str(context.exception).lower())

    # ========================================================================
    # Automatic Restoration Tests
    # ========================================================================

    def test_automatic_restoration_with_get(self):
        """Test automatic restoration when accessing key with GET"""
        self.client.set('auto1', 'value1')
        self.client.execute_command('EVICT', 'auto1')

        # GET should automatically restore
        value = self.client.get('auto1')
        self.assertEqual(value, 'value1')

        # Key should be in memory now
        self.assertIn('auto1', self.client.keys('*'))

    def test_automatic_restoration_with_exists(self):
        """Test automatic restoration when checking with EXISTS"""
        self.client.set('auto2', 'value2')
        self.client.execute_command('EVICT', 'auto2')

        # EXISTS should return 1 and restore the key
        exists = self.client.exists('auto2')
        self.assertEqual(exists, 1)

        # Key should be accessible
        value = self.client.get('auto2')
        self.assertEqual(value, 'value2')

    def test_automatic_restoration_with_type(self):
        """Test automatic restoration when checking with TYPE"""
        self.client.set('auto3', 'value3')
        self.client.execute_command('EVICT', 'auto3')

        # TYPE should restore the key
        key_type = self.client.type('auto3')
        self.assertEqual(key_type, 'string')

    # ========================================================================
    # TTL Preservation Tests
    # ========================================================================

    def test_ttl_preserved_on_restore(self):
        """Test TTL is preserved during eviction and restoration"""
        # Set key with 60 second TTL
        self.client.setex('ttlkey', 60, 'ttlvalue')

        # Check TTL before eviction
        ttl_before = self.client.ttl('ttlkey')
        self.assertGreater(ttl_before, 50)

        # Evict the key
        self.client.execute_command('EVICT', 'ttlkey')

        # Restore immediately
        self.client.execute_command('SPILL.RESTORE', 'ttlkey')

        # Check TTL after restoration
        ttl_after = self.client.ttl('ttlkey')
        # TTL should be close to original (within a few seconds)
        self.assertGreater(ttl_after, 50)
        self.assertLessEqual(ttl_after, ttl_before)

    # ========================================================================
    # Different Data Types Tests
    # ========================================================================

    def test_list_eviction_and_restoration(self):
        """Test list data type is preserved during eviction/restoration"""
        # Create a list
        self.client.rpush('mylist', 'a', 'b', 'c')

        # Evict it
        self.client.execute_command('EVICT', 'mylist')

        # Restore it
        self.client.execute_command('SPILL.RESTORE', 'mylist')

        # Verify list contents
        items = self.client.lrange('mylist', 0, -1)
        self.assertEqual(items, ['a', 'b', 'c'])

    def test_set_eviction_and_restoration(self):
        """Test set data type is preserved during eviction/restoration"""
        # Create a set
        self.client.sadd('myset', 'x', 'y', 'z')

        # Evict it
        self.client.execute_command('EVICT', 'myset')

        # Automatic restoration via SMEMBERS
        members = self.client.smembers('myset')
        self.assertEqual(members, {'x', 'y', 'z'})

    def test_hash_eviction_and_restoration(self):
        """Test hash data type is preserved during eviction/restoration"""
        # Create a hash
        self.client.hset('myhash', mapping={'f1': 'v1', 'f2': 'v2'})

        # Evict it
        self.client.execute_command('EVICT', 'myhash')

        # Automatic restoration via HGETALL
        hash_data = self.client.hgetall('myhash')
        self.assertEqual(hash_data, {'f1': 'v1', 'f2': 'v2'})

    # ========================================================================
    # SPILL.INFO Tests
    # ========================================================================

    def test_info_shows_cleanup_interval(self):
        """Test SPILL.INFO displays cleanup_interval configuration"""
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)

        # Should contain cleanup_interval
        self.assertIn('cleanup_interval', info_str)

        # Extract and verify it's a positive number
        match = re.search(r'cleanup_interval:(\d+)', info_str)
        self.assertIsNotNone(match, "cleanup_interval not found in SPILL.INFO")

        interval = int(match.group(1))
        self.assertGreaterEqual(interval, 0)

    def test_info_shows_all_sections(self):
        """Test SPILL.INFO displays all documented sections"""
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)

        # Check for all required sections
        self.assertIn('# spill', info_str)
        self.assertIn('# rocksdb_memory', info_str)
        self.assertIn('# rocksdb_storage', info_str)
        self.assertIn('# rocksdb_compaction', info_str)

        # Check for key metrics
        self.assertIn('keys_stored:', info_str)
        self.assertIn('keys_restored:', info_str)
        self.assertIn('keys_expired:', info_str)
        self.assertIn('keys_cleaned:', info_str)
        self.assertIn('max_memory:', info_str)
        self.assertIn('path:', info_str)

    def test_info_memory_format(self):
        """Test SPILL.INFO memory values show both bytes and MB"""
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)

        # max_memory should show format like: 262144000 (250MB)
        match = re.search(r'max_memory:(\d+)\s*\((\d+)MB\)', info_str)
        self.assertIsNotNone(match, "max_memory format incorrect")

    # ========================================================================
    # SPILL.STATS Tests
    # ========================================================================

    def test_stats_returns_array(self):
        """Test SPILL.STATS returns array with correct fields"""
        stats = self.client.execute_command('SPILL.STATS')

        # Should be a list (array)
        self.assertIsInstance(stats, list)

        # Should have alternating key-value pairs
        self.assertGreater(len(stats), 0)
        self.assertEqual(len(stats) % 2, 0)

        # Convert to dict for easier checking
        stats_dict = {stats[i]: stats[i+1] for i in range(0, len(stats), 2)}

        # Check for required fields
        self.assertIn('keys_stored', stats_dict)
        self.assertIn('keys_restored', stats_dict)
        self.assertIn('keys_expired', stats_dict)
        self.assertIn('keys_cleaned', stats_dict)
        self.assertIn('bytes_written', stats_dict)
        self.assertIn('bytes_read', stats_dict)

    def test_stats_counters_are_numeric(self):
        """Test SPILL.STATS counters are numeric values"""
        stats = self.client.execute_command('SPILL.STATS')
        stats_dict = {stats[i]: stats[i+1] for i in range(0, len(stats), 2)}

        # All values should be integers
        for key, value in stats_dict.items():
            self.assertIsInstance(value, int, f"{key} should be integer")
            self.assertGreaterEqual(value, 0, f"{key} should be non-negative")

    def test_stats_increments_on_operations(self):
        """Test SPILL.STATS counters increment correctly"""
        # Get initial stats
        stats_before = self.client.execute_command('SPILL.STATS')
        stats_dict_before = {stats_before[i]: stats_before[i+1] for i in range(0, len(stats_before), 2)}

        # Perform operations
        self.client.set('k1', 'v1')
        self.client.execute_command('EVICT', 'k1')
        self.client.execute_command('SPILL.RESTORE', 'k1')

        # Get stats after
        stats_after = self.client.execute_command('SPILL.STATS')
        stats_dict_after = {stats_after[i]: stats_after[i+1] for i in range(0, len(stats_after), 2)}

        # Verify increments
        self.assertGreater(stats_dict_after['keys_stored'], stats_dict_before['keys_stored'])
        self.assertGreater(stats_dict_after['keys_restored'], stats_dict_before['keys_restored'])
        self.assertGreater(stats_dict_after['bytes_written'], stats_dict_before['bytes_written'])
        self.assertGreater(stats_dict_after['bytes_read'], stats_dict_before['bytes_read'])

    # ========================================================================
    # SPILL.CLEANUP Command Tests
    # ========================================================================

    def test_cleanup_returns_statistics(self):
        """Test SPILL.CLEANUP returns keys_checked and keys_removed"""
        result = self.client.execute_command('SPILL.CLEANUP')

        # Should be a list
        self.assertIsInstance(result, list)

        # Convert to dict
        result_dict = {result[i]: result[i+1] for i in range(0, len(result), 2)}

        # Should have required fields
        self.assertIn('keys_checked', result_dict)
        self.assertIn('keys_removed', result_dict)

        # Values should be integers
        self.assertIsInstance(result_dict['keys_checked'], int)
        self.assertIsInstance(result_dict['keys_removed'], int)

    def test_cleanup_removes_expired_keys(self):
        """Test SPILL.CLEANUP removes expired keys"""
        # Create keys with short TTL
        for i in range(3):
            self.client.setex(f'exp{i}', 1, f'value{i}')
            self.client.execute_command('EVICT', f'exp{i}')

        # Wait for expiration
        time.sleep(2)

        # Run cleanup
        result = self.client.execute_command('SPILL.CLEANUP')
        result_dict = {result[i]: result[i+1] for i in range(0, len(result), 2)}

        # Should have found and removed the expired keys
        self.assertGreater(result_dict['keys_checked'], 0)
        self.assertGreater(result_dict['keys_removed'], 0)

    def test_cleanup_increments_keys_cleaned_counter(self):
        """Test SPILL.CLEANUP increments keys_cleaned in stats"""
        # Get initial keys_cleaned
        stats_before = self.client.execute_command('SPILL.STATS')
        stats_dict_before = {stats_before[i]: stats_before[i+1] for i in range(0, len(stats_before), 2)}
        initial_cleaned = stats_dict_before['keys_cleaned']

        # Create and expire keys
        for i in range(2):
            self.client.setex(f'temp{i}', 1, f'val{i}')
            self.client.execute_command('EVICT', f'temp{i}')

        time.sleep(2)

        # Run cleanup
        cleanup_result = self.client.execute_command('SPILL.CLEANUP')
        cleanup_dict = {cleanup_result[i]: cleanup_result[i+1] for i in range(0, len(cleanup_result), 2)}
        removed = cleanup_dict['keys_removed']

        # Check keys_cleaned increased
        stats_after = self.client.execute_command('SPILL.STATS')
        stats_dict_after = {stats_after[i]: stats_after[i+1] for i in range(0, len(stats_after), 2)}

        self.assertEqual(stats_dict_after['keys_cleaned'], initial_cleaned + removed)

    # ========================================================================
    # Periodic Cleanup Thread Tests
    # ========================================================================

    def test_cleanup_interval_configuration(self):
        """Test that cleanup_interval is properly configured and displayed"""
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)

        # Should show cleanup_interval
        match = re.search(r'cleanup_interval:(\d+)', info_str)
        self.assertIsNotNone(match)

        interval = int(match.group(1))
        # Should have a reasonable default (e.g., 300 seconds)
        self.assertGreater(interval, 0)

    def test_automatic_cleanup_happens(self):
        """Test that automatic periodic cleanup removes expired keys"""
        # Note: This test requires cleanup_interval to be set to a short value
        # For CI/CD, the module should be loaded with cleanup-interval 10 or similar

        # Create expired keys
        for i in range(5):
            self.client.setex(f'autoexp{i}', 1, f'val{i}')
            self.client.execute_command('EVICT', f'autoexp{i}')

        # Get initial stats
        stats_before = self.client.execute_command('SPILL.STATS')
        stats_dict_before = {stats_before[i]: stats_before[i+1] for i in range(0, len(stats_before), 2)}

        # Wait for keys to expire
        time.sleep(2)

        # Get cleanup interval from INFO
        info = self.client.execute_command('SPILL.INFO')
        info_str = str(info)
        match = re.search(r'cleanup_interval:(\d+)', info_str)
        if match:
            interval = int(match.group(1))
            if interval > 0 and interval < 60:  # Only test if interval is reasonable
                # Wait for automatic cleanup to run (interval + buffer)
                print(f"Waiting {interval + 5} seconds for automatic cleanup...")
                time.sleep(interval + 5)

                # Check that keys_cleaned increased (automatic cleanup ran)
                stats_after = self.client.execute_command('SPILL.STATS')
                stats_dict_after = {stats_after[i]: stats_after[i+1] for i in range(0, len(stats_after), 2)}

                # Either keys_cleaned or keys_expired should have increased
                # (expired keys might be caught during restore or periodic cleanup)
                total_cleaned_before = stats_dict_before['keys_cleaned'] + stats_dict_before['keys_expired']
                total_cleaned_after = stats_dict_after['keys_cleaned'] + stats_dict_after['keys_expired']

                self.assertGreater(total_cleaned_after, total_cleaned_before,
                                 "Automatic cleanup should have removed expired keys")
            else:
                self.skipTest(f"Cleanup interval too long ({interval}s) for this test")

    # ========================================================================
    # Edge Cases from Documentation
    # ========================================================================

    def test_double_restore_returns_nil(self):
        """Test restoring same key twice returns nil on second attempt"""
        self.client.set('dup', 'value')
        self.client.execute_command('EVICT', 'dup')

        # First restore should work
        result1 = self.client.execute_command('SPILL.RESTORE', 'dup')
        self.assertEqual(result1, 'OK')

        # Second restore should return nil (key not in RocksDB anymore)
        result2 = self.client.execute_command('SPILL.RESTORE', 'dup')
        self.assertIsNone(result2)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
