#!/usr/bin/env python3
"""
DiceDB Spill Module - Commands and Cleanup Thread Tests

Tests for:
1. SPILL.RESTORE command behavior as documented
2. INFO command showing cleanup_interval
3. SPILL.CLEANUP command
4. INFO command integration with spill stats
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

    def parse_info_response(self, info_data):
        """Helper to parse INFO response into a dictionary"""
        result = {}

        # Handle both string and dict responses
        if isinstance(info_data, dict):
            # Client already parsed it as dict
            for key, value in info_data.items():
                # Strip spill_ prefix if present
                if key.startswith('spill_'):
                    key = key[6:]
                result[key] = value
            return result

        # Parse string format
        for line in info_data.split('\r\n'):
            line = line.strip()
            if line and not line.startswith('#') and ':' in line:
                key, value = line.split(':', 1)
                # Strip spill_ prefix if present (from INFO command)
                if key.startswith('spill_'):
                    key = key[6:]  # Remove 'spill_' prefix
                # Try to convert to int, otherwise keep as string
                try:
                    result[key] = int(value)
                except ValueError:
                    result[key] = value
        return result

    def get_spill_info(self):
        """Helper to get spill stats from INFO command"""
        info = self.client.execute_command('INFO', 'spill')
        return self.parse_info_response(info)

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

        # Check num_keys_stored in RocksDB
        info_dict = self.get_spill_info()
        keys_stored_before = info_dict.get('num_keys_stored', 0)
        self.assertGreater(keys_stored_before, 0)

        # Restore the key
        self.client.execute_command('SPILL.RESTORE', 'key1')

        # Check num_keys_stored again - should be decreased
        info_dict = self.get_spill_info()
        keys_stored_after = info_dict.get('num_keys_stored', 0)
        self.assertEqual(keys_stored_after, keys_stored_before - 1)

    def test_restore_wrong_number_of_arguments(self):
        """Test SPILL.RESTORE with wrong number of arguments"""
        with self.assertRaises(redis.ResponseError) as context:
            self.client.execute_command('SPILL.RESTORE')
        self.assertIn('wrong number of arguments', str(context.exception).lower())

    def test_restore_expired_key(self):
        """Test SPILL.RESTORE handles expired key correctly"""
        # Set key with 1 second TTL
        self.client.setex('expkey', 1, 'value')

        # Evict it
        self.client.execute_command('EVICT', 'expkey')

        # Wait for expiration
        time.sleep(2)

        # Try to restore - should either get expiration error or None (if already cleaned)
        try:
            result = self.client.execute_command('SPILL.RESTORE', 'expkey')
            # If no error, result should be None (key not found/already cleaned)
            self.assertIsNone(result, "Expired key should return None if already cleaned")
        except redis.ResponseError as e:
            # If error, should contain "expired" in the message
            self.assertIn('expired', str(e).lower(), "Error should mention key expired")

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
    # INFO Command Tests
    # ========================================================================

    def test_info_shows_cleanup_interval(self):
        """Test INFO displays cleanup_interval configuration"""
        info_dict = self.get_spill_info()

        # Should contain cleanup_interval_seconds
        self.assertIn('cleanup_interval_seconds', info_dict)

        # Verify it's a non-negative number
        interval = info_dict['cleanup_interval_seconds']
        self.assertIsInstance(interval, int)
        self.assertGreaterEqual(interval, 0)

    def test_info_shows_config_sections(self):
        """Test INFO displays configuration fields"""
        info_dict = self.get_spill_info()

        # Check for required sections and fields
        self.assertIn('num_keys_stored', info_dict)
        self.assertIn('total_keys_restored', info_dict)
        self.assertIn('total_keys_cleaned', info_dict)
        self.assertIn('max_memory_bytes', info_dict)
        self.assertIn('path', info_dict)
        self.assertIn('cleanup_interval_seconds', info_dict)

    def test_info_config_format(self):
        """Test INFO config values are properly formatted"""
        info_dict = self.get_spill_info()

        # max_memory_bytes should be a positive integer
        self.assertIn('max_memory_bytes', info_dict)
        max_memory = info_dict['max_memory_bytes']
        self.assertIsInstance(max_memory, int)
        self.assertGreater(max_memory, 0)

        # path should be a non-empty string
        self.assertIn('path', info_dict)
        path = info_dict['path']
        self.assertIsInstance(path, str)
        # Path could be empty string, so just check it exists

    # ========================================================================
    # INFO Command Tests
    # ========================================================================

    def test_stats_returns_bulk_string(self):
        """Test INFO returns correct fields"""
        stats_dict = self.get_spill_info()

        # Check for required fields
        self.assertIn('num_keys_stored', stats_dict)
        self.assertIn('total_keys_restored', stats_dict)
        self.assertIn('total_keys_cleaned', stats_dict)
        self.assertIn('total_bytes_written', stats_dict)
        self.assertIn('total_bytes_read', stats_dict)

    def test_stats_counters_are_numeric(self):
        """Test INFO counters are numeric values"""
        stats_dict = self.get_spill_info()

        # Numeric stats fields (excluding string config fields like 'path')
        numeric_fields = ['num_keys_stored', 'total_keys_written', 'total_keys_restored',
                         'total_keys_cleaned', 'last_num_keys_cleaned', 'last_cleanup_at',
                         'total_bytes_written', 'total_bytes_read', 'max_memory_bytes', 'cleanup_interval_seconds']

        for key in numeric_fields:
            if key in stats_dict:
                value = stats_dict[key]
                self.assertIsInstance(value, int, f"{key} should be integer")
                self.assertGreaterEqual(value, 0, f"{key} should be non-negative")

    def test_stats_increments_on_operations(self):
        """Test INFO counters increment correctly"""
        # Get initial stats
        stats_dict_before = self.get_spill_info()

        # Perform operations
        self.client.set('k1', 'v1')
        self.client.execute_command('EVICT', 'k1')
        self.client.execute_command('SPILL.RESTORE', 'k1')

        # Get stats after
        stats_dict_after = self.get_spill_info()

        # Verify increments
        # num_keys_stored should be same (evict +1, restore -1 = net 0)
        # total_keys_written should increase (eviction wrote to RocksDB)
        # total_keys_restored should increase (restore read from RocksDB)
        self.assertGreater(stats_dict_after['total_keys_written'], stats_dict_before['total_keys_written'])
        self.assertGreater(stats_dict_after['total_keys_restored'], stats_dict_before['total_keys_restored'])
        self.assertGreater(stats_dict_after['total_bytes_written'], stats_dict_before['total_bytes_written'])
        self.assertGreater(stats_dict_after['total_bytes_read'], stats_dict_before['total_bytes_read'])

    # ========================================================================
    # SPILL.CLEANUP Command Tests
    # ========================================================================

    def test_cleanup_returns_statistics(self):
        """Test SPILL.CLEANUP returns num_keys_scanned and num_keys_cleaned"""
        result = self.client.execute_command('SPILL.CLEANUP')

        # Should be a list
        self.assertIsInstance(result, list)

        # Convert to dict
        result_dict = {result[i]: result[i+1] for i in range(0, len(result), 2)}

        # Should have required fields
        self.assertIn('num_keys_scanned', result_dict)
        self.assertIn('num_keys_cleaned', result_dict)

        # Values should be integers
        self.assertIsInstance(result_dict['num_keys_scanned'], int)
        self.assertIsInstance(result_dict['num_keys_cleaned'], int)

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
        self.assertGreater(result_dict['num_keys_scanned'], 0)
        self.assertGreater(result_dict['num_keys_cleaned'], 0)

    def test_cleanup_increments_keys_cleaned_counter(self):
        """Test SPILL.CLEANUP increments total_keys_cleaned in stats"""
        # Get initial total_keys_cleaned
        stats_dict_before = self.get_spill_info()
        initial_cleaned = stats_dict_before['total_keys_cleaned']

        # Create and expire keys
        for i in range(2):
            self.client.setex(f'temp{i}', 1, f'val{i}')
            self.client.execute_command('EVICT', f'temp{i}')

        time.sleep(2)

        # Run cleanup
        cleanup_result = self.client.execute_command('SPILL.CLEANUP')
        cleanup_dict = {cleanup_result[i]: cleanup_result[i+1] for i in range(0, len(cleanup_result), 2)}
        removed = cleanup_dict['num_keys_cleaned']

        # Check total_keys_cleaned increased
        stats_dict_after = self.get_spill_info()

        self.assertEqual(stats_dict_after['total_keys_cleaned'], initial_cleaned + removed)

    # ========================================================================
    # Periodic Cleanup Thread Tests
    # ========================================================================

    def test_cleanup_interval_configuration(self):
        """Test that cleanup_interval is properly configured and displayed"""
        info_dict = self.get_spill_info()

        # Should show cleanup_interval_seconds
        self.assertIn('cleanup_interval_seconds', info_dict)

        interval = info_dict['cleanup_interval_seconds']
        self.assertIsInstance(interval, int)
        # Should have a non-negative value
        self.assertGreaterEqual(interval, 0)

    def test_automatic_cleanup_happens(self):
        """Test that automatic periodic cleanup removes expired keys"""
        # Note: This test requires cleanup_interval to be set to a short value (e.g., 10 seconds)
        # For CI/CD, the module should be loaded with cleanup-interval 10 or similar

        # Create expired keys
        for i in range(5):
            self.client.setex(f'autoexp{i}', 1, f'val{i}')
            self.client.execute_command('EVICT', f'autoexp{i}')

        # Get initial stats
        stats_dict_before = self.get_spill_info()

        # Wait for keys to expire
        time.sleep(2)

        # Get cleanup interval from INFO
        info_dict = self.get_spill_info()
        interval = info_dict.get('cleanup_interval_seconds', 300)

        if interval > 0 and interval < 60:  # Only test if interval is reasonable for testing
            # Wait for automatic cleanup to run (interval + buffer)
            print(f"Waiting {interval + 5} seconds for automatic cleanup...")
            time.sleep(interval + 5)

            # Check that total_keys_cleaned increased (automatic cleanup ran)
            stats_dict_after = self.get_spill_info()

            # total_keys_cleaned should have increased (expired keys caught during restore or periodic cleanup)
            keys_cleaned_before = stats_dict_before['total_keys_cleaned']
            keys_cleaned_after = stats_dict_after['total_keys_cleaned']

            self.assertGreater(keys_cleaned_after, keys_cleaned_before,
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
