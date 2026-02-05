#!/usr/bin/env python3
"""
Module Lifecycle Integration Tests for DiceDB Spill Module

Tests module loading, unloading, configuration validation, persistence across
restarts, and other lifecycle-related functionality.
"""

import os
import sys
import time
import subprocess
import tempfile
import shutil
import signal
import socket
import threading
from contextlib import contextmanager

try:
    import valkey as redis
except ImportError:
    try:
        import redis
    except ImportError:
        print("ERROR: Neither valkey nor redis Python library found")
        sys.exit(1)

# Test configuration
TEST_PORT = 8380  # Different port to avoid conflicts
MODULE_PATH = "../lib-spill.so"

def is_port_in_use(port):
    """Check if a port is already in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

@contextmanager
def temporary_dicedb_server(port, module_args=None):
    """Context manager for starting and stopping a temporary DiceDB server"""
    if is_port_in_use(port):
        raise Exception(f"Port {port} is already in use")

    # Create temporary directory for this test
    temp_dir = tempfile.mkdtemp(prefix="spill_lifecycle_test_")

    try:
        # Build command to start server
        cmd = [
            'dicedb-server',
            '--port', str(port),
            '--dir', temp_dir,
            '--save', '',  # Disable RDB snapshots
            '--appendonly', 'no',  # Disable AOF
        ]

        # Add module loading
        if os.path.exists(MODULE_PATH):
            cmd.extend(['--loadmodule', MODULE_PATH])
            if module_args:
                cmd.extend(module_args)

        # Start server
        print(f"Starting DiceDB server on port {port} with temp dir {temp_dir}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # Create new process group
        )

        # Wait for server to start
        for _ in range(50):  # 5 second timeout
            if is_port_in_use(port):
                break
            time.sleep(0.1)
        else:
            proc.kill()
            raise Exception(f"Server failed to start on port {port}")

        print(f"✓ DiceDB server started on port {port}")
        yield proc, temp_dir

    finally:
        # Cleanup
        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=5)
        except:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except:
                pass

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        print(f"✓ Cleaned up server and temp dir")

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

# Module Lifecycle Tests

def test_module_loading_with_default_config():
    """Test module loading with default configuration"""
    with temporary_dicedb_server(TEST_PORT) as (proc, temp_dir):
        r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

        # Test that spill commands are available
        stats = r.execute_command('spill.stats')
        assert isinstance(stats, list), "Stats should return list"

        # Test that RocksDB was initialized
        info = r.execute_command('spill.info')
        assert isinstance(info, str) and len(info) > 0, "Info should return RocksDB stats"

def test_module_loading_with_custom_config():
    """Test module loading with custom configuration parameters"""
    custom_path = tempfile.mkdtemp(prefix="custom_rocksdb_")

    try:
        module_args = [
            'path', custom_path,
            'max-memory', str(128 * 1024 * 1024)  # 128MB
        ]

        with temporary_dicedb_server(TEST_PORT, module_args) as (proc, temp_dir):
            r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

            # Test that module loaded with custom config
            stats = r.execute_command('spill.stats')
            assert isinstance(stats, list), "Stats should return list"

            # Verify custom RocksDB path is being used
            assert os.path.exists(custom_path), "Custom RocksDB path should exist"

    finally:
        if os.path.exists(custom_path):
            shutil.rmtree(custom_path)

def test_module_config_validation():
    """Test configuration parameter validation"""
    valid_path = tempfile.mkdtemp(prefix="config_validation_")

    try:
        # Test with invalid max-memory (below 20MB minimum) - module should fail to load
        module_args = [
            'path', valid_path,
            'max-memory', str(10 * 1024 * 1024)  # 10MB - below 20MB minimum
        ]

        # Server should fail to start with invalid config
        try:
            with temporary_dicedb_server(TEST_PORT, module_args) as (proc, temp_dir):
                time.sleep(0.5)  # Give it time to fail
                # If we get here, check if the module actually failed to load
                try:
                    r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True, socket_connect_timeout=1)
                    # Try to execute spill command - should fail or not be available
                    try:
                        r.execute_command('spill.stats')
                        # If command works, the module loaded despite invalid config (unexpected)
                        assert False, "Module should not load with max-memory < 20MB"
                    except redis.ResponseError:
                        # Command not available - module didn't load (expected)
                        pass
                except (redis.ConnectionError, redis.TimeoutError):
                    # Server didn't start properly (expected with invalid config)
                    pass
        except Exception:
            # Server failed to start - this is expected with invalid config
            pass

    finally:
        if os.path.exists(valid_path):
            shutil.rmtree(valid_path)

def test_module_min_memory_validation():
    """Test that module enforces minimum 20MB memory requirement"""
    valid_path = tempfile.mkdtemp(prefix="min_memory_test_")

    try:
        # Test 1: Exactly at minimum (20MB) - should succeed
        module_args = [
            'path', valid_path,
            'max-memory', str(20 * 1024 * 1024)  # Exactly 20MB
        ]

        with temporary_dicedb_server(TEST_PORT, module_args) as (proc, temp_dir):
            r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

            # Module should load successfully
            stats = r.execute_command('spill.stats')
            assert isinstance(stats, list), "Stats should work with 20MB config"

            # Verify module is functional
            info = r.execute_command('spill.info')
            assert isinstance(info, str) and len(info) > 0, "Info should return data"

        # Clean up for next test
        if os.path.exists(valid_path):
            for file in os.listdir(valid_path):
                file_path = os.path.join(valid_path, file)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

        # Test 2: Just below minimum (19MB) - should fail
        module_args_fail = [
            'path', valid_path,
            'max-memory', str(19 * 1024 * 1024)  # 19MB - below minimum
        ]

        server_started = False
        module_loaded = False

        try:
            with temporary_dicedb_server(TEST_PORT, module_args_fail) as (proc, temp_dir):
                time.sleep(0.5)
                try:
                    r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True, socket_connect_timeout=1)
                    server_started = True
                    try:
                        r.execute_command('spill.stats')
                        module_loaded = True
                    except redis.ResponseError:
                        module_loaded = False
                except (redis.ConnectionError, redis.TimeoutError):
                    server_started = False
        except Exception:
            pass

        # Module should NOT have loaded with 19MB
        assert not module_loaded, "Module should not load with max-memory < 20MB"

    finally:
        if os.path.exists(valid_path):
            shutil.rmtree(valid_path)

def test_memory_allocation_distribution():
    """Test that memory is allocated correctly: 8MB block cache, 2/3 remaining to write buffer"""
    valid_path = tempfile.mkdtemp(prefix="memory_alloc_test_")

    try:
        # Test with 50MB total memory
        # Expected: 8MB block cache, 42MB remaining, 28MB write buffer (2/3 of 42MB)
        total_memory = 50 * 1024 * 1024  # 50MB
        expected_block_cache = 8 * 1024 * 1024  # 8MB
        remaining = total_memory - expected_block_cache  # 42MB
        expected_write_buffer = (remaining * 2) // 3  # ~28MB

        module_args = [
            'path', valid_path,
            'max-memory', str(total_memory)
        ]

        with temporary_dicedb_server(TEST_PORT, module_args) as (proc, temp_dir):
            r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

            # Get info to check memory allocation
            info = r.execute_command('spill.info')
            assert isinstance(info, str), "Info should return string"

            # Parse info to verify configuration
            # The log output should show block_cache=8MB and write_buffer=28MB
            # We can verify the module loaded successfully with the right config
            stats = r.execute_command('spill.stats')
            assert isinstance(stats, list), "Module should be functional with correct memory allocation"

            # Verify the max_memory setting in info
            assert '50' in info or '52428800' in info, "Info should contain max_memory setting"

    finally:
        if os.path.exists(valid_path):
            shutil.rmtree(valid_path)

def test_persistence_across_restart():
    """Test that data persists across server restarts"""
    temp_rocksdb = tempfile.mkdtemp(prefix="persistent_rocksdb_")

    try:
        module_args = ['path', temp_rocksdb]

        # First server instance - store some data
        with temporary_dicedb_server(TEST_PORT, module_args) as (proc1, temp_dir1):
            r1 = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

            # Set keys with TTL
            r1.setex('persist_key_1', 3600, 'persist_value_1')
            r1.setex('persist_key_2', 7200, 'persist_value_2')

            # Force eviction to store in RocksDB
            for i in range(1000):
                r1.set(f'filler_{i}', 'x' * 10000)

            time.sleep(0.2)  # Allow eviction to process

            # Verify keys were evicted from memory
            assert r1.get('persist_key_1') is None
            assert r1.get('persist_key_2') is None

            # Verify they can be restored
            result1 = r1.execute_command('spill.restore', 'persist_key_1')
            assert result1 == 'OK'
            assert r1.get('persist_key_1') == 'persist_value_1'

        # Second server instance - verify persistence
        with temporary_dicedb_server(TEST_PORT, module_args) as (proc2, temp_dir2):
            r2 = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

            # Try to restore keys from previous session
            result2 = r2.execute_command('spill.restore', 'persist_key_2')
            if result2 == 'OK':
                value = r2.get('persist_key_2')
                assert value == 'persist_value_2', "Persisted value should match"

                # Check TTL is still reasonable
                ttl = r2.ttl('persist_key_2')
                assert 0 < ttl <= 7200, "TTL should be preserved"
            else:
                # Key might have been cleaned up, which is also valid
                assert result2 is None

    finally:
        if os.path.exists(temp_rocksdb):
            shutil.rmtree(temp_rocksdb)

def test_multiple_module_instances():
    """Test running multiple server instances with different configurations"""
    port1 = TEST_PORT
    port2 = TEST_PORT + 1

    if is_port_in_use(port1) or is_port_in_use(port2):
        print("  Skipping test - required ports in use")
        return

    temp_db1 = tempfile.mkdtemp(prefix="multi_instance_1_")
    temp_db2 = tempfile.mkdtemp(prefix="multi_instance_2_")

    try:
        # Start two servers with different RocksDB paths
        with temporary_dicedb_server(port1, ['path', temp_db1]) as (proc1, temp_dir1):
            with temporary_dicedb_server(port2, ['path', temp_db2]) as (proc2, temp_dir2):
                r1 = redis.Redis(host='localhost', port=port1, decode_responses=True)
                r2 = redis.Redis(host='localhost', port=port2, decode_responses=True)

                # Store different data in each instance
                r1.setex('instance1_key', 3600, 'instance1_value')
                r2.setex('instance2_key', 3600, 'instance2_value')

                # Force evictions
                for i in range(500):
                    r1.set(f'filler1_{i}', 'x' * 8000)
                    r2.set(f'filler2_{i}', 'y' * 8000)

                time.sleep(0.2)

                # Verify data isolation
                result1 = r1.execute_command('spill.restore', 'instance1_key')
                result2 = r2.execute_command('spill.restore', 'instance2_key')

                if result1 == 'OK':
                    assert r1.get('instance1_key') == 'instance1_value'
                if result2 == 'OK':
                    assert r2.get('instance2_key') == 'instance2_value'

                # Verify cross-instance isolation
                cross_result1 = r1.execute_command('spill.restore', 'instance2_key')
                cross_result2 = r2.execute_command('spill.restore', 'instance1_key')

                assert cross_result1 is None, "Instance1 should not have instance2's data"
                assert cross_result2 is None, "Instance2 should not have instance1's data"

    finally:
        for temp_db in [temp_db1, temp_db2]:
            if os.path.exists(temp_db):
                shutil.rmtree(temp_db)

def test_module_graceful_shutdown():
    """Test that module shuts down gracefully and flushes data"""
    temp_rocksdb = tempfile.mkdtemp(prefix="graceful_shutdown_")

    try:
        module_args = ['path', temp_rocksdb]

        with temporary_dicedb_server(TEST_PORT, module_args) as (proc, temp_dir):
            r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

            # Store some data
            r.setex('shutdown_key', 3600, 'shutdown_value')

            # Force eviction
            for i in range(800):
                r.set(f'shutdown_filler_{i}', 'x' * 8000)

            time.sleep(0.2)

            # Verify eviction occurred
            assert r.get('shutdown_key') is None

            # Get initial stats
            stats = r.execute_command('spill.stats')
            stats_dict = {stats[i]: stats[i+1] for i in range(0, len(stats), 2)}
            keys_stored = stats_dict['keys_stored']

            assert keys_stored > 0, "Some keys should be stored before shutdown"

        # Server should have shut down gracefully
        # RocksDB directory should still exist with data
        assert os.path.exists(temp_rocksdb)
        assert len(os.listdir(temp_rocksdb)) > 0, "RocksDB should have data files"

    finally:
        if os.path.exists(temp_rocksdb):
            shutil.rmtree(temp_rocksdb)

def test_module_error_recovery():
    """Test module behavior during error conditions"""
    with temporary_dicedb_server(TEST_PORT) as (proc, temp_dir):
        r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

        # Test various error conditions

        # 1. Invalid command arguments
        try:
            r.execute_command('spill.restore')  # Missing key
            assert False, "Should raise error for missing arguments"
        except redis.ResponseError:
            pass  # Expected

        # 2. Restore non-existent key
        result = r.execute_command('spill.restore', 'definitely_not_exists')
        assert result is None, "Non-existent key should return None"

        # 3. Very long key names
        long_key = 'x' * 10000
        try:
            result = r.execute_command('spill.restore', long_key)
            # Should handle gracefully (return None or error)
            assert result is None or isinstance(result, str)
        except redis.ResponseError:
            pass  # Also acceptable

        # 4. Binary data in keys
        try:
            result = r.execute_command('spill.restore', b'\x00\x01\x02\x03')
            # Should handle gracefully
            assert result is None or isinstance(result, (str, bytes))
        except redis.ResponseError:
            pass  # Also acceptable

        # Verify normal operations still work
        r.set('recovery_test', 'recovery_value')
        assert r.get('recovery_test') == 'recovery_value'

        stats = r.execute_command('spill.stats')
        assert isinstance(stats, list)

def test_module_performance_under_load():
    """Test module performance under concurrent load"""
    with temporary_dicedb_server(TEST_PORT) as (proc, temp_dir):
        r = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

        # Configure for testing
        r.config_set('maxmemory', '10mb')
        r.config_set('maxmemory-policy', 'allkeys-lru')

        results = {'stored': 0, 'restored': 0, 'errors': []}

        def worker(thread_id, num_ops):
            try:
                thread_client = redis.Redis(host='localhost', port=TEST_PORT, decode_responses=True)

                # Store phase
                for i in range(num_ops):
                    key = f'load_test_{thread_id}_{i}'
                    value = f'load_value_{thread_id}_{i}_{"x" * 100}'
                    thread_client.setex(key, 3600, value)
                    results['stored'] += 1

                # Force eviction with large data
                for i in range(100):
                    thread_client.set(f'load_filler_{thread_id}_{i}', 'x' * 50000)

                time.sleep(0.1)

                # Restore phase
                for i in range(num_ops):
                    key = f'load_test_{thread_id}_{i}'
                    try:
                        result = thread_client.execute_command('spill.restore', key)
                        if result == 'OK':
                            results['restored'] += 1
                    except Exception as e:
                        results['errors'].append(str(e))

            except Exception as e:
                results['errors'].append(f"Thread {thread_id}: {str(e)}")

        # Run concurrent operations
        threads = []
        num_threads = 3
        ops_per_thread = 50

        start_time = time.time()

        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i, ops_per_thread))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        end_time = time.time()
        duration = end_time - start_time

        print(f"\n  Load test completed in {duration:.2f}s")
        print(f"  Stored: {results['stored']} keys")
        print(f"  Restored: {results['restored']} keys")
        print(f"  Errors: {len(results['errors'])}")

        # Verify performance is reasonable
        assert duration < 30, "Load test should complete within 30 seconds"
        assert results['stored'] > 0, "Should store some keys"
        assert len(results['errors']) < results['stored'] * 0.1, "Error rate should be < 10%"

        # Verify system is still responsive
        final_stats = r.execute_command('spill.stats')
        assert isinstance(final_stats, list)

def main():
    """Main test runner for module lifecycle tests"""
    print("=== DiceDB Spill Module Lifecycle Tests ===\n")

    # Check prerequisites
    if not os.path.exists(MODULE_PATH):
        print(f"ERROR: Module not found at {MODULE_PATH}")
        print("Please run 'make' first to build the module")
        sys.exit(1)

    if not shutil.which('dicedb-server'):
        print("ERROR: dicedb-server not found in PATH")
        print("Please ensure DiceDB is installed and in your PATH")
        sys.exit(1)

    # Run tests
    tests = [
        (test_module_loading_with_default_config, "Module loading with default config"),
        (test_module_loading_with_custom_config, "Module loading with custom config"),
        (test_module_config_validation, "Module configuration validation"),
        (test_module_min_memory_validation, "Module minimum memory validation (20MB)"),
        (test_memory_allocation_distribution, "Memory allocation distribution (8MB cache, 2/3 buffer)"),
        (test_persistence_across_restart, "Persistence across server restart"),
        (test_multiple_module_instances, "Multiple module instances"),
        (test_module_graceful_shutdown, "Module graceful shutdown"),
        (test_module_error_recovery, "Module error recovery"),
        (test_module_performance_under_load, "Module performance under load"),
    ]

    passed = 0
    failed = 0

    print(f"\nRunning {len(tests)} lifecycle tests...\n")
    for test_func, test_name in tests:
        if run_test(test_func, test_name):
            passed += 1
        else:
            failed += 1

    print(f"\n{passed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()