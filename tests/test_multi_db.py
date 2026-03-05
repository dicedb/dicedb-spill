#!/usr/bin/env python3
"""
Multi-database and directory cleanup tests for DiceDB Spill module

Tests:
1. All databases cleaned up on FLUSHALL (num_keys_stored → 0, all keys unrestorable)
2. Single database cleaned up on FLUSHDB (other databases unaffected)
3. Key stored in the correct RocksDB instance (per database isolation)
4. Directory completely clean when module is loaded (no stale data from prior run)
"""

import os
import sys
import time
import tempfile
import shutil
import signal
import socket
import subprocess
import unittest

try:
    import valkey as redis
except ImportError:
    try:
        import redis
    except ImportError:
        print("ERROR: Neither valkey nor redis Python library found")
        sys.exit(1)

REDIS_PORT = 6379
MODULE_PATH = os.path.join(os.path.dirname(__file__), "../lib-spill.so")
LIFECYCLE_PORT = 8381  # Separate port to avoid conflicts with existing server


# ---------------------------------------------------------------------------
# Helpers shared by lifecycle tests
# ---------------------------------------------------------------------------

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def start_server(port, spill_path, extra_module_args=None):
    """Start a DiceDB server with the spill module and return the process."""
    cmd = [
        'dicedb-server',
        '--port', str(port),
        '--save', '',
        '--appendonly', 'no',
        '--loadmodule', os.path.abspath(MODULE_PATH),
        'path', spill_path,
        'cleanup-interval', '0',
    ]
    if extra_module_args:
        cmd.extend(extra_module_args)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid,
    )

    # Wait up to 5 seconds for the server to accept connections
    for _ in range(50):
        if is_port_in_use(port):
            break
        time.sleep(0.1)
    else:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        raise RuntimeError(f"Server on port {port} never became ready")

    return proc


def stop_server(proc):
    """Gracefully stop a server process."""
    try:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests against the already-running server on REDIS_PORT
# ---------------------------------------------------------------------------

class MultiDatabaseTests(unittest.TestCase):
    """Tests requiring a live DiceDB server with the spill module on port 6379."""

    @classmethod
    def setUpClass(cls):
        cls.client = redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=True)
        try:
            cls.client.ping()
        except redis.ConnectionError:
            raise unittest.SkipTest(
                "DiceDB server not reachable on port 6379. "
                "Start it with the spill module before running these tests."
            )

        # Confirm the spill module is loaded
        try:
            cls.client.execute_command('SPILL.CLEANUP')
        except redis.ResponseError as exc:
            if 'unknown command' in str(exc).lower():
                raise unittest.SkipTest("Spill module is not loaded on the running server.")

    def setUp(self):
        # Start each test with a clean slate on all DBs
        self.client.execute_command('FLUSHALL')
        time.sleep(0.1)

    def tearDown(self):
        self.client.execute_command('FLUSHALL')
        time.sleep(0.1)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _select(self, db):
        self.client.execute_command('SELECT', db)

    def _get_num_keys_stored(self):
        raw = self.client.execute_command('INFO', 'spill')
        if isinstance(raw, dict):
            for key in ('spill_num_keys_stored', 'num_keys_stored'):
                if key in raw:
                    return int(raw[key])
            return None
        for line in raw.split('\r\n'):
            line = line.strip()
            if line.startswith('spill_num_keys_stored:') or line.startswith('num_keys_stored:'):
                return int(line.split(':', 1)[1])
        return None

    def _evict(self, key):
        """Explicitly evict a key via the EVICT command."""
        return self.client.execute_command('EVICT', key)

    def _restore(self, key):
        return self.client.execute_command('SPILL.RESTORE', key)

    # ------------------------------------------------------------------
    # 1. FLUSHALL cleans up all databases
    # ------------------------------------------------------------------

    def test_flushall_cleans_all_databases(self):
        """FLUSHALL must wipe every RocksDB instance; no key should be restorable afterward."""
        # Evict one key in DB 0
        self._select(0)
        self.client.set('db0_key', 'db0_value')
        self._evict('db0_key')

        # Evict one key in DB 1
        self._select(1)
        self.client.set('db1_key', 'db1_value')
        self._evict('db1_key')

        # Confirm both keys are stored in RocksDB
        self._select(0)
        num_before = self._get_num_keys_stored()
        self.assertIsNotNone(num_before)
        self.assertGreaterEqual(num_before, 2)

        # FLUSHALL
        self.client.execute_command('FLUSHALL')
        time.sleep(0.1)

        # num_keys_stored must be 0
        num_after = self._get_num_keys_stored()
        self.assertEqual(num_after, 0, "num_keys_stored should be 0 after FLUSHALL")

        # SPILL.RESTORE must return nil for both keys
        self._select(0)
        result0 = self._restore('db0_key')
        self.assertIsNone(result0, "DB 0 key should not be restorable after FLUSHALL")

        self._select(1)
        result1 = self._restore('db1_key')
        self.assertIsNone(result1, "DB 1 key should not be restorable after FLUSHALL")

    def test_flushall_with_multiple_databases(self):
        """FLUSHALL across three databases: all evicted keys become unrestorable."""
        for db in range(3):
            self._select(db)
            self.client.set(f'key_db{db}', f'value_db{db}')
            self._evict(f'key_db{db}')

        self.client.execute_command('FLUSHALL')
        time.sleep(0.1)

        self.assertEqual(self._get_num_keys_stored(), 0)

        for db in range(3):
            self._select(db)
            self.assertIsNone(
                self._restore(f'key_db{db}'),
                f"DB {db} key must not be restorable after FLUSHALL",
            )

    # ------------------------------------------------------------------
    # 2. FLUSHDB cleans up a single database, leaving others intact
    # ------------------------------------------------------------------

    def test_flushdb_cleans_only_target_database(self):
        """FLUSHDB on DB 0 must not affect keys evicted from DB 1."""
        # Evict a key in DB 0
        self._select(0)
        self.client.set('db0_target', 'value0')
        self._evict('db0_target')

        # Evict a key in DB 1
        self._select(1)
        self.client.set('db1_safe', 'value1')
        self._evict('db1_safe')

        # Flush only DB 0
        self._select(0)
        self.client.execute_command('FLUSHDB')
        time.sleep(0.1)

        # DB 0 key must be gone
        self._select(0)
        result0 = self._restore('db0_target')
        self.assertIsNone(result0, "DB 0 key must be unrestorable after FLUSHDB on DB 0")

        # DB 1 key must still be restorable
        self._select(1)
        result1 = self._restore('db1_safe')
        self.assertEqual(result1, 'OK', "DB 1 key must still be restorable after FLUSHDB on DB 0")

        # Confirm the DB 1 value is correct
        value1 = self.client.get('db1_safe')
        self.assertEqual(value1, 'value1')

    def test_flushdb_leaves_other_databases_intact(self):
        """Flushing DB 2 must leave DB 0 and DB 1 intact."""
        for db in [0, 1, 2]:
            self._select(db)
            self.client.set(f'key{db}', f'val{db}')
            self._evict(f'key{db}')

        # Flush only DB 2
        self._select(2)
        self.client.execute_command('FLUSHDB')
        time.sleep(0.1)

        # DB 2 key gone
        self._select(2)
        self.assertIsNone(self._restore('key2'))

        # DB 0 and DB 1 keys must survive
        self._select(0)
        self.assertEqual(self._restore('key0'), 'OK')
        self.assertEqual(self.client.get('key0'), 'val0')

        self._select(1)
        self.assertEqual(self._restore('key1'), 'OK')
        self.assertEqual(self.client.get('key1'), 'val1')

    def test_flushdb_num_keys_stored_decreases(self):
        """num_keys_stored must reflect the removal of only the flushed database's keys."""
        self._select(0)
        self.client.set('only_db0_key', 'v')
        self._evict('only_db0_key')

        num_before = self._get_num_keys_stored()
        self.assertGreater(num_before, 0)

        # Flush DB 0
        self._select(0)
        self.client.execute_command('FLUSHDB')
        time.sleep(0.1)

        num_after = self._get_num_keys_stored()
        self.assertLess(num_after, num_before,
                        "num_keys_stored should decrease after FLUSHDB")

    # ------------------------------------------------------------------
    # 3. Key stored in the correct RocksDB instance (per-database isolation)
    # ------------------------------------------------------------------

    def test_evicted_key_only_restorable_from_originating_database(self):
        """
        A key evicted while in DB 0 must only be restorable from DB 0.
        Attempting SPILL.RESTORE from DB 1 must return nil.
        """
        self._select(0)
        self.client.set('isolated_key', 'isolated_value')
        self._evict('isolated_key')

        # Trying to restore from a different database must fail
        self._select(1)
        wrong_db_result = self._restore('isolated_key')
        self.assertIsNone(
            wrong_db_result,
            "Restoring a key from the wrong database should return nil",
        )

        # Restoring from the correct database must succeed
        self._select(0)
        correct_db_result = self._restore('isolated_key')
        self.assertEqual(
            correct_db_result, 'OK',
            "Restoring from the originating database should return OK",
        )
        self.assertEqual(self.client.get('isolated_key'), 'isolated_value')

    def test_same_key_name_in_different_databases_are_independent(self):
        """
        The same key name in DB 0 and DB 1 must be stored independently in
        their respective RocksDB instances and must not bleed across databases.
        """
        # Set and evict 'shared_name' in DB 0
        self._select(0)
        self.client.set('shared_name', 'value_from_db0')
        self._evict('shared_name')

        # Set and evict 'shared_name' in DB 1 with a different value
        self._select(1)
        self.client.set('shared_name', 'value_from_db1')
        self._evict('shared_name')

        # Restore from DB 0 and verify value matches DB 0's write
        self._select(0)
        self.assertEqual(self._restore('shared_name'), 'OK')
        self.assertEqual(self.client.get('shared_name'), 'value_from_db0',
                         "DB 0 restore must return DB 0's value")

        # Restore from DB 1 and verify value matches DB 1's write
        self._select(1)
        self.assertEqual(self._restore('shared_name'), 'OK')
        self.assertEqual(self.client.get('shared_name'), 'value_from_db1',
                         "DB 1 restore must return DB 1's value")

    def test_key_in_db1_not_visible_in_db0(self):
        """A key evicted from DB 1 must not appear when SPILL.RESTORE is called from DB 0."""
        self._select(1)
        self.client.set('db1_only', 'secret')
        self._evict('db1_only')

        self._select(0)
        result = self._restore('db1_only')
        self.assertIsNone(result, "DB 0 must not see keys stored for DB 1")


# ---------------------------------------------------------------------------
# 4. Directory completely clean when the module is loaded
# ---------------------------------------------------------------------------

class DirectoryCleanOnLoadTests(unittest.TestCase):
    """
    Verify that the module destroys any pre-existing RocksDB data at the
    configured path when it starts up.

    These tests require 'dicedb-server' in PATH and the built lib-spill.so.
    """

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MODULE_PATH):
            raise unittest.SkipTest(
                f"Module binary not found at {MODULE_PATH}. Run 'make' first."
            )
        if not shutil.which('dicedb-server'):
            raise unittest.SkipTest(
                "dicedb-server not found in PATH. Install DiceDB first."
            )
        if is_port_in_use(LIFECYCLE_PORT):
            raise unittest.SkipTest(
                f"Port {LIFECYCLE_PORT} is already in use."
            )

    def test_module_starts_with_empty_state(self):
        """
        After the module loads, num_keys_stored must be 0 regardless of any
        data that existed on disk from a previous run.
        """
        spill_path = tempfile.mkdtemp(prefix='spill_clean_load_')
        try:
            proc = start_server(LIFECYCLE_PORT, spill_path)
            try:
                r = redis.Redis(host='localhost', port=LIFECYCLE_PORT, decode_responses=True)

                raw = r.execute_command('INFO', 'spill')
                num_stored = None
                for line in raw.split('\r\n'):
                    line = line.strip()
                    if 'num_keys_stored:' in line:
                        num_stored = int(line.split(':', 1)[1])
                        break

                self.assertIsNotNone(num_stored, "INFO spill must include num_keys_stored")
                self.assertEqual(num_stored, 0,
                                 "num_keys_stored must be 0 immediately after module load")
            finally:
                stop_server(proc)
        finally:
            shutil.rmtree(spill_path, ignore_errors=True)

    def test_stale_rocksdb_data_destroyed_on_reload(self):
        """
        Data written to RocksDB by a previous server run must NOT be accessible
        after the module reloads with the same path (destroy_db on open).
        """
        spill_path = tempfile.mkdtemp(prefix='spill_stale_data_')
        try:
            # ---- First server run: evict a key so it lands in RocksDB ----
            proc1 = start_server(LIFECYCLE_PORT, spill_path)
            try:
                r1 = redis.Redis(host='localhost', port=LIFECYCLE_PORT, decode_responses=True)
                r1.set('stale_key', 'stale_value')
                r1.execute_command('EVICT', 'stale_key')

                # Confirm the eviction was persisted
                result = r1.execute_command('SPILL.RESTORE', 'stale_key')
                self.assertEqual(result, 'OK',
                                 "Key must be restorable within the first server run")
            finally:
                stop_server(proc1)

            # Wait for the port to be freed before starting the second server
            for _ in range(30):
                if not is_port_in_use(LIFECYCLE_PORT):
                    break
                time.sleep(0.1)

            # ---- Second server run: same path, module should wipe old data ----
            proc2 = start_server(LIFECYCLE_PORT, spill_path)
            try:
                r2 = redis.Redis(host='localhost', port=LIFECYCLE_PORT, decode_responses=True)

                # num_keys_stored must start at 0 (old data destroyed on open)
                raw = r2.execute_command('INFO', 'spill')
                num_stored = None
                for line in raw.split('\r\n'):
                    line = line.strip()
                    if 'num_keys_stored:' in line:
                        num_stored = int(line.split(':', 1)[1])
                        break

                self.assertEqual(num_stored, 0,
                                 "num_keys_stored must be 0 on fresh load (stale data destroyed)")

                # The stale key must not be restorable
                result = r2.execute_command('SPILL.RESTORE', 'stale_key')
                self.assertIsNone(result,
                                  "Stale key from previous run must not be restorable after reload")
            finally:
                stop_server(proc2)

        finally:
            shutil.rmtree(spill_path, ignore_errors=True)

    def test_rocksdb_directories_recreated_fresh_on_load(self):
        """
        After the module loads, each db<N> sub-directory must exist (created by
        RocksDB) but must contain no user keys.
        """
        spill_path = tempfile.mkdtemp(prefix='spill_dir_fresh_')
        try:
            proc = start_server(LIFECYCLE_PORT, spill_path)
            try:
                r = redis.Redis(host='localhost', port=LIFECYCLE_PORT, decode_responses=True)

                # db0 directory must exist (RocksDB opens it on load)
                db0_path = os.path.join(spill_path, 'db0')
                self.assertTrue(os.path.isdir(db0_path),
                                "db0 directory must be created by the module on load")

                # No user data means SPILL.RESTORE returns nil for any key
                result = r.execute_command('SPILL.RESTORE', 'phantom_key')
                self.assertIsNone(result,
                                  "No user data should exist in a freshly opened RocksDB")
            finally:
                stop_server(proc)
        finally:
            shutil.rmtree(spill_path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("=== DiceDB Spill Multi-Database and Directory Cleanup Tests ===\n")

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(MultiDatabaseTests))
    suite.addTests(loader.loadTestsFromTestCase(DirectoryCleanOnLoadTests))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == '__main__':
    main()
