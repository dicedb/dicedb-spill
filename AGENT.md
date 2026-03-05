# Agent Guide — dicedb-spill

## Project Overview

A DiceDB/Valkey module (`lib-spill.so`) that spills evicted keys to RocksDB and transparently restores them on cache miss. Written in C, lives as a git submodule at `modules/dicedb-spill/` inside the DiceDB repo.

## Repository Layout

```
spill.c                        # Entire module implementation (single file)
Makefile                       # Build + test targets
docs/
  commands.md                  # SPILL.RESTORE, SPILL.CLEANUP, EVICT docs
  configuration.md             # Module args: path, max-memory, cleanup-interval
scripts/
  install_rocksdb.sh           # RocksDB build/install script
tests/
  test_unit.c                  # Standalone C unit tests (no server needed)
  integration_test.py          # Integration tests (server required, port 6379)
  test_commands_and_cleanup.py # Commands + cleanup thread tests
  test_multi_db.py             # Multi-database isolation + FLUSHDB/FLUSHALL tests
  test_edge_cases.py           # Edge cases
  test_advanced_scenarios.py   # Advanced scenarios
  test_module_lifecycle.py     # Module load/unload lifecycle (needs dicedb-server in PATH)
  run_tests.sh                 # Master test runner
  run_integration_tests.sh     # Integration test runner
  requirements.txt             # Python deps: redis, valkey
```

## Build

```bash
# From the dicedb-spill directory
make                        # Produces lib-spill.so
make ENABLE_COMPRESSION=1   # With snappy/zlib/bz2/lz4/zstd compression
make clean
```

Dependencies: RocksDB installed at `/usr/local/lib` and `/usr/local/include`. Install via:

```bash
sudo ./scripts/install_rocksdb.sh
```

## Start the Server (required for integration/multi-db tests)

```bash
# Create data dir first
mkdir -p /tmp/data

# Start server (run from the dicedb repo root)
cd ~/workspace/dicedb/dicedb
src/dicedb-server \
    --protected-mode no \
    --loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data max-memory 262144000 cleanup-interval 200
```

The server listens on port 6379. The module wipes all RocksDB data on every load (fresh start).

## Run Tests

```bash
make test   # Runs all test suites
```

Tests run in this order:
1. Unit (`test_unit.c`) — standalone C, no server needed
2. Integration (`integration_test.py`) — requires server on port 6379
3. Edge cases (`test_edge_cases.py`) — requires server on port 6379
4. Commands & cleanup (`test_commands_and_cleanup.py`) — requires server on port 6379
5. Multi-DB (`test_multi_db.py`) — requires server on port 6379
6. Lifecycle (`test_module_lifecycle.py`) — requires `dicedb-server` in PATH (skipped if absent)
7. Advanced (`test_advanced_scenarios.py`) — requires server on port 6379

Run a specific suite:
```bash
cd tests && ./run_tests.sh --multi-db-only
cd tests && ./run_tests.sh --unit-only
cd tests && ./run_tests.sh --integration-only
```

## Key Implementation Details

`spill.c` — single source file, ~1100 lines:
- One `rocksdb_t*` per DiceDB database, array sized from `CONFIG GET databases` (default 16)
- RocksDB paths: `{config.path}/db0`, `{config.path}/db1`, ...
- Data format stored per key: `[8 bytes expiry_ms][N bytes DUMP output]`
- Events subscribed: `preeviction` (store key → RocksDB), `premiss` (restore key ← RocksDB)
- Server events: `FlushDB` → wipes RocksDB for affected DB(s)
- Module args: `path <dir>`, `max-memory <bytes>` (min 20MB), `cleanup-interval <seconds>`
- Commands: `SPILL.RESTORE <key>` → OK / nil; `SPILL.CLEANUP` → array of stats
- INFO section: `INFO spill` — returns stats dict (not a raw string) in newer clients
- Thread safety: `pthread_rwlock_t db_rwlock` — cleanup thread holds read lock; FLUSHDB holds write lock

## Common Pitfalls

- `INFO spill` via `execute_command()` returns a dict (not a string) in redis-py/valkey >= 6.x. Keys are prefixed: `spill_num_keys_stored`, `spill_total_keys_written`, etc.
- The data dir must exist before starting the server (`mkdir -p /tmp/data`).
- `cleanup-interval 0` disables the background cleanup thread entirely.
- `SPILL.RESTORE` returns `nil` (not an error) when the key is not in RocksDB.
- After `FLUSHDB`/`FLUSHALL`, the corresponding RocksDB instance(s) are destroyed and recreated — `num_keys_stored` resets to 0.
- The module always starts fresh — existing RocksDB data at the configured path is destroyed on load.
