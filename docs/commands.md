# Spill Module Commands

Spill module transparently persists evicted keys to disk (RocksDB), providing
disk-bound cache capacity with transparent key restoration and full TTL preservation.

## Overview

When keys are evicted (either automatically due to memory pressure with an eviction policy,
or manually using the `EVICT` command), they are transparently moved to RocksDB. These keys
remain logically accessible - any read operation automatically restores them back to memory.

### Automatic Cleanup

The module runs a background thread that periodically scans RocksDB to remove expired keys.
This cleanup interval can be configured using the `cleanup-interval` parameter (default: 300 seconds).
The automatic cleanup runs asynchronously and does not block the main DiceDB thread.

### Configuration

The Spill module accepts the following configuration parameters at load time:

- `max-memory` - Memory budget for RocksDB in bytes (e.g., `268435456` for 256MB)
- `cleanup-interval` - Seconds between automatic cleanup runs (default: 300)

Example module load:
```
loadmodule /path/to/spill.so max-memory 268435456 cleanup-interval 600
```

The module provides four commands for management and monitoring

## SPILL.RESTORE

```
SPILL.RESTORE key
```

Manually restore an evicted key from RocksDB storage.

After eviction, keys are automatically restored when accessed through commands like
GET, EXISTS, TYPE, LRANGE, SMEMBERS, HGETALL, etc. This command allows explicit
restoration without triggering an access command.

Return value:

- `OK` - Key was successfully restored
- `(nil)` - Key not found in RocksDB
- `ERR Key has expired` - Key was found but expired
- `ERR Corrupted data in RocksDB` - Data corruption detected
- `ERR RocksDB not initialized` - Module not properly initialized

Examples:

```
> SET mykey "myvalue"
OK

> EVICT mykey
mykey

> KEYS *
(empty array)

> SPILL.RESTORE mykey
OK

> GET mykey
"myvalue"

> SPILL.RESTORE nonexistent
(nil)

> SET existing "in_memory"
OK

> SET evicted "in_rocksdb"
OK

> EVICT evicted
evicted

> SET evicted "new_memory_value"
OK

> SPILL.RESTORE evicted
(nil)

> GET evicted
"new_memory_value"
```

**Notes:**

- Restored keys are removed from RocksDB after successful restoration
- TTL is preserved during restoration
- If a key with the same name already exists in memory, restoration is skipped and returns `(nil)`
- After eviction, keys logically still exist and accessing them automatically triggers restoration
- Works with all Redis data types: strings, lists, sets, hashes, sorted sets, etc.

## SPILL.INFO

```
SPILL.INFO
```

Returns statistics and metrics about the Spill module and RocksDB.
The command returns the info metrics in four sections

### spill

Module-specific metrics and configuration.

- `keys_stored` - Total keys written to RocksDB since module load
- `keys_restored` - Total keys read back from RocksDB since module load
- `keys_expired` - Total expired keys encountered and deleted
- `keys_cleaned` - Total keys removed by SPILL.CLEANUP command
- `bytes_written` - Total bytes written to RocksDB (includes metadata)
- `bytes_read` - Total bytes read from RocksDB (includes metadata)
- `path` - RocksDB database directory path
- `max_memory` - Configured memory budget for RocksDB in bytes and MB
- `cleanup_interval` - Automatic cleanup interval in seconds (configurable)

### rocksdb_memory

Memory usage breakdown for RocksDB internal structures.

- `block_cache_usage` - Memory used by block cache (cached data blocks)
- `block_cache_pinned_usage` - Memory pinned in block cache (cannot be evicted)
- `memtable_size` - Memory used by active and immutable memtables
- `table_readers_mem` - Memory used by SSTable readers (indexes, filters)

### rocksdb_storage

Storage and data statistics.

- `estimated_keys` - Approximate number of keys in RocksDB
- `live_data_size` - Estimated size of live data (excludes obsolete data)
- `total_sst_files_size` - Total size of all SST files on disk
- `num_snapshots` - Number of active snapshots

### rocksdb_compaction

Compaction status and background operation metrics.

- `num_immutable_memtables` - Immutable memtables waiting to be flushed
- `memtable_flush_pending` - `yes` if memtable flush is pending, `no` otherwise
- `compaction_pending` - `yes` if compaction is pending, `no` otherwise
- `background_errors` - Count of background errors (should be 0)
- `base_level` - LSM base level for compaction
- `num_files_L0` through `num_files_L6` - Number of files at each LSM level

Example:

```
> SPILL.INFO
# spill
keys_stored:1523
keys_restored:847
keys_expired:23
keys_cleaned:23
bytes_written:45678901
bytes_read:23456789
path:/tmp/dicedb-l2
max_memory:268435456 (256MB)
cleanup_interval:300

# rocksdb_memory
block_cache_usage:4194304 (4MB)
block_cache_pinned_usage:524288 (0MB)
memtable_size:16777216 (16MB)
table_readers_mem:2097152 (2MB)

# rocksdb_storage
estimated_keys:676
live_data_size:28311552 (27MB)
total_sst_files_size:30408704 (29MB)
num_snapshots:0

# rocksdb_compaction
num_immutable_memtables:0
memtable_flush_pending:no
compaction_pending:no
background_errors:0
base_level:1
num_files_L0:2
num_files_L1:3
num_files_L2:5
```

**Notes:**

- All memory values show bytes and MB (e.g., `262144000 (250MB)`)
- Estimated values (like `estimated_keys`) are approximate
- Use this command to monitor RocksDB health and memory usage
- The `num_files_L0` through `num_files_L6` show the distribution of SST files across LSM tree levels
- The `cleanup_interval` parameter can be configured at module load time (default: 300 seconds)
- The `keys_cleaned` counter includes both automatic periodic cleanups and manual SPILL.CLEANUP invocations

## SPILL.CLEANUP

```
SPILL.CLEANUP
```

Manually scans the entire RocksDB database and removes all expired keys.

**⚠️ Warning**: This is a blocking operation that runs on the main DiceDB thread and may
stall other commands while executing. Use with caution in production environments.

This is a manual cleanup operation that performs a full database scan. The module automatically:
1. Cleans expired keys during restore attempts (returning `ERR Key has expired`)
2. Runs periodic background cleanup every `cleanup-interval` seconds (default: 300)

Use this command only when you need to immediately reclaim disk space from expired keys
or when the automatic cleanup interval is not sufficient.

Return value:

Array with cleanup statistics:

- `keys_checked` - Number of keys scanned in RocksDB
- `keys_removed` - Number of expired keys deleted

Example:

```
> SET temp1 "value" EX 1
OK

> SET temp2 "value" EX 1
OK

> EVICT temp1 temp2
temp1
temp2

(wait for keys to expire...)

> SPILL.CLEANUP
1) "keys_checked"
2) (integer) 1523
3) "keys_removed"
4) (integer) 47

> SPILL.INFO
...
keys_expired:47
...
```

**Notes:**

- **This is a blocking operation** - it runs on the main thread and may impact performance
- Scans the entire RocksDB database sequentially
- Performance depends on the number of keys in RocksDB
- Removed keys increment both the `keys_expired` and `keys_cleaned` counters in SPILL.INFO
- In most cases, rely on the automatic periodic cleanup instead of using this command
- Returns an array (displayed as alternating key-value pairs in CLI)

## SPILL.STATS

```
SPILL.STATS
```

Returns basic statistics about Spill module operations.

This command is a simpler alternative to SPILL.INFO, returning only core operational
metrics as an array without RocksDB internals.

Return value:

Array with statistics:

- `keys_stored` - Total keys written to RocksDB
- `keys_restored` - Total keys read from RocksDB
- `keys_expired` - Total expired keys encountered
- `keys_cleaned` - Total keys removed by cleanup
- `bytes_written` - Total bytes written to RocksDB
- `bytes_read` - Total bytes read from RocksDB

Example:

```
> SPILL.STATS
 1) "keys_stored"
 2) (integer) 1523
 3) "keys_restored"
 4) (integer) 847
 5) "keys_expired"
 6) (integer) 23
 7) "keys_cleaned"
 8) (integer) 23
 9) "bytes_written"
10) (integer) 45678901
11) "bytes_read"
12) (integer) 23456789
```

**Notes:**

- All counters are cumulative since module load
- Counters reset when the module is reloaded or server is restarted
- Returns an array (displayed as alternating key-value pairs in CLI)
- Use SPILL.INFO for comprehensive metrics including RocksDB memory and storage internals
- The `bytes_written` and `bytes_read` include metadata overhead, not just key-value data
