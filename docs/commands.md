# Spill Module Commands

Spill module automatically persists evicted keys to RocksDB storage, providing
disk bound cache capacity with transparent key restoration and full TTL preservation.
The module provides four commands

## SPILL.RESTORE

```
SPILL.RESTORE key
```

Manually restore an evicted key from RocksDB storage.

Normally, keys are restored automatically when accessed after eviction.
This command allows explicit restoration without requiring a key access.

Return value:

- `OK` - Key was successfully restored
- `(nil)` - Key not found in RocksDB
- `ERR Key has expired` - Key was found but expired
- `ERR Corrupted data in RocksDB` - Data corruption detected
- `ERR RocksDB not initialized` - Module not properly initialized

Examples:

```
> SPILL.RESTORE mykey
OK

> SPILL.RESTORE nonexistent
(nil)
```

**Notes:**

- Restored keys are removed from RocksDB after successful restoration
- TTL is preserved during restoration
- If a key with the same name exists in memory, it will be replaced

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

- All memory values show bytes and MB
- Estimated values are approximate
- Use this command to monitor RocksDB health and memory usage

## SPILL.CLEANUP

```
SPILL.CLEANUP
```

Cleanup command scans entire RocksDB and remove all expired keys.

This is a manual cleanup operation. The module automatically cleans expired
keys during restore attempts, but this command performs a full scan to cleanup
and delete the expired keys.

Return value:

Array with cleanup statistics:

- `keys_checked` - Number of keys scanned in RocksDB
- `keys_removed` - Number of expired keys deleted

Example:

```
> SPILL.CLEANUP
1) "keys_checked"
2) (integer) 1523
3) "keys_removed"
4) (integer) 47
```

**Notes:**

- This operation scans the entire RocksDB database
- Performance depends on number of keys in RocksDB
- Removed keys increment the `keys_expired` counter in SPILL.INFO
- Use periodically to reclaim disk space from expired keys

## SPILL.STATS

```
SPILL.STATS
```

Returns basic statistics about Spill module operations.

This command is a simpler alternative to SPILL.INFO, returning only core metrics as an array.

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
- Counters reset when module is reloaded
- Use SPILL.INFO for comprehensive metrics including RocksDB internals
