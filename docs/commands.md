# Spill Module Commands

Spill module transparently persists evicted keys to disk (RocksDB), providing
disk-bound cache capacity with transparent key restoration and full TTL preservation.

## Overview

When keys are evicted (either automatically due to memory pressure with an eviction policy,
or manually using the [`EVICT` command](https://dicedb.io/commands/evict)), they are transparently
moved to RocksDB. These keys remain logically accessible - any read operation
restores them back to memory.

### Automatic Cleanup

The module runs a background thread that periodically scans RocksDB to remove expired keys.
This cleanup interval can be configured using the `cleanup-interval` parameter (default: 300 seconds).
The automatic cleanup runs asynchronously and does not block the main DiceDB thread.

### Configuration

The Spill module accepts the following configuration parameters at load time:

- `path` - Path on the disk where evicted keys are to be stored
- `max-memory` - Memory budget for RocksDB in bytes (e.g., `268435456` for 256MB)
- `cleanup-interval` - Seconds between automatic cleanup runs (default: 300)

Configurations and their details can be found at [docs/configuration.md](docs/configuration.md).

```
loadmodule /path/to/spill.so path /tmp/dicedb-data max-memory 268435456 cleanup-interval 600
```

## Statistics and Monitoring

Spill statistics are automatically integrated into the standard `INFO` and `INFO ALL` commands.
You can view spill metrics alongside other server statistics without needing a separate command.

The spill metrics are split across two sections

1. stats - surfacing all the key stats related to Spill module
2. config - surfacing the configuration of the module

### stats section

- `num_keys_stored` - Number of keys currently in RocksDB (approximated)
- `total_keys_written` - Total write operations to RocksDB since server restart (increments on every write, including overwrites)
- `total_keys_restored` - Total keys restored from RocksDB since server restart
- `total_keys_cleaned` - Total keys removed due to expiration since server restart (includes keys expired during restore attempts and cleanup operations)
- `last_num_keys_cleaned` - Number of keys cleaned up in the most recent cleanup job (manual or automatic)
- `last_cleanup_at` - Unix timestamp in seconds when the last cleanup was run (0 if never run)
- `total_bytes_written` - Total bytes written to RocksDB since server restart (includes metadata overhead)
- `total_bytes_read` - Total bytes read from RocksDB since server restart (includes metadata overhead)

### config section

- `path` - Path to RocksDB storage directory
- `max_memory_bytes` - Maximum memory budget for RocksDB in bytes
- `cleanup_interval_seconds` - Interval between automatic cleanup runs in seconds

Example:

```
> INFO spill
# stats
spill_num_keys_stored:1523
spill_total_keys_written:2347
spill_total_keys_restored:847
spill_total_keys_cleaned:23
spill_last_num_keys_cleaned:5
spill_last_cleanup_at:1707512345
spill_total_bytes_written:45678901
spill_total_bytes_read:23456789

# config
spill_path:/var/lib/dicedb/spill
spill_max_memory_bytes:268435456
spill_cleanup_interval_seconds:300
```

## Commands

The module provides two commands for management and monitoring

## SPILL.RESTORE

```
SPILL.RESTORE key
```

Manually restore an evicted key from RocksDB storage.

After eviction, keys are automatically restored when accessed through commands like
`GET`, `EXISTS`, `TYPE`, `LRANGE`, `SMEMBERS`, `HGETALL`, etc. This command allows explicit
restoration without triggering an access command.

Return value:

- `OK` - Key was successfully restored
- `(nil)` - Key not found in RocksDB
- `ERR Key has expired` - Key was found but expired
- `ERR Corrupted data in RocksDB` - Data corruption detected
- `ERR RocksDB not initialized` - Module not properly initialized

Example:

```
> SET k1 v1
OK

> EVICT k1
k1

> KEYS *
(empty array)

> GET k1
v1
```

- Restored keys are removed from RocksDB after successful restoration
- TTL is preserved during restoration
- If a key with the same name already exists in memory, restoration is skipped and returns `(nil)`
- After eviction, keys logically still exist and accessing them automatically triggers restoration
- Works with all Redis data types: strings, lists, sets, hashes, sorted sets, etc.

## SPILL.CLEANUP

```
SPILL.CLEANUP
```

Manually scans the entire RocksDB database and removes all expired keys.

> This is a blocking operation that runs on the main DiceDB thread and may
> stall other commands while executing. Use with caution in production environments.

This is a manual cleanup operation that performs a full database scan. The module:

1. Cleans expired keys during restore attempts (returning `ERR Key has expired`)
2. Runs periodic background cleanup every `cleanup-interval` seconds (default: 300)

Use this command only when you need to immediately reclaim disk space from expired keys
or when the automatic cleanup interval is not sufficient.

Return value:

Array with cleanup statistics:

- `num_keys_scanned` - Number of keys scanned in RocksDB
- `num_keys_cleaned` - Number of expired keys deleted

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
1) "num_keys_scanned"
2) (integer) 1523
3) "num_keys_cleaned"
4) (integer) 47

> INFO spill
...
spill_total_keys_cleaned:47
...
```

Notes:

- This is a blocking operation - it runs on the main thread and may impact performance
- Scans the entire RocksDB database sequentially
- Performance depends on the number of keys in RocksDB
- Removed keys increment the `total_keys_cleaned` counter in INFO output
- In most cases, rely on the automatic periodic cleanup instead of using this command
- Returns an array (displayed as alternating key-value pairs in CLI)
