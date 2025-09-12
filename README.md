# DiceDB - Infinite Cache with RocksDB

This Valkey/DiceDB module automatically backs up evicted keys to RocksDB before they are removed from memory, providing an infinite cache capability with full TTL support.

## Features

- **Automatic Backup**: Captures keys using DUMP command before eviction
- **TTL Preservation**: Stores and restores key expiration times accurately
- **RocksDB Storage**: Persistent storage with configurable compression and performance settings
- **Compact Format**: Efficient storage format combining serialized data with expiration metadata
- **Auto-expiration**: Keys that have expired are automatically removed during restore attempts

## Prerequisites

Install RocksDB development libraries:

```bash
# Ubuntu/Debian
sudo apt-get install librocksdb-dev

# Or build from source using the provided script
./install_rocksdb.sh
```

## Building the Module

1. Make sure you have gcc and RocksDB installed
2. Update the path in Makefile if needed
3. Run the build command:
```bash
make
```

This will create `dicedb-infcache.so` shared library file.

## Loading the Module

Start DiceDB server with the module:

```bash
# Basic loading with default settings
dicedb-server --loadmodule /path/to/dicedb-infcache.so

# With custom configuration
dicedb-server --loadmodule /path/to/dicedb-infcache.so \
    path ./dicedb-l2 \
    compression 1 \
    write_buffer_size 67108864
```

Or add to your configuration file (see `valkey.conf.example`).

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | string | ./dicedb-l2 | RocksDB database directory |
| `create_if_missing` | 0\|1 | 1 | Create database if it doesn't exist |
| `compression` | 0\|1 | 1 | Enable Snappy compression |
| `write_buffer_size` | bytes | 64MB | Size of write buffer |
| `max_open_files` | number | 1000 | Maximum number of open files |
| `block_size` | bytes | 4KB | Block size for SST files |

## Commands

### infcache.restore

Restore an evicted key from RocksDB back to memory with its original TTL.

```
infcache.restore <key>
```

Returns:
- `OK` if key was successfully restored with TTL
- `(nil)` if key not found in RocksDB
- `ERR Key has expired` if the key's TTL has passed
- Error message if restoration failed

### infcache.info

Display RocksDB statistics and performance metrics.

```
infcache.info
```

## Data Storage Format

The module stores data in a compact binary format:
- **8 bytes**: Absolute expiration timestamp in milliseconds (0 if no expiration)
- **4 bytes**: Length of DUMP data
- **N bytes**: Serialized key-value data from DUMP command

## How It Works

1. **Pre-eviction Hook**: When a key is about to be evicted, the module intercepts the event
2. **Capture Data**: Executes `DUMP` to serialize the key and `PTTL` to get remaining TTL
3. **Calculate Expiration**: Converts relative TTL to absolute timestamp
4. **Store in RocksDB**: Saves combined data (expiration + dump) to RocksDB
5. **Restoration**: When `infcache.restore` is called:
   - Retrieves data from RocksDB
   - Checks if key has expired
   - Uses `RESTORE` with calculated TTL to recreate the key
   - Removes from RocksDB after successful restore

## Testing

1. Set memory limits to trigger eviction:
   ```
   CONFIG SET maxmemory 10mb
   CONFIG SET maxmemory-policy allkeys-lru
   ```

2. Add keys with TTL:
   ```
   SET key1 "value1" EX 3600
   SET key2 "value2" EX 7200
   ```

3. Fill memory to trigger eviction:
   ```
   # Add more data until eviction occurs
   ```

4. Restore evicted keys:
   ```
   infcache.restore key1
   TTL key1  # Should show remaining TTL
   ```

## Performance Considerations

- **Eviction Overhead**: Each eviction requires DUMP + PTTL + RocksDB write
- **Storage Efficiency**: Compression reduces disk usage, TTL adds only 12 bytes overhead
- **Expired Keys**: Automatically cleaned up on restore attempt
- **Memory vs Disk**: Trade memory for unlimited disk-based storage

## Testing

Run unit tests:
```bash
make test-unit
```

Run integration tests (requires DiceDB server running on port 6379 with module loaded):
```bash
# First start DiceDB with the module
dicedb-server --port 6379 --loadmodule ./dicedb-infcache.so

# Then run tests (automatically sets up venv with valkey client)
make test-integration
```

Run all tests:
```bash
make test-all
# or
./run_tests.sh
```

## Limitations

- Only captures keys evicted due to memory pressure (not DEL commands)
- DUMP/RESTORE may not support all Redis module data types
- Requires clock synchronization for accurate TTL preservation across restarts