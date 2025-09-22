## Module Architecture

The infcache module is a Valkey/Redis module that provides infinite cache capabilities by persisting evicted keys to RocksDB. The module integrates with Valkey's keyspace event notification system to automatically capture keys before eviction and restore them on demand.

## Data Storage Format

The module stores data in RocksDB using a compact binary format:

```
[8 bytes] Absolute expiration timestamp (milliseconds since epoch, 0 if no expiration)
[4 bytes] Length of DUMP data (uint32_t)
[N bytes] Serialized key-value data from DUMP command
```

Total overhead per key: 12 bytes + serialized data size

## Module Configuration

The module accepts configuration parameters during loading via `ParseModuleArgs`:

```
path                  - RocksDB database directory (default: /tmp/dicedb-l2)
create_if_missing     - Create DB if not exists (default: 1)
error_if_exists       - Fail if DB exists (default: 0)
paranoid_checks       - Enable paranoid checks (default: 0)
compression           - Enable Snappy compression (default: 1)
write_buffer_size     - Write buffer size in bytes (default: 64MB)
max_open_files        - Maximum open file descriptors (default: 1000)
block_size            - SST file block size (default: 4KB)
block_restart_interval- Block restart interval (default: 16)
max_file_size         - Target SST file size (default: 64MB)
```

## Initialization Process

1. Module Loading (`ValkeyModule_OnLoad`):
   - Parses configuration arguments
   - Initializes RocksDB with specified options
   - Sets up block-based table options for performance tuning
   - Subscribes to keyspace events (PREEVICTION and PREMISS)
   - Registers module commands (`infcache.restore` and `infcache.info`)

2. RocksDB Setup (`InitRocksDB`):
   - Creates RocksDB options with configured parameters
   - Sets compression type (Snappy or none)
   - Configures write buffer, file limits, and block settings
   - Opens database at specified path
   - Creates read/write option objects for operations

## Event Handling

### Pre-eviction Event (`PreevictionKeyNotification`)

When a key is about to be evicted from memory:

1. Receives notification with key name
2. Executes `DUMP` command to serialize the key-value data
3. Executes `PTTL` command to get remaining TTL in milliseconds
4. Calculates absolute expiration timestamp:
   ```
   if (pttl > 0):
       absolute_expire_ms = current_time_ms + pttl
   else:
       absolute_expire_ms = 0
   ```
5. Creates combined data structure with expiration, length, and dump data
6. Stores in RocksDB with key name as the database key
7. Logs the operation (key name, data size, expiration)

### Pre-miss Event (`PremissNotification`)

When a key is accessed but not found in memory:

1. Receives notification with missing key name
2. Queries RocksDB for the key
3. If found, extracts expiration timestamp and dump data
4. Checks expiration:
   - If expired: Deletes from RocksDB and returns
   - If valid: Continues with restoration
5. Executes restoration:
   - For keys with TTL: `RESTORE key ttl_seconds dump_data REPLACE ABSTTL`
   - For keys without TTL: `RESTORE key 0 dump_data REPLACE`
6. On successful restore, deletes key from RocksDB
7. Logs the automatic restoration

## Module Commands

### infcache.restore Command (`RestoreCommand`)

Manual restoration of a specific key:

1. Validates arguments (expects exactly one key name)
2. Queries RocksDB for the specified key
3. Extracts and validates data format
4. Calculates remaining TTL from absolute timestamp:
   ```
   ttl_ms = absolute_expire_ms - current_time_ms
   ```
5. If expired, deletes from RocksDB and returns error
6. Executes `RESTORE` with calculated TTL or 0
7. On success, removes key from RocksDB
8. Returns OK or appropriate error message

### infcache.info Command (`InfoCommand`)

Retrieves RocksDB statistics:

1. Queries RocksDB for `rocksdb.stats` property
2. Returns formatted statistics as verbatim string
3. Provides insights into database performance and usage

## Memory Management

The module carefully manages memory allocation:

- Uses `malloc/free` for temporary buffers
- Creates Valkey strings via `ValkeyModule_CreateString`
- Frees all call replies with `ValkeyModule_FreeCallReply`
- Cleans up configuration strings on module unload
- Properly destroys RocksDB objects and options

## Error Handling

The module implements comprehensive error handling:

- Checks all RocksDB operations for errors
- Validates data integrity (length mismatches, corruption)
- Handles expired keys by automatic deletion
- Logs warnings for failed operations
- Returns appropriate error messages to clients

## Cleanup Process

On module unload (`ValkeyModule_OnUnload`):

1. Closes RocksDB database connection
2. Destroys all RocksDB option objects
3. Frees allocated configuration memory
4. Ensures graceful shutdown

## Performance Optimizations

- Uses Snappy compression to reduce disk usage
- Configurable write buffer to batch writes
- Block-based table format for efficient reads
- Automatic cleanup of expired keys
- Single disk write per eviction
- Minimal memory overhead for metadata

## Thread Safety

The module relies on Valkey's single-threaded execution model:
- All operations occur in the main thread
- No explicit locking required
- RocksDB handles its own internal thread safety
