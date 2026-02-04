# DiceDB Infinite Cache Module

An infinite cache module for DiceDB that automatically persists evicted keys to RocksDB storage, providing disk bound cache capacity with transparent key restoration and full TTL preservation.

## Features

- Automatic persistence of evicted keys to RocksDB
- Transparent key restoration on cache misses
- Full TTL preservation across evictions and restores
- Configurable RocksDB storage with compression
- Minimal overhead with 12-byte metadata per key
- Automatic cleanup of expired keys

## Quick Start

### Cloning

Please note, this repository should be cloned as a submodule
and hence must be present at `modules/dicedb-infcache` in the
dicedb repository.

### Prerequisites

Install RocksDB (requires root/sudo):

```bash
# Basic build (portable, no compression)
$ sudo ./scripts/install_rocksdb.sh

# With compression support
$ sudo ENABLE_COMPRESSION=1 ./scripts/install_rocksdb.sh

# With native CPU optimizations (SIMD, AVX - better performance, less portable)
$ sudo PORTABLE=0 ./scripts/install_rocksdb.sh

# Full optimization (compression + native CPU optimizations)
$ sudo ENABLE_COMPRESSION=1 PORTABLE=0 ./scripts/install_rocksdb.sh
```

**Build options:**
| Option | Default | Description |
|--------|---------|-------------|
| `ENABLE_COMPRESSION` | 0 | Enable compression libraries (snappy, zlib, bz2, lz4, zstd) |
| `PORTABLE` | 1 | Cross-CPU compatible build. Set to 0 for native CPU optimizations |

### Build Module

```bash
# Without compression
$ make

# With compression support (must match RocksDB build)
$ make ENABLE_COMPRESSION=1
```

### Basic Configuration

```bash
$ dicedb-server -p 6379 \
    --loadmodule ./modules/dicedb-infcache/lib-infcache.so \
    path /tmp/dicedb-l2 \
    max-memory 268435456
```

Note: The module uses Snappy compression automatically. RocksDB should be built with compression support (`ENABLE_COMPRESSION=1`) for optimal disk usage.

## Documentation

- [Commands Reference](docs/commands.md) - Complete command documentation
- [Internals](docs/internals.md) - Architecture and implementation details
- [Configuration](docs/configuration.md) - Module configuration options

## Commands

### infcache.restore

Manually restore an evicted key from RocksDB:

```
infcache.restore <key>
```

### infcache.info

Display comprehensive statistics and metrics in Redis INFO format:

```
infcache.info
```

Output includes four sections:

**# Infcache** - Module-specific metrics
- `keys_stored`, `keys_restored`, `keys_expired`, `keys_cleaned` - Operation counters
- `bytes_written`, `bytes_read` - I/O statistics
- `path` - RocksDB database path
- `max_memory` - Configured memory budget

**# RocksDB_Memory** - Memory usage breakdown
- `block_cache_usage` - Current block cache memory usage
- `block_cache_pinned_usage` - Pinned block cache memory
- `memtable_size` - Total memtable memory usage
- `table_readers_mem` - Memory used by SSTable readers

**# RocksDB_Storage** - Storage statistics
- `estimated_keys` - Approximate number of keys
- `live_data_size` - Estimated live data size
- `total_sst_files_size` - Total SST file size on disk
- `num_snapshots` - Number of active snapshots

**# RocksDB_Compaction** - Compaction status
- `num_immutable_memtables` - Immutable memtables pending flush
- `memtable_flush_pending` - Whether memtable flush is pending
- `compaction_pending` - Whether compaction is pending
- `background_errors` - Number of background errors
- `base_level` - Base level for compaction
- `num_files_L0` through `num_files_L6` - Files at each level

## Tests

```bash
$ make test
```

## Performance

- O(1) persistence on eviction with batched writes
- O(1) key restoration from RocksDB
- Snappy compression reduces disk usage by ~50% (when enabled)
- Native CPU optimizations (SIMD/AVX) available with `PORTABLE=0`
- Write buffer batching minimizes disk I/O
- Automatic expiration prevents storage bloat

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `path` | (required) | RocksDB database directory |
| `max-memory` | 256MB | Maximum memory budget for RocksDB (block cache + write buffers) |

### Memory Distribution

The `max-memory` parameter controls total RocksDB memory usage:
- **50%** allocated to block cache (read performance)
- **~50%** allocated to write buffers (3 buffers @ ~17% each)
- Remaining memory used for indexes, bloom filters, and metadata

### Default RocksDB Settings

The module uses carefully tuned defaults optimized for evicted key storage:

| Setting | Value | Rationale |
|---------|-------|-----------|
| **Compression** | Snappy | Reduces disk usage 2-4x for evicted data |
| **Write buffers** | 3 memtables | Smooths write flow during eviction bursts |
| **Max open files** | 1000 | Prevents exhausting system file descriptors |
| **Point lookup optimization** | Enabled | Critical for fast single-key restoration |
| **Memory-mapped I/O** | Enabled | Reduces syscall overhead for reads/writes |
| **Background compactions** | 2 threads | Avoids CPU contention with Redis/Valkey |
| **Dynamic level sizing** | Enabled | Reduces space amplification |
| **SST file size** | 64MB | Balances compaction overhead and file count |
| **Block size** | 4KB | Matches typical SSD page size |
| **Bloom filter** | 10 bits/key | ~1% false positive rate for key existence |
| **Cache index/filters** | Enabled | Keeps metadata in memory for faster lookups |
| **WAL** | Enabled | Provides crash recovery for evicted data |
| **Fsync** | Async | Improves eviction throughput (data in OS cache) |

## Architecture

The module integrates with DiceDB's keyspace notifications:

- Pre-eviction: Captures keys before removal from memory
- Pre-miss: Automatically restores keys on cache misses
- RocksDB: Provides persistent, high-performance storage
- TTL Management: Preserves expiration with absolute timestamps
