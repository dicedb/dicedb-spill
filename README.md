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
    compression 1 \
    write_buffer_size 67108864
```

Note: The `compression` module parameter only works if RocksDB was built with compression support (`ENABLE_COMPRESSION=1`).

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

Display RocksDB statistics and metrics:

```
infcache.info
```

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
| `path` | /tmp/dicedb-l2 | RocksDB database directory |
| `compression` | 1 | Enable Snappy compression (requires ENABLE_COMPRESSION=1 build) |
| `write_buffer_size` | 64MB | Write buffer size |
| `max_open_files` | 1000 | Maximum open file descriptors |
| `block_size` | 4KB | SST file block size |

## Architecture

The module integrates with DiceDB's keyspace notifications:

- Pre-eviction: Captures keys before removal from memory
- Pre-miss: Automatically restores keys on cache misses
- RocksDB: Provides persistent, high-performance storage
- TTL Management: Preserves expiration with absolute timestamps
