# DiceDB Infinite Cache Module

A high-performance infinite cache module for DiceDB that automatically persists evicted keys to RocksDB storage, providing unlimited cache capacity with transparent key restoration and full TTL preservation.

## Features

- Automatic persistence of evicted keys to RocksDB
- Transparent key restoration on cache misses
- Full TTL preservation across evictions and restores
- Configurable RocksDB storage with compression
- Minimal overhead with 12-byte metadata per key
- Automatic cleanup of expired keys

## Quick Start

### Prerequisites

Install RocksDB development libraries:

```bash
$ bash scripts/install_rocksdb.sh
```

### Build and Load Module

```bash
$ make
```

### Basic Configuration

```bash
# Load with custom settings
$ dicedb-server -p 8379 \
    --loadmodule ./lib-infcache.so \
    path /tmp/dicedb-l2 \
    compression 1 \
    write_buffer_size 67108864
```

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
- Snappy compression reduces disk usage by ~50%
- Write buffer batching minimizes disk I/O
- Automatic expiration prevents storage bloat

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `path` | /tmp/dicedb-l2 | RocksDB database directory |
| `compression` | 1 | Enable Snappy compression |
| `write_buffer_size` | 64MB | Write buffer size |
| `max_open_files` | 1000 | Maximum open file descriptors |
| `block_size` | 4KB | SST file block size |

## Architecture

The module integrates with DiceDB's keyspace notifications:

- Pre-eviction: Captures keys before removal from memory
- Pre-miss: Automatically restores keys on cache misses
- RocksDB: Provides persistent, high-performance storage
- TTL Management: Preserves expiration with absolute timestamps
