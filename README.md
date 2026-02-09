# DiceDB Spill

Spill module persists evicted keys to RocksDB, expanding cache capacity onto disk while transparently restoring keys and fully preserving TTLs.

## Features

- Automatically persists evicted keys to RocksDB
- Transparently restores keys on cache misses
- Fully preserves TTLs across evictions and restores
- Supports configurable RocksDB storage with optional compression
- Adds minimal overhead with only 12 bytes of metadata per key
- Automatically cleans up expired keys

## Quick Start

### Cloning

This repository is intended to be used as a submodule and must be located at `modules/dicedb-spill` inside the DiceDB repository.

### Prerequisites

Install RocksDB (requires root or sudo access):

```bash
# Basic build (portable, no compression)
$ sudo ./scripts/install_rocksdb.sh

# Build with compression support
$ sudo ENABLE_COMPRESSION=1 ./scripts/install_rocksdb.sh

# Build with native CPU optimizations (SIMD, AVX - higher performance, less portable)
$ sudo PORTABLE=0 ./scripts/install_rocksdb.sh

# Full optimization (compression + native CPU optimizations)
$ sudo ENABLE_COMPRESSION=1 PORTABLE=0 ./scripts/install_rocksdb.sh
```

- `ENABLE_COMPRESSION` - Enables compression libraries (snappy, zlib, bz2, lz4, zstd)
- `PORTABLE` - Builds a cross-CPU compatible version. Set to `0` to enable native CPU optimizations

### Build Module

```bash
# Build without compression
$ make

# Build with compression support (must match the RocksDB build configuration)
$ make ENABLE_COMPRESSION=1
```

### Basic Configuration

```bash
$ dicedb-server -p 6379 \
    --loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data/spill \
    max-memory 268435456 \
    cleanup-interval 300
```

Note: The module automatically uses Snappy compression. For optimal disk usage, RocksDB should be built with compression enabled (`ENABLE_COMPRESSION=1`).

## Documentation

- [Configuration Reference](docs/configuration.md)
- [Commands Reference](docs/commands.md)

## Tests

Start the DiceDB server with the Spill module loaded:

```bash
./src/dicedb-server \
    --loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data/spill max-memory 256 cleanup-interval 300
```

Run the test suite:

```bash
make test
```
