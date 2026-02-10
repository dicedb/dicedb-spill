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

The quickest and easiest way to see dicedb-spill in action is by using the official Docker image of DiceDB which comes with everything pre-configured.

```bash
docker run \
    --name dicedb-001 \
    -p 6379:6379 -v $(pwd)/data:/data/ \
    dicedb/dicedb
```

This command starts a DiceDB container with the `spill` module already enabled. By default, the spill module uses RocksDB and is configured with a maximum memory limit of 250MB.

### Custom Configuration

```bash
docker run \
    --name dicedb-001 \
    -p 6379:6379 -v $(pwd)/data:/data/ \
    dicedb/dicedb \
    dicedb-server \
      --port 6379 \
      --maxmemory 500mb \
      --protected-mode no \
      --loadmodule /usr/local/lib/lib-spill.so path /data/spill/ max-memory 262144000
```

## Spill in Action

Once the DiceDB server is running with spill module loaded,
run the following commands which - sets a key, evicts a key,
and when we try to access it (via `GET`) it trasparently
loads it in memory.

```bash
docker exec -it dicedb-001 dicedb-cli
```

We are explicitly eviciting the key using `EVICT` command, but in a production setup
this might happen due to memory pressure

```
SET k1 v1
EVICT k1
KEYS *
GET k1
```

## Documentation

- [Configuration Reference](docs/configuration.md)
- [Commands Reference](docs/commands.md)

## Development

### Cloning

This repository is intended to be used as a submodule and must be located at `modules/dicedb-spill` inside the DiceDB repository and please clone [dicedb/dicedb](https://github.com/dicedb/dicedb) repository with
a `--recursive` flag and then init the git submodule.

### Install RocksDB

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

## Tests

Start the DiceDB server with the Spill module loaded:

```bash
./src/dicedb-server \
    --loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data max-memory 256 cleanup-interval 300
```

Run the test suite:

```bash
make test
```
