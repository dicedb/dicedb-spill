# Configurations

## Module Parameters

| Parameter | Required | Default | Purpose |
|-----------|----------|---------|---------|
| `path` | Yes | - | RocksDB data directory path |
| `max-memory` | No | 256MB | Maximum memory budget for RocksDB. Minimum: 20MB |
| `cleanup-interval` | No | 300 seconds | Interval between automatic cleanup runs for expired keys. Set to 0 to disable periodic cleanup |

## Parameter Details

### `path`

Required: Yes

Base directory for RocksDB storage. The module creates one subdirectory per database (`db0`, `db1`, …) under this path. The number of subdirectories matches the server's `databases` config value (read automatically at load time).

Example:
```bash
--loadmodule ./modules/dicedb-spill/lib-spill.so path /tmp/data/spill
# Creates /tmp/data/spill/db0, /tmp/data/spill/db1, … /tmp/data/spill/dbN
```

### `max-memory`

- Required: No
- Default: 268435456 (256MB)
- Minimum: 20971520 (20MB)

Total memory budget across all RocksDB instances. The budget is divided evenly per database (shared block cache + per-database write buffers). This controls the RAM footprint of the persistence layer.

Example:
```bash
--loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data/spill \
    max-memory 536870912  # 512MB
```

### `cleanup-interval`

- Required: No
- Default: 300 (5 minutes)
- Range: 0 or positive integer

Interval in seconds between automatic background cleanup runs that remove expired keys from RocksDB. Set to 0 to disable periodic cleanup (manual cleanup via `SPILL.CLEANUP` command will still work).

Example:

```bash
--loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data/spill \
    cleanup-interval 600  # 10 minutes
```

## Complete Example

```bash
dicedb-server -p 6379 \
    --loadmodule ./modules/dicedb-spill/lib-spill.so \
    path /tmp/data/spill \
    max-memory 268435456 \
    cleanup-interval 300
```
