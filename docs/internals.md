## Data Storage Format

The module stores data in a compact binary format:
- 8 bytes: Absolute expiration timestamp in milliseconds (0 if no expiration)
- 4 bytes: Length of DUMP data
- N bytes: Serialized key-value data from DUMP command

## How It Works

1. Pre-eviction Hook: When a key is about to be evicted, the module intercepts the event
2. Capture Data: Executes `DUMP` to serialize the key and `PTTL` to get remaining TTL
3. Calculate Expiration: Converts relative TTL to absolute timestamp
4. Store in RocksDB: Saves combined data (expiration + dump) to RocksDB
5. Restoration: When `infcache.restore` is called:
   - Retrieves data from RocksDB
   - Checks if key has expired
   - Uses `RESTORE` with calculated TTL to recreate the key
   - Removes from RocksDB after successful restore
