# system\_distributed.snapshot\_sstables

## Purpose

This table is used during tablet-aware restore to exchange per-SSTable metadata between
the coordinator and worker nodes. When a backup snapshot is taken, the coordinator node
populates this table with information about each SSTable extracted from the snapshot
manifests. Worker nodes then read from this table to determine which SSTables need to
be downloaded from object storage and restored locally.

Rows are inserted with a TTL so that stale restore metadata is automatically cleaned up.

## Schema

~~~
CREATE TABLE system_distributed.snapshot_sstables (
    snapshot_name text,
    "keyspace" text,
    "table" text,
    datacenter text,
    rack text,
    id uuid,
    first_token bigint,
    last_token bigint,
    toc_name text,
    prefix text,
    PRIMARY KEY ((snapshot_name, "keyspace", "table", datacenter, rack), id, first_token)
)
~~~

Column descriptions:

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_name` | text (partition key) | Name of the snapshot |
| `keyspace` | text (partition key) | Keyspace the snapshot was taken from |
| `table` | text (partition key) | Table within the keyspace |
| `datacenter` | text (partition key) | Datacenter where the SSTable is located |
| `rack` | text (partition key) | Rack where the SSTable is located |
| `id` | uuid (clustering key) | Unique identifier for the SSTable |
| `first_token` | bigint (clustering key) | First token in the token range covered by this SSTable |
| `last_token` | bigint | Last token in the token range covered by this SSTable |
| `toc_name` | text | TOC filename of the SSTable (e.g. `me-1-big-TOC.txt`) |
| `prefix` | text | Prefix path in object storage where the SSTable was backed up |

## APIs

The following C++ APIs are provided in `db::system_distributed_keyspace`:

### insert\_snapshot\_sstable

```cpp
future<> insert_snapshot_sstable(
    sstring snapshot_name, sstring ks, sstring table,
    sstring dc, sstring rack, utils::UUID id,
    dht::token first_token, dht::token last_token,
    sstring toc_name, sstring prefix, uint64_t ttl_seconds);
```

Inserts a single SSTable entry for a given snapshot, keyspace, table, datacenter,
and rack. The row is written with the specified TTL (in seconds). Uses consistency
level `EACH_QUORUM`.

### get\_snapshot\_sstables

```cpp
future<utils::chunked_vector<sstable_info>> get_snapshot_sstables(
    sstring snapshot_name, sstring ks, sstring table,
    sstring dc, sstring rack) const;
```

Retrieves all SSTable entries for a given snapshot, keyspace, table, datacenter, and rack.
Returns a vector of `sstable_info` structs containing `id`, `first_token`, `last_token`,
`toc_name`, and `prefix`. Uses consistency level `LOCAL_QUORUM`.
