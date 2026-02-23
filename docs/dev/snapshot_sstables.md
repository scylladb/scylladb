# system\_distributed.snapshot\_sstables

## Purpose

This table is used during tablet-aware restore to exchange per-SSTable metadata between
the coordinator and worker nodes. When the restore process starts, the coordinator node
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
    first_token bigint,
    sstable_id uuid,
    last_token bigint,
    toc_name text,
    prefix text,
    PRIMARY KEY ((snapshot_name, "keyspace", "table", datacenter, rack), first_token, sstable_id)
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
| `first_token` | bigint (clustering key) | First token in the token range covered by this SSTable |
| `sstable_id` | uuid (clustering key) | Unique identifier for the SSTable |
| `last_token` | bigint | Last token in the token range covered by this SSTable |
| `toc_name` | text | TOC filename of the SSTable (e.g. `me-3gdq_0bki_2cvk01yl83nj0tp5gh-big-TOC.txt`) |
| `prefix` | text | Prefix path in object storage where the SSTable was backed up |

## APIs

The following C++ APIs are provided in `db::system_distributed_keyspace`:

- insert\_snapshot\_sstable

- get\_snapshot\_sstables
