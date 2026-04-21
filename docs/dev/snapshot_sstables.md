# system\_distributed.snapshot\_sstables

## Purpose

This table is used during tablet-aware restore to exchange per-SSTable metadata between
the coordinator and worker nodes. When the restore process starts, the coordinator node
populates this table with information about each SSTable extracted from the snapshot
manifests. Worker nodes then read from this table to determine which SSTables need to
be downloaded from object storage and restored locally.

Entries are also created when generating cluster wide snapshots to provide metadata
on snapshot contents.

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
    node uuid,
    tablet bigint,
    state int,
    repaired_at bigint
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
| `node` | uuid | Owning node if sstable is the result of a snapshot and in local state |
| `tablet` | bigint | Tablet the sstable belonged to when snapshot |
| `state` | int | State of the sstable, whether it is local only, both or remote |
| `repaired_at` | bigint | When sstable was repaired. Only valid for snapshot sstables |


## APIs

The following C++ APIs are provided in `db::system_distributed_keyspace`:

- insert\_snapshot\_sstable

- get\_snapshot\_sstables

---

# system\_distributed.snapshot\_remote\_locations

## Purpose

This table stores per-datacenter object storage coordinates for snapshots. When the
restore\_tablets API is called, it populates this table with the endpoint, bucket and
prefix for the given snapshot and datacenter. Worker nodes look up the object storage
location from this table using the snapshot name carried in the tablet transition.

## Schema

~~~
CREATE TABLE system_distributed.snapshot_remote_locations (
    snapshot_name text,
    datacenter text,
    endpoint text,
    bucket text,
    prefix text,
    state int,
    PRIMARY KEY (snapshot_name, datacenter)
)
~~~

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_name` | text (partition key) | Name of the snapshot |
| `datacenter` | text (clustering key) | Datacenter where the snapshot is located |
| `endpoint` | text | Object storage endpoint URL |
| `bucket` | text | Object storage bucket name |
| `prefix` | text | Prefix path within the bucket |
| `state` | int | Snapshot state (0=unknown, 1=local, 2=being\_backed\_up, 3=remote\_and\_local, 4=remote) |

## APIs

The following C++ APIs are provided in `db::system_distributed_keyspace`:

- insert\_snapshot\_remote\_location

- get\_snapshot\_remote\_location
