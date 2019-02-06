# System keyspace layout

This section describes layouts and usage of system.* tables.

## system.large\_partitions

Large partition table can be used to trace largest partitions in a cluster.

Schema:
~~~
CREATE TABLE system.large_partitions (
    keyspace_name text,
    table_name text,
    sstable_name text,
    partition_size bigint,
    partition_key text,
    compaction_time timestamp,
    PRIMARY KEY ((keyspace_name, table_name), sstable_name, partition_size, partition_key)
) WITH CLUSTERING ORDER BY (sstable_name ASC, partition_size DESC, partition_key ASC);
~~~

### Example usage

#### Extracting large partitions info
~~~
SELECT * FROM system.large_partitions;
~~~

#### Extracting large partitions info for a single table
~~~
SELECT * FROM system.large_partitions WHERE keyspace_name = 'ks1' and table_name = 'standard1';
~~~

## system.truncated

Holds truncation replay positions per table and shard

Schema:
~~~
CREATE TABLE system.truncated (
    table_uuid uuid,    # id of truncated table
    shard int,          # shard
    position int,       # replay position
    segment_id bigint,  # replay segment
    truncated_at timestamp static,  # truncation time
    PRIMARY KEY (table_uuid, shard)
) WITH CLUSTERING ORDER BY (shard ASC)
~~~

When a table is truncated, sstables are removed and the current replay position for each 
shard (last mutation to be committed to either sstable or memtable) is collected.
These are then inserted into the above table, using shard as clustering.

When doing commitlog replay (in case of a crash), the data is read from the above 
table and mutations are filtered based on the replay positions to ensure 
truncated data is not resurrected.
 
Note that until the above table was added, truncation records where kept in the 
`truncated_at` map column in the `system.local` table. When booting up, scylla will
merge the data in the legacy store with data the `truncated` table. Until the whole
cluster agrees on the feature `TRUNCATION_TABLE` truncation will write both new and 
legacy records. When the feature is agreed upon the legacy map is removed.

## TODO: the rest
