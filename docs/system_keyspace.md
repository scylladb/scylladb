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

## TODO: the rest
