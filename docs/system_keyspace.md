# System keyspace layout

This section describes layouts and usage of system.* tables.

## system.large\_partitions

Large partition table can be used to trace largest partitions in a cluster.

Schema:
~~~
CREATE TABLE large_partitions (
   keyspace_name text,
   table_name tex,
   sstable_name text,
   partition_size bigint,
   key text,
   compaction_time timestamp,
   PRIMARY KEY((keyspace_name, table_name), sstable_name, partition_size, key)
) WITH CLUSTERING ORDER BY (partition_size DESC);
~~~

### Example usage

#### Extracting large partitions info
~~~
SELECT * FROM system.large_partitions;
~~~

#### Extracting large partitions info for a single table
~~~
SELECT * FROM system.large_partitions WHERE sstable_name = 'ks1.standard1';
~~~

## TODO: the rest
