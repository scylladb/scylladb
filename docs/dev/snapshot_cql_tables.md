# system\_distributed.snapshot\_cql\_tables

## Purpose

This table is used during tablet-aware restore to store the CQL schema of tables included in a snapshot.
The schema of the table is then altered (table is dropped and recreated with a new schema) so that
the min_tablet_count/max_tablet_count hints are set equal to the tablet count, thus preventing the load
balancer from kicking in. The schema of the restored table is then restored to the original schema stored
in the snapshot_cql_tables table.

Note that rows are inserted with a TTL so that stale restore metadata is automatically cleaned up.
This is a temporary solution which will be replaced at some point with a mechanism for either
cleaning up snapshots once restore is done or by doing it manually.

## Schema

~~~
CREATE TABLE system_distributed.snapshot_cql_tables (
    snapshot_name text,
    keyspace_name text,
    table_name text,
    is_view boolean,
    create_statement text,
    PRIMARY KEY (snapshot_name, keyspace_name, table_name)
)
~~~

Column descriptions:

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_name` | text (partition key) | Name of the snapshot |
| `keyspace_name` | text (clustering key) | Keyspace the table belongs to |
| `table_name` | text (clustering key) | Table name within the keyspace |
| `is_view` | boolean | Whether this table is a materialized view |
| `create_statement` | text | CQL create statement of the table |

## APIs

The following C++ APIs are provided in `db::system_distributed_keyspace`:

- insert\_snapshot\_cql\_table
- get\_snapshot\_cql\_table\_schema
