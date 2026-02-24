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
    "keyspace" text,
    "table" text,
    is_view boolean,
    "schema" text,
    PRIMARY KEY (snapshot_name, "keyspace", "table")
)
~~~

Column descriptions:

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_name` | text (partition key) | Name of the snapshot |
| `keyspace` | text (clustering key) | Keyspace the table belongs to |
| `table` | text (clustering key) | Table name within the keyspace |
| `is_view` | boolean | Whether this table is a materialized view |
| `schema` | text | CQL schema string of the table |

## APIs

The following C++ APIs are provided in `db::system_distributed_keyspace`:

### insert\_snapshot\_cql\_table

```cpp
future<> insert_snapshot_cql_table(
    sstring snapshot_name, sstring ks, sstring table,
    bool is_view, sstring schema,
    db::consistency_level cl = db::consistency_level::EACH_QUORUM);
```

Inserts a single table entry for a given snapshot, keyspace, and table.
The row is written with a 3-day TTL (this will change in the future, see the note in the beginning).

### get\_snapshot\_cql\_table\_schema

```cpp
future<sstring> get_snapshot_cql_table_schema(
    sstring snapshot_name, sstring ks, sstring table,
    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM) const;
```

Retrieves the CQL schema string for a specific table within a snapshot.
The combination of `snapshot_name`, `ks`, and `table` forms the full primary key,
so exactly one row is expected.
