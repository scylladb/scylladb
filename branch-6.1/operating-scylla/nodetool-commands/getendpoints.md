# Nodetool getendpoints

**getendpoints** - `<keyspace>` `<table>` `<key>`- Print the end-points IP or name of the nodes that own the partition key.

Use this command to know which node holds the specific partition key.

For example:

`nodetool getendpoints nba player_name Russell`

| Parameter   | Description   |
|-------------|---------------|
| keyspace    | keyspace name |
| table       | table name    |
| key         | partition key |

In a case of a composite partition key, use `:` between columns in the key. All columns in the partition key are required.
For example:

```default
cqlsh:mykeyspace> CREATE TABLE superheroes (firstname text, lastname text, age int, PRIMARY KEY ((firstname, lastname)));
```

```default
nodetool getendpoints mykeyspace superheroes "peter:parker"
```

ScyllaDB does not support *getendpoints* for a partition key with a frozen UDT.

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md)
