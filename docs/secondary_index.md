# Secondary indexes in Scylla
Secondary indexes can currently be either global (default) or local. Global indexes use the indexed column as its partition key, while local indexes share their partition key with their base table, which ensures local lookup.

The distinction is stored in index target, which is a string kept in index's options map under the key "target".
Example of a global and local indexes on the same table and column:

SELECT * FROM system\_schema.indexes;
 keyspace\_name | table\_name | index\_name | kind       | options
----------------+-------------+-------------+------------+----------------------
         demodb |           t |  local_t_v1 | COMPOSITES | {'target': '{"pk":["p"],"ck":["v1"]}'}
         demodb |           t |    t_v1_idx | COMPOSITES | {'target': 'v1'}


## Default naming

By default, index names are generated from table name, column name and "_idx" postfix. If the name is taken (e.g. because somebody already created a named index with the exact same name),
"_X" is appended, where X is the smallest number that ensures name uniqueness.

Default name for an index created on table t and column v1 is thus t\_v1\_idx, but it can also become t\_v1\_idx\_1 if the first one was already taken.
Both global and local indexes share the same default naming conventions.

When in doubt, `DESCRIBE index_name` or `SELECT * FROM system_schema.indexes` commands can be leveraged to see more details on index targets and type.

## Global index

Global index's target is usually just the indexed column name, unless the index has a specific type. All supported types are:
 - regular index: v
 - full collection index: FULL(v)
 - index on map keys: KEYS(v)
 - index on map values: ENTRIES(v)

Their serialization is just string representation, so:
"v", "FULL(v)", "KEYS(v)", "ENTRIES(v)" are all valid targets.

## Local index

Local index's target consists of explicit partition key followed by indexed column definition. Currently the partition key must match the partition key of base table.

Their serialization is a string representing primary key in JSON. Examples:
{
  "pk": ["p1", "p2", "p3"],
  "ck": ["v"]
}

{
  "pk": ["p"],
  "ck": ["v"]
}

