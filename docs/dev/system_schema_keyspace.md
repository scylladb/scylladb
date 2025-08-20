# System schema keyspace layout

This section describes layouts and usage of system\_schema.* tables.

## system\_schema.keyspaces

This table contains one row per keyspaces.

Schema:

```
CREATE TABLE system_schema.keyspaces (
    keyspace_name text PRIMARY KEY,
    durable_writes boolean,
    replication frozen<map<text, text>>
)
```

Columns:

* `keyspace_name` - name of the keyspace
* `durable_writes` - whether writes to the keyspace are using commitlog.
* `replication` - replication settings for the keyspace. The value for the `"class"` key determines
   replication strategy name. The structure of other options depends
   on the replication strategy.

   For `NetworkTopologyStrategy` the other options specify replication factors for datacenters,
   stored as a flattened map of the extended options map (see below).

   For `SimpleStrategy` there is a single option `"replication_factor"` specifying the replication factor.

Extended options map used by NetworkTopologyStrategy is a map where values can be either strings or lists of strings.

For example:


```
   {
      'dc1': '3',
      'dc2': ['rack1', 'rack2'],
      'dc3': []
   }
```

The options above mean that the replication factor for datacenter `dc1` is 3, for datacenter `dc2` it is 2,
with replicas placed on racks `rack1` and `rack2`. For 'dc3' the replication factor is 0, expressed as an empty list of racks.

The extended map is stored in the "replication" column in a flattened form, where values which are lists
are represented as multiple entries in the map with the list index appended to the key, with `:` as the separator.
The index can be negative, which is used to indicate that the list is empty.

The example extended options map from above has a flattened representation of:

```
  {
    'dc1': '3',
    'dc2:0': 'rack1',
    'dc2:1': 'rack2',
    'dc3:-1': ''
  }
```

## system\_schema.computed\_columns

Computed columns are a special kind of columns. Rather than having their value provided directly
by the user, they are computed - possibly from other column values. Examples of such computed
columns could be:
 * token column generated from the base partition key for secondary indexes
 * map value column, generated as the extraction of a single value from a map stored in a different column

Computed columns in many ways act as regular columns, so they are also present in the `system_schema.columns` table -
`system_schema.computed_columns` is an additional mapping that marks the column as computed and provides its computation.

Schema:
~~~
CREATE TABLE system_schema.computed_columns (
    keyspace_name text,
    table_name text,
    column_name text,
    computation blob,
    PRIMARY KEY (keyspace_name, table_name, column_name)
) WITH CLUSTERING ORDER BY (table_name ASC, column_name ASC);
~~~

`computation` is stored as a blob and its contents are assumed to be a JSON representation of computation's type
and any custom fields needed.

Example representations:
~~~
{'type':'token'}

{'type':'map_value','map':'my_map_column_name','key':'AF$^GESHHgge6yhf'}
~~~

The token computation does not need additional arguments, as it returns the token of base's partition key.
In order to compute a map value, what's additionally needed is the column name that stores the map and the key
at which the value is expected.

