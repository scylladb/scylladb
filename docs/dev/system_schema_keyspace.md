# System schema keyspace layout

This section describes layouts and usage of system\_schema.* tables.

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

