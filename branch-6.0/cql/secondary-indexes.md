<!-- Licensed to the Apache Software Foundation (ASF) under one -->
<!-- or more contributor license agreements.  See the NOTICE file -->
<!-- distributed with this work for additional information -->
<!-- regarding copyright ownership.  The ASF licenses this file -->
<!-- to you under the Apache License, Version 2.0 (the -->
<!-- "License"); you may not use this file except in compliance -->
<!-- with the License.  You may obtain a copy of the License at -->
<!-- http://www.apache.org/licenses/LICENSE-2.0 -->
<!-- Unless required by applicable law or agreed to in writing, software -->
<!-- distributed under the License is distributed on an "AS IS" BASIS, -->
<!-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. -->
<!-- See the License for the specific language governing permissions and -->
<!-- limitations under the License. -->

<a id="secondary-indexes"></a>

# Global Secondary Indexes

CQL supports creating secondary indexes on tables, allowing queries on the table to use those indexes. A secondary index
is identified by a name defined by:

```cql
index_name: re('[a-zA-Z_0-9]+')
```

<a id="create-index-statement"></a>

## CREATE INDEX

Creating a secondary index on a table uses the `CREATE INDEX` statement:

```cql
create_index_statement: CREATE INDEX [ `index_name` ]
                      :     ON `table_name` '(' `index_identifier` ')'
                      :     [ USING `string` [ WITH OPTIONS = `map_literal` ] ]
index_identifier: `column_name`
                :| ( FULL ) '(' `column_name` ')'
```

For instance:

```cql
CREATE INDEX userIndex ON NerdMovies (user);
CREATE INDEX ON Mutants (abilityId);
```

The `CREATE INDEX` statement is used to create a new (automatic) secondary index for a given (existing) column in a
given table. A name for the index itself can be specified before the `ON` keyword, if desired. If data already exists
for the column, it will be indexed asynchronously. After the index is created, new data for the column is indexed
automatically at insertion time.

## Local Secondary Index

[Local Secondary Indexes](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/local-secondary-indexes.md) is an enhancement of [Global Secondary Indexes](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/secondary-indexes.md), which allows Scylla to optimize the use case in which the partition key of the base table is also the partition key of the index. Local Secondary Index syntax is the same as above, with extra parentheses on the partition key.

```cql
index_identifier: `column_name`
                :| ( PK ) | KEYS | VALUES | FULL ) '(' `column_name` ')'
```

Example:

```cql
CREATE TABLE menus (location text, name text, price float, dish_type text, PRIMARY KEY(location, name));
CREATE INDEX ON menus((location),dish_type);
```

More on [Local Secondary Indexes](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/local-secondary-indexes.md)

<!-- Attempting to create an already existing index will return an error unless the ``IF NOT EXISTS`` option is used. If it -->
<!-- is used, the statement will be a no-op if the index already exists. -->
<!-- Indexes on Map Keys (supported in Scylla 2.2) -->
<!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
<!-- When creating an index on a :ref:`maps <maps>`, you may index either the keys or the values. If the column identifier is -->
<!-- placed within the ``keys()`` function, the index will be on the map keys, allowing you to use ``CONTAINS KEY`` in -->
<!-- ``WHERE`` clauses. Otherwise, the index will be on the map values. -->

<a id="drop-index-statement"></a>

## DROP INDEX

Dropping a secondary index uses the `DROP INDEX` statement:

```cql
drop_index_statement: DROP INDEX [ IF EXISTS ] `index_name`
```

The `DROP INDEX` statement is used to drop an existing secondary index. The argument of the statement is the index
name, which may optionally specify the keyspace of the index.

<!-- If the index does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case the -->
<!-- operation is a no-op. -->

## Additional Information

* [Global Secondary Indexes](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/secondary-indexes.md)
* [Local Secondary Indexes](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/local-secondary-indexes.md)

The following courses are available from Scylla University:

* [Materialized Views and Secondary Indexes](https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/)
* [Global Secondary Indexes](https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/global-secondary-indexes/)
* [Local Secondary Indexes](https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/local-secondary-indexes-and-combining-both-types-of-indexes/)

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
