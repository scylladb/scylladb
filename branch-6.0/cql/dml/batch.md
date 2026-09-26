<a id="batch-statement"></a>

# BATCH

Multiple `INSERT`, `UPDATE` and `DELETE` can be executed in a single statement by grouping them through a
`BATCH` statement:

```cql
batch_statement: BEGIN [ UNLOGGED | COUNTER ] BATCH
               : [ USING `update_parameter` ( AND `update_parameter` )* ]
               : `modification_statement` ( ';' `modification_statement` )*
               : APPLY BATCH
modification_statement: `insert_statement` | `update_statement` | `delete_statement`
```

For instance:

```cql
BEGIN BATCH
   INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
   UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
   INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
   DELETE name FROM users WHERE userid = 'user1';
APPLY BATCH;
```

The `BATCH` statement group multiple modification statements (insertions/updates and deletions) into a single
statement. It serves several purposes:

- It saves network round-trips between the client and the server (and sometimes between the server coordinator and the
  replicas) when batching multiple updates.
- All updates in a `BATCH` belonging to a given partition key are performed atomically.
- By default, all operations in the batch are performed as *logged*, to ensure all mutations eventually complete (or
  none will). See the notes on [UNLOGGED batches](#unlogged-batches) for more details.

Note that:

- `BATCH` statements may only contain `UPDATE`, `INSERT` and `DELETE` statements (not other batches, for instance).
- Batches are *not* a full analogue for SQL transactions.
- If a timestamp is not specified for each operation, then all operations will be applied with the same timestamp
  (either one generated automatically, or the timestamp provided at the batch level). Due to Scylla’s conflict
  resolution procedure in the case of timestamp ties, operations may be applied in an order that is different from the order they are listed in the `BATCH` statement. To force a
  particular operation ordering, you must specify per-operation timestamps.
- A LOGGED batch to a single partition will be converted to an UNLOGGED batch as an optimization.

`BATCH` supports the `TIMESTAMP` option with the same semantics as the TIMESTAMP parameter in `UPDATE` statement.
For more information on the `update_parameter` refer to the [UPDATE](https://opensource.docs.scylladb.com/branch-6.0/cql/dml.md#update-parameters) section.

<a id="unlogged-batches"></a>

## `UNLOGGED` batches

By default, Scylla uses a batch log to ensure all operations in a batch eventually complete or none will (note,
however, that operations are only isolated within a single partition).

There is a performance penalty for batch atomicity when a batch spans multiple partitions. If you do not want to incur
this penalty, you can tell Scylla to skip the batchlog with the `UNLOGGED` option. If the `UNLOGGED` option is
used, a failed batch might leave the batch only partly applied.

## `COUNTER` batches

Use the `COUNTER` option for batched counter updates. Unlike other
updates in Scylla, counter updates are not idempotent.

[Apache Cassandra Query Language (CQL) Reference](https://opensource.docs.scylladb.com/branch-6.0/cql/index.md)

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.

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
