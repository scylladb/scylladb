# CDC Overview

 is a feature that allows you to not only query the current state of a database’s table, but also query the history of all changes made to the table.

As an example, suppose you made a sequence of changes to some table in the given order:

```cql
UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 0;
UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 0;
UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 1;
UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 1;
UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 1;
```

Normally, querying the table would return

```cql
 pk | ck | v
----+----+---
  0 |  0 | 2
  0 |  1 | 0

(2 rows)
```

but with CDC, you can also learn the history of all changes:

```none
change at 2020-01-29 14:37:32: UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
change at 2020-01-29 14:37:33: UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 0;
change at 2020-01-29 14:37:35: UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 0; <- latest change
change at 2020-01-29 14:37:38: UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 1;
change at 2020-01-29 14:37:39: UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 1;
change at 2020-01-29 14:37:40: UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 1; <- latest change
```

(not an actual syntax, the above example just presents the general concept).

## Use cases for CDC

Some examples where CDC may be beneficial:

* Heterogeneous database replication: applying captured changes to another database or table. The other database may use a different schema (or no schema at all), better suited for some specific workloads. An example is replication to ElasticSearch for efficient text searches.
* Implementing a notification system.
* In-flight analytics: looking for patterns in the changes in order to derive useful information, e.g. for fraud detection.

In ScyllaDB CDC is optional and enabled on a per-table basis. The history of changes made to a CDC-enabled table is stored in a separate associated table.

## Terminology

* **Base Table** - this is the original table, where all changes are made.
* **Log Table** - this is the table associated to the base table which is created when CDC is enabled. Read about it in the [log table document](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/cdc/cdc-log-table.md).

## Enabling CDC

You can enable CDC when creating or altering a table using the `cdc` option, for example:

```none
CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v)) WITH cdc = {'enabled':true};
```

#### NOTE
If you enabled CDC and later decide to disable it, you need to **stop all writes** to the base table before issuing the `ALTER TABLE ... WITH cdc = {'enabled':false};` command.

## CDC Parameters

The following table contains parameters available inside the `cdc` option:

| Parameter name   | Definition                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Default   |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| enabled          | If true, the log table is created and each base table write will get a corresponding log table write: a delta row. The delta row describes “what has changed”.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | false     |
| preimage         | If true, each base write will get a corresponding preimage row in the log table. Preimage rows exist to show the affected row’s state prior to the write. The amount of information can be changed: `true` value of the `'preimage'` parameter configures the preimages to contain only the columns that were changed by the write; `'full'` value to the `'preimage'` configures the preimages to contain the entire row (how it was before the write was made). In the case of collection columns, preimage contains the state of the whole collection before the change (not only the affected cells of the collection). Note that preimages are costly: they require an additional read-before-write. | false     |
| postimage        | If true, each base write will get a corresponding postimage row in the log table. Postimage rows exist to show the affected row’s state after to the write. The postimage row always contains all the columns no matter if they were affected by the change or not. Note that postimages, similarly to preimages, are costly: they require an additional read-before-write. However, if you enable both preimage and postimage, only one read will be required for both of them.                                                                                                                                                                                                                          | false     |
| delta            | If `'full'`, each delta row will contain information about every modified column. If `'keys'`, only the primary key of the change will be recorded in the delta row. You may want to use the second option if you’re only interested in preimages and want to save some space.                                                                                                                                                                                                                                                                                                                                                                                                                            | full      |
| ttl              | Each log table row has a TTL (time-to-live) set on each of its columns, so that the log doesn’t grow endlessly. This option specifies what the TTL should be in seconds; the default is 86400 seconds (24 hours). You can also set it to 0, which means that the TTL won’t be set, thus log rows won’t be removed. Be careful however: in that case the log will consume more and more disk space. You will probably want to setup a separate cleaning mechanism if you set TTL to 0.                                                                                                                                                                                                                     | 86400     |

## Using CDC with Applications

When writing applications, you can now use our language specific libraries to simplify writing applications which will read from ScyllaDB CDC.
The following libraries are available:

* [Go](https://github.com/scylladb/scylla-cdc-go)
* [Java](https://github.com/scylladb/scylla-cdc-java)
* [Rust](https://github.com/scylladb/scylla-cdc-rust)

## More information

[ScyllaDB University: Change Data Capture (CDC) lesson](https://university.scylladb.com/courses/data-modeling/lessons/change-data-capture-cdc/) -  Learn how to use CDC. Some of the topics covered are:

* An overview of Change Data Capture,  what exactly is it, what are some common use cases, what does it do, and an overview of how it works
* How can that data be consumed? Different options for consuming the data changes including normal CQL, a layered approach, and integrators
* How does CDC work under the hood? Covers an example of what happens in the DB on different operations to allow CDC
* A summary of  CDC: It’s easy to integrate and consume, it uses plain CQL tables, it’s robust, it’s replicated in the same way as normal data, it has a reasonable overhead, it does not overflow if the consumer fails to act and data is TTL’ed. The summary also includes a comparison with Cassandra, DynamoDB, and MongoDB.
