# Motivation
Scylla clusters keep various kinds of metadata that needs to be global. The list includes but is not limited to:

* schema,
* authorization/authentication,
* service levels.

In the past, they were replicated using mechanisms like custom synchronization RPC, while auth and service levels—distributed tables. All of them suffered from consistency issues, e.g. concurrent modification could make them break, some were prone to data loss if a node was lost. To solve those issues, we replaced the mechanisms with the Raft algorithm and now they're managed by group0.

Since we want to support cluster-wide backup and restore[^1], we need to be able to backup and restore cluster metadata. The currently recommended way of backing-up tables with user data is to take a snapshot, upload the SSTables elswehere and, during restore, download them back to appropriate nodes in the fresh cluster.

Unfortunately, that doesn't work well with Raft-replicated tables for the following reasons:

* group0-managed tables are just local system tables and any modifications to them are propagated to all nodes using Raft. That assumes that if there are no unapplied Raft commands, all nodes will have the same state of the table.

  Theoretically, we could make it work if we restored the same SSTables on all nodes, but although it sounds simple, it's not always possible: Scylla Manager cannot transfer SSTables backed-up in one data center to another one. Taking a snapshot on more than one node is not atomic and does not guarantee that the contents of their snapshots will be consistent. In case of any differences between the nodes, there is no repair algorithm that could reconcile their states. The fact that application of group0 commands to tables is not synchronized with the flush on snapshot doesn't help either.

* Restoring data by putting SSTables into the data directory is potentially unsafe. Nothing prevents the administrator from restoring only some of the backed-up SSTables. Also, group0 generally assumes that all its data was created by relevant code in Scylla and has not been tampered with by the user. It assumes that for all data stored in SSTables, the commitlog, etc.

To summarize, it's very easy to make a mistake here and there is no easy way to fix inconsistencies. Supporting some existing use cases requires awkward workarounds.

We would like there to be an alternative that provides the following properties:

* be simple for an administrator,

* should work even if the schema of the metadata changes in the future, e.g. new parameters are added to service levels,

* be atomic. Group0 changes are linearizable and a backup should produce a description that corresponds to the state of metadata at a specific point—close to the time when the user issued a backup.

# Synopsis of the solution

Because the schema, auth, and service levels can be manipulated by administrators via the CQL interface, we want to generate a sequence of CQL statements that, when applied in order, would restore their current state. The order of those statements is important since there may be dependencies between the entities the statements would recreate, e.g. we can only grant a role to another if they already exist. If new dependencies are introduced, the CQL interface will still be usable as long as we update the implementation.

The solution relies on the assumption that such a sequence of statements can be generated from the internal state.

# Implementation of the solution

We introduce a statement responsible for describing schema, auth, and service level entities: `DESC SCHEMA`. It encompasses three "tiers":

* `DESCRIBE [FULL] SCHEMA`: describe elements of the non-system schema: keyspaces, tables, views, UDTs, etc. When `FULL` is used, it also includes the elements of the system schema, e.g. system tables.

* `DESCRIBE [FULL] SCHEMA WITH INTERNALS`: in addition to the output of the previous tier, the statement also describes auth and service levels. The statements corresponding to restoring roles do *not* contain any information about their passwords. What's more, additional information about tables, materialized views, and secondary indices may be provided.

* `DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS`: aside from the information retrieved as part of the previous tier, the statements corresponding to restoring roles *do* contain information about their passwords, i.e. their salted hashes. For more information, see the relevant section below.

Instead of `DESCRIBE`, the user can use its shortened form: `DESC`.

As a result of the query, the user will obtain a set of rows, each of which consists of four values:

* `keyspace_name`: the name of the keyspace the entity is part of,
* `type`: the type of the entity,
* `name`: the name of the entity,
* `create_statement`: the statement used for restoring the entity.

All of the values are always present.

Executing `DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS` requires that the user performing the query be a superuser.

## Seemingly partial information when describing auth

It may seem as though we might miss some of the information stored by auth in some cases. To better understand the problem,
consider the following scenario:

1. Scylla is configured to use `PasswordAuthenticator` and `CassandraAuthorizer`. That means the user can create
   roles with passwords and grant them permissions.
2. The user creates new roles and grants them permissions.
3. The user decides to start using `AllowAllAuthenticator` and `AllowAllAuthorizer`.

At this point, there are a few important things to note.

Although the column `salted_hash` in `system.roles` is not used anymore,
its contents are still present. Similarly, even though the current authorizer doesn't need to query `system.role_permissions`,
the data that was created when `CassandraAuthorizer` was in use is still there. To summarize, it may happen that the current
configuration doesn't utilize and doesn't necessarily need all of the information that has been created.

That phenomenon is also reflected in the semantics of various structures in auth. For instance, calling `list_all()` on
`auth::allow_all_authorizer` results in an exception instead of a set of permission grants. That example shows that implementations of the interfaces in auth can block access to data that they don't use.

The last thing that's important to note here is the fact that since some data is not used, it's likely that new instances of that
data cannot be created either. For example, when using `AllowAllAuthenticator`, it's impossible to create a role with a password, so no new values in the column `salted_hash` in `system.roles` will appear.

Taking all of this into consideration is crucial when designing the semantics of `DESCRIBE SCHEMA`. Should we include the information
that is unused in the output of the statement, like permission grants? Or should we not?

The conclusion we reached is to rely on the current configuration and on it only. That means that if some data cannot be accessed
via the API, it won't be included. For example, if we use `AllowAllAuthenticator`, no hashed passwords will be printed
when executing `DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS` because hashed passwords are not used by the authenticator.

The rationale for that decision is the fact that we want to backup the current state of Scylla. Restoring hashed passwords, permission grants, etc.
would require changing the configuration to be even able to insert the data. That's something we want to avoid.

For that reason, the user should be aware of this fact and make sure they are prepared for possible "data loss".

## Describing CDC tables
Creating a table with `cdc = {'enabled': true}` leads to the creation of a corresponding CDC log table.
Because of that, when describing the schema, we cannot include create statements for CDC log tables—they'll
have already been recreated by restoring the base tables.

However, there are two problems with not providing any information regarding log tables:

* If the log table has been modified, we need to alter it after it's restored by recreating the base table.
  The reason for that is the options of the log table are independent of the options the base table uses,
  e.g. the base table may use `bloom_filter_fp_chance = 0.1`, while the log table `bloom_filter_fp_chance = 0.3`.
  If we didn't provide an `ALTER` statement for the log table, the restored one would use the default options
  and the schema would be different from the original one,

* When the user executes `DESCRIBE SCHEMA`, they may want to learn about *all* of the elements of the schema.
  Skipping the log tables might suggest that they don't exist.

That's why we impose the following semantics on describing CDC tables:

* `DESCRIBE SCHEMA/KEYSPACE/TABLE` print a `CREATE` statement for the CDC base table,

* `DESCRIBE SCHEMA/KEYSPACE` print an `ALTER` statement for the CDC log table. That statement will
  ensure that the restored log table uses the same parameters as the original one,

* `DESCRIBE TABLE <base table>` prints a `CREATE` statement for the base table and an `ALTER`
  statement for the log table,

* `DESCRIBE TABLE <log table>` prints a `CREATE` statement for the log table *only*.
  The statement is wrapped within CQL comment markers: `/* */`.

The reason why `DESC TABLE <log table>` prints a `CREATE` and not an `ALTER` statement is that it enforces providing
a list of its columns with their types, i.e. we provide complete information about the table.

The rationale for printing the statement as a comment is that it shouldn't be executed by anyone—we only print
the create statement for informational purposes. The user is also told that explicitly in the comment, e.g.

```sql
$ DESC TABLE ks.t_scylla_cdc_log;

/* Do NOT execute this statement! It's only for informational purposes.
   A CDC log table is created automatically when the base is created.

CREATE TABLE ks.t_scylla_cdc_log (
    "cdc$stream_id" blob,
    "cdc$time" timeuuid,
    "cdc$batch_seq_no" int,
    "cdc$end_of_batch" boolean,
    "cdc$operation" tinyint,
    "cdc$ttl" bigint,
    p int,
    PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")
) WITH CLUSTERING ORDER BY ("cdc$time" ASC, "cdc$batch_seq_no" ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'enabled': 'false', 'keys': 'NONE', 'rows_per_partition': 'NONE'}
    AND comment = 'CDC log for ks.t'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': '1800'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND tablets = {'expected_data_size_in_gb': '250', 'min_per_shard_tablet_count': '0.8', 'min_tablet_count': '1'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE';

*/
```


## Restoring process and its side effects

When a resource is created, the current role is always granted full permissions to that resource. As a consequence, the role used to restore the backup will gain permissions to all of the resources that will be recreated.

However, we don't see it as an issue. The role that is used to restore the schema, auth, and service levels *should* be a superuser. Superusers are granted full access to all resources, so unless the superuser status of the role is revoked, there will be no unwanted side effects.

## Restoring roles with passwords

Scylla doesn't store the passwords of the roles; instead, it stores salted hashes corresponding to them. When the user wants to log in, they provide a password. Then, that password and so-called "salt" are used to generate a hash. Finally, that hash is compared against the salted hash stored by the database to determine if the provided password is correct.

To restore a role with its password, we want utilize that salted hash. We introduce a new form of the `CREATE ROLE` statement:

```sql
CREATE ROLE [ IF NOT EXISTS ] `role_name` WITH HASHED PASSWORD = '`hashed_password`' ( AND `role_option` )*
```

where

```sql
role_option: LOGIN '=' `string`
           :| SUPERUSER '=' `boolean`
```

Performing that query will result in creating a new role whose hashed password stored in the database is exactly the same as the one provided by the user. If the specified role already exists, the query will fail and no side effect will be observed—in short, we follow the semantics of the "normal" version of `CREATE ROLE`.

The user executing that statement is required to be a superuser.

[^1]: See: `docs/operating-scylla/procedures/backup-restore`.
