# ScyllaDB CQL Extensions

ScyllaDB extends the CQL language to provide a few extra features. This document
lists those extensions.

## BYPASS CACHE clause

The `BYPASS CACHE` clause on `SELECT` statements informs the database that the data
being read is unlikely to be read again in the near future, and also
was unlikely to have been read in the near past; therefore no attempt
should be made to read it from the cache or to populate the cache with
the data. This is mostly useful for range scans; these typically
process large amounts of data with no temporal locality and do not
benefit from the cache.

The clause is placed immediately after the optional `ALLOW FILTERING`
clause:

    SELECT ... FROM ...
    WHERE ...
    ALLOW FILTERING          -- optional
    BYPASS CACHE

## "Paxos grace seconds" per-table option

The `paxos_grace_seconds` option is used to set the amount of seconds which
are used to TTL data in paxos tables when using LWT queries against the base
table.

This value is intentionally decoupled from `gc_grace_seconds` since,
in general, the base table could use completely different strategy to garbage
collect entries, e.g. can set `gc_grace_seconds` to 0 if it doesn't use
deletions and hence doesn't need to repair.

However, paxos tables still rely on repair to achieve consistency, and
the user is required to execute repair within `paxos_grace_seconds`.

Default value is equal to `DEFAULT_GC_GRACE_SECONDS`, which is 10 days.

The option can be specified at `CREATE TABLE` or `ALTER TABLE` queries in the same
way as other options by using `WITH` clause:

    CREATE TABLE tbl ...
    WITH paxos_grace_seconds=1234

## USING TIMEOUT

TIMEOUT extension allows specifying per-query timeouts. This parameter accepts a single
duration and applies it as a timeout specific to a single particular query.
The parameter is supported for prepared statements as well.
The parameter acts as part of the USING clause, and thus can be combined with other
parameters - like timestamps and time-to-live.
For example, one can use ``USING TIMEOUT ... and TTL ...`` to specify both a non-default timeout and a ttl.

Examples:
```cql
	SELECT * FROM t USING TIMEOUT 200ms;
```
```cql
	INSERT INTO t(a,b,c) VALUES (1,2,3) USING TIMESTAMP 42 AND TIMEOUT 50ms;
```
```cql
	TRUNCATE TABLE t USING TIMEOUT 5m;
```

Working with prepared statements works as usual - the timeout parameter can be
explicitly defined or provided as a marker:

```cql
	SELECT * FROM t USING TIMEOUT ?;
```
```cql
	INSERT INTO t(a,b,c) VALUES (?,?,?) USING TIMESTAMP 42 AND TIMEOUT 50ms;
```

The timeout parameter can be applied to the following data modification queries:
INSERT, UPDATE, DELETE, PRUNE MATERIALIZED VIEW, BATCH,
and to the TRUNCATE data definition query.

In addition, the timeout parameter can be applied to SELECT queries as well.

```{eval-rst}
.. _keyspace-storage-options:
 ```
 
## Keyspace storage options

<!---
This section must be moved to Data Definition> CREATE KEYSPACE
when support for object storage is GA.
 --->

By default, SStables of a keyspace are stored in a local directory.
As an alternative, you can configure your keyspace to be stored
on Amazon S3 or another S3-compatible object store.

Support for object storage is experimental and must be explicitly
enabled in the ``scylla.yaml`` configuration file by specifying 
the ``keyspace-storage-options`` option:

```
 experimental_features:
     - keyspace-storage-options
```

With support for object storage enabled, add your endpoint configuration
to ``scylla.yaml``:

1. Create an ``object-storage-config-file.yaml`` file with a description of 
   allowed endpoints, for example:

    ```
      endpoints:
        - name: $endpoint_address_or_domain_name
          port: $port_number
          https: optional True or False
          aws_region: optional region name, e.g. us-east-1
          aws_access_key_id: optional AWS access key ID
          aws_secret_access_key: optional AWS secret access key
          aws_session_token: optional AWS session token
    ```
1. Specify the ``object-storage-config-file`` option in your ``scylla.yaml``,
   providing ``object-storage-config-file.yaml`` as the value:

   ```
   object-storage-config-file: object-storage-config-file.yaml
   ```


Now you can configure your object storage when creating a keyspace:

```cql
CREATE KEYSPACE with STORAGE = { 'type': 'S3', 'endpoint': '$endpoint_name', 'bucket': '$bucket' } 
```

**Example**

```cql
CREATE KEYSPACE ks
    WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }
    AND STORAGE = { 'type' : 'S3', 'bucket' : '/tmp/b1', 'endpoint' : 'localhost' } ;
```

Storage options can be inspected by checking the new system schema table: `system_schema.scylla_keyspaces`:

```cql
    cassandra@cqlsh> select * from system_schema.scylla_keyspaces;
    
     keyspace_name | storage_options                                | storage_type
    ---------------+------------------------------------------------+--------------
               ksx | {'bucket': '/tmp/xx', 'endpoint': 'localhost'} |           S3
```

## PRUNE MATERIALIZED VIEW statements

A special statement is dedicated for pruning ghost rows from materialized views.
Ghost row is an inconsistency issue which manifests itself by having rows
in a materialized view which do not correspond to any base table rows.
Such inconsistencies should be prevented altogether and ScyllaDB is striving to avoid
them, but *if* they happen, this statement can be used to restore a materialized view
to a fully consistent state without rebuilding it from scratch.

Example usages:
```cql
  PRUNE MATERIALIZED VIEW my_view;
  PRUNE MATERIALIZED VIEW my_view WHERE token(v) > 7 AND token(v) < 1535250;
  PRUNE MATERIALIZED VIEW my_view WHERE v = 19;
```

The statement works by fetching requested rows from a materialized view
and then trying to fetch their corresponding rows from the base table.
If it turns out that the base row does not exist, the row is considered
a ghost row and is thus deleted. The statement implicitly works with
consistency level ALL when fetching from the base table to avoid false
positives. As the example shows, a materialized view can be pruned
in one go, but one can also specify specific primary keys or token ranges,
which is recommended in order to make the operation less heavyweight
and allow for running multiple parallel pruning statements for non-overlapping
token ranges.

## Synchronous materialized views

Usually, when a table with materialized views is updated, the update to the
views happens _asynchronously_, i.e., in the background. This means that
the user cannot know when the view updates have all finished - or even be
sure that they succeeded.

ScyllaDB allows marking a view as synchronous. When a view
is marked synchronous, base-table updates will wait for that view to be
updated before returning. A base table may have multiple views marked
synchronous, and will wait for all of them. The consistency level of a
write applies to synchronous views as well as to the base table: For
example, writing with QUORUM consistency level returns only after a
quorum of the base-table replicas were updated *and* also a quorum of
each synchronous view table was also updated.

Synchronous views tend to reduce the observed availability of the base table,
because a base-table write would only succeed if enough synchronous view
updates also succeed. On the other hand, failed view updates would be
detected immediately, and appropriate action can be taken, such as retrying
the write or pruning the materialized view (as mentioned in the previous
section). This can improve the consistency of the base table with its views.

To create a new materialized view with synchronous updates, use:

```cql
CREATE MATERIALIZED VIEW main.mv
  AS SELECT * FROM main.t
  WHERE v IS NOT NULL
  PRIMARY KEY (v, id)
  WITH synchronous_updates = true;
```

To make an existing materialized view synchronous, use:

```cql
ALTER MATERIALIZED VIEW main.mv WITH synchronous_updates = true;
```

To return a materialized view to the default behavior (which, as explained
above, _usually_ means asynchronous updates), use:

```cql
ALTER MATERIALIZED VIEW main.mv WITH synchronous_updates = false;
```

Even in an asynchronous view, _some_ view updates may be done synchronously.
This happens when the materialized-view replica is on the same node as the
base-table replica. This happens, for example, in tables using vnodes where
the base table and the view have the same partition key; But is not the case
if the table uses tablets: With tablets, the base and view tablets may migrate
to different nodes. In general, users should not, and cannot, rely on these
serendipitous synchronous view updates; If synchronous view updates are
important, mark the view explicitly with `synchronous_updates = true`.

### Synchronous global secondary indexes

Synchronous updates can also be turned on for global secondary indexes.
At the time of writing this paragraph there is no direct syntax to do that,
but it's possible to mark the underlying materialized view of an index
as synchronous. ScyllaDB's implementation of secondary indexes is based
on materialized views and the generated view's name can be extracted
from schema tables, and is generally constructed by appending `_index`
suffix to the index name:

```cql
create table main.t(id int primary key, v int);
create index on main.t(v);

select * from system_schema.indexes ;

 keyspace_name | table_name | index_name | kind       | options
---------------+------------+------------+------------+-----------------
          main |          t |    t_v_idx | COMPOSITES | {'target': 'v'}

(1 rows)


select keyspace_name, view_name from system_schema.views ;

 keyspace_name | view_name
---------------+---------------
          main | t_v_idx_index

(1 rows)

alter materialized view t_v_idx_index with synchronous_updates = true;

```

Local secondary indexes already have synchronous updates, so there's no need
to explicitly mark them as such.

## Expressions

### NULL

Scylla aims for a uniform handling of NULL values in expressions, inspired
by SQL: The overarching principle is that a NULL signifies an _unknown value_,
so most expressions calculated based on a NULL also results in a NULL.
For example, the results of `x + NULL`, `x = NULL` or `x < NULL` are all NULL,
no matter what `x` is. Even the expression `NULL = NULL` evaluates to NULL,
not TRUE.

But not all expressions of NULL evaluate to NULL. An interesting example
is boolean conjunction:`FALSE AND NULL` returns FALSE - not NULL. This is
because no matter which unknown value the NULL represents, ANDing it with
FALSE will always result in FALSE. So the return value is not unknown - it
is a FALSE. In contrast, `TRUE AND NULL` does return NULL, because if we AND
a TRUE with an unknown value the result is also unknown: `TRUE AND TRUE` is
TRUE but `TRUE AND FALSE` is FALSE.

Because `x = NULL` always evaluates to NULL, a `SELECT` filter `WHERE x = NULL`
matches no row (_matching_ means evaluating to TRUE). It does **not** match
rows where x is missing. If you really want to match rows with missing x,
SQL offers a different syntax `x IS NULL` (and similarly, also `x IS NOT
NULL`), Scylla does not yet implement this syntax.

In contrast, Cassandra is less consistent in its handling of nulls.
The example `x = NULL` is considered an error, not a valid expression
whose result is NULL.

The rules explained above apply to most expressions, in particular to `WHERE`
filters in `SELECT`. However, the evaluation rules for LWT IF clauses
(_conditional updates_) are _different_: a `IF x = NULL` condition succeeds
if `x` is unset. This non-standard behavior of NULLs in IF expressions may
be made configurable in a future version.

## `NULL` is valid input for LWT IF clause element access

The LWT IF clauses

```cql
IF some_map[:var] = 3
```

or

```cql
IF some_map[:var] != 3
```


is an error if `:var` is `NULL` on Cassandra, but is accepted by
Scylla. The result of the comparison, for both `=` and `!=`, is `FALSE`.

## `NULL` is valid input for LWT IF clause `LIKE` patterns

The LWT IF clauses

```cql
IF some_column LIKE :pattern
```

is an error if `:pattern` is `NULL` on Cassandra, but is accepted by
Scylla. The result of the pattern match is `FALSE`.


## REDUCEFUNC for UDA

REDUCEFUNC extension adds optional reduction function to user-defined aggregate.
This allows to speed up aggregation query execution by distributing the calculations
to other nodes and reducing partial results into final one.
Specification of this function is it has to be scalar function with two arguments,
both of the same type as UDA's state, also returning the state type.

```cql
CREATE FUNCTION row_fct(acc tuple<bigint, int>, val int)
RETURNS NULL ON NULL INPUT
RETURNS tuple<bigint, int>
LANGUAGE lua
AS $$
  return { acc[1]+val, acc[2]+1 }
$$;

CREATE FUNCTION reduce_fct(acc tuple<bigint, int>, acc2 tuple<bigint, int>)
RETURNS NULL ON NULL INPUT
RETURNS tuple<bigint, int>
LANGUAGE lua
AS $$
  return { acc[1]+acc2[1], acc[2]+acc2[2] }
$$;

CREATE FUNCTION final_fct(acc tuple<bigint, int>)
RETURNS NULL ON NULL INPUT
RETURNS double
LANGUAGE lua
AS $$
  return acc[1]/acc[2]
$$;

CREATE AGGREGATE custom_avg(int)
SFUNC row_fct
STYPE tuple<bigint, int>
REDUCEFUNC reduce_fct
FINALFUNC final_fct
INITCOND (0, 0);
```

### Lists elements for filtering

Subscripting a list in a WHERE clause is supported as are maps.

```cql
WHERE some_list[:index] = :value
```

## Per-partition rate limit

The `per_partition_rate_limit` option can be used to limit the allowed
rate of requests to each partition in a given table. When the cluster detects
that the rate of requests exceeds configured limit, the cluster will start
rejecting some of them in order to bring the throughput back to the configured
limit. Rejected requests are less costly which can help reduce overload.

_NOTE_: Due to ScyllaDB's distributed nature, tracking per-partition request rates
is not perfect and the actual rate of accepted requests may be higher up to
a factor of keyspace's `RF`. This feature should not be used to enforce precise
limits but rather serve as an overload protection feature.

_NOTE_: This feature works best when shard-aware drivers are used (rejected
requests have the least cost).

Limits are configured separately for reads and writes. Some examples:

```cql
    ALTER TABLE t WITH per_partition_rate_limit = {
        'max_reads_per_second': 100,
        'max_writes_per_second': 200
    };
```

Limit reads only, no limit for writes:
```cql
    ALTER TABLE t WITH per_partition_rate_limit = {
        'max_reads_per_second': 200
    };
```

Rejected requests receive the scylla-specific "Rate limit exceeded" error.
If the driver doesn't support it, `Config_error` will be sent instead.

For more details, see:

- Detailed [design notes](https://github.com/scylladb/scylla/blob/master/docs/dev/per-partition-rate-limit.md)
- Description of the [rate limit exceeded](https://github.com/scylladb/scylla/blob/master/docs/dev/protocol-extensions.md#rate-limit-error) error

## Effective service level

Actual values of service level's options may come from different service levels, not only from the one user is assigned with.

To facilitate insight into which values come from which service level, there is ``LIST EFFECTIVE SERVICE LEVEL OF <role_name>`` command.
```cql
    > LIST EFFECTIVE SERVICE LEVEL OF role2;

     service_level_option | effective_service_level | value
    ----------------------+-------------------------+-------------
            workload_type |                     sl2 |       batch
                  timeout |                     sl1 |          2s
```

For more details, check [Service Levels docs](https://github.com/scylladb/scylla/blob/master/docs/cql/service-levels.rst)
