# Scylla CQL extensions

Scylla extends the CQL language to provide a few extra features. This document
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
In order for this parameter to be effective for read operations as well, it's possible
to attach USING clause to SELECT statements.

Examples:
```cql
	SELECT * FROM t USING TIMEOUT 200ms;
```
```cql
	INSERT INTO t(a,b,c) VALUES (1,2,3) USING TIMESTAMP 42 AND TIMEOUT 50ms;
```

Working with prepared statements works as usual - the timeout parameter can be
explicitly defined or provided as a marker:

```cql
	SELECT * FROM t USING TIMEOUT ?;
```
```cql
	INSERT INTO t(a,b,c) VALUES (?,?,?) USING TIMESTAMP 42 AND TIMEOUT 50ms;
```
