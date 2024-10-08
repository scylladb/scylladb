# Alternator-specific APIs

Alternator's primary goal is to be compatible with Amazon DynamoDB(TM)
and its APIs, so that any application written to use Amazon DynamoDB could
be run, unmodified, against Scylla with Alternator enabled. The extent of
Alternator's compatibility with DynamoDB is described in the
[Scylla Alternator for DynamoDB users](compatibility.md) document.

But Alternator also adds several features and APIs that are not available in
DynamoDB. These Alternator-specific APIs are documented here.

## Write isolation policies
DynamoDB API update requests may involve a read before the write - e.g., a
_conditional_ update or an update based on the old value of an attribute.
The read and the write should be treated as a single transaction - protected
(_isolated_) from other parallel writes to the same item.

Alternator could do this isolation by using Scylla's LWT (lightweight
transactions) for every write operation, but this significantly slows
down writes, and not necessary for workloads which don't use read-modify-write
(RMW) updates.

So Alternator supports four _write isolation policies_, which can be chosen
on a per-table basis and may make sense for certain workloads as explained
below.

A default write isolation policy **must** be chosen using the
`--alternator-write-isolation` configuration option. Additionally, the write
isolation policy for a specific table can be overridden by tagging the table
(at CreateTable time, or any time later with TagResource) with the key
`system:write_isolation`, and one of the following values:

  * `a`, `always`, or `always_use_lwt` - This mode performs every write
    operation - even those that do not need a read before the write - as a
    lightweight transaction.

    This is the slowest choice, but also the only choice guaranteed to work
    correctly for every workload.

  * `f`, `forbid`, or `forbid_rmw` - This mode _forbids_ write requests
    which need a read before the write. An attempt to use such statements
    (e.g.,  UpdateItem with a ConditionExpression) will result in an error.
    In this mode, the remaining write requests which are allowed - pure writes
    without a read - are performed using standard Scylla writes, not LWT,
    so they are significantly faster than they would have been in the
    `always_use_lwt`, but their isolation is still correct.

    This mode is the fastest mode which is still guaranteed to be always
    safe. However, it is not useful for workloads that do need read-modify-
    write requests on this table - which this mode forbids.

  * `o`, or `only_rmw_uses_lwt` - This mode uses LWT only for updates that
    require read-modify-write, and does normal quorum writes for write-only
    updates.

    The benefit of this mode is that it allows fast write-only updates to some
    items, while still allowing some slower read-modify-write operations to
    other items. However, This mode is only safe if the workload does not mix
    read-modify-write and write-only updates to the same item, concurrently.
    It cannot verify that this condition is actually honored by the workload.

  * `u`, `unsafe`, or `unsafe_rmw` - This mode performs read-modify-write
    operations as separate reads and writes, without any isolation guarantees.
    It is the fastest option, but not safe - it does not correctly isolate
    read-modify-write updates. This mode is not recommended for any use case,
    and will likely be removed in the future.

## Accessing system tables from Scylla
 * Scylla exposes lots of useful information via its internal system tables,
   which can be found in system keyspaces: 'system', 'system\_auth', etc.
   In order to access to these tables via alternator interface,
   Scan and Query requests can use a special table name:
   .scylla.alternator.KEYSPACE\_NAME.TABLE\_NAME
   which will return results fetched from corresponding Scylla table.
   This interface can be used only to fetch data from system tables.
   Attempts to read regular tables via the virtual interface will result
   in an error.
   Example: in order to query the contents of Scylla's system.large_rows,
   pass TableName='.scylla.alternator.system.large_rows' to a Query/Scan request.
