# Alternator: DynamoDB API in Scylla

## Introduction
Alternator is a Scylla feature adding compatibility with Amazon DynamoDB(TM).
DynamoDB's API uses JSON-encoded requests and responses which are sent over
an HTTP or HTTPS transport. It is described in detail on Amazon's site:
  https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/

Our goal is that any application written to use Amazon DynamoDB could
be run, unmodified, against Scylla with Alternator enabled. However, at this
stage the Alternator implementation is incomplete, and some of DynamoDB's
API features are not yet supported. The extent of Alternator's compatibility
with DynamoDB is described in the "current compatibility" section below.

## Running Alternator
By default, Scylla does not listen for DynamoDB API requests. To enable
such requests, you must set the `alternator-port` configuration option
(via command line or YAML) to the port on which you wish to listen for
DynamoDB API requests.

For example., "`--alternator-port=8000`" on the command line will run
Alternator on port 8000 - the traditional port used by DynamoDB.

By default, Scylla listens on this port on all network interfaces.
To listen only on a specific interface, pass also an "`alternator-address`"
option.

As we explain below in the "Write isolation policies", Alternator has
four different choices for the implementation of writes, each with
different advantages. You should consider which of the options makes
more sense for your intended use case, and use the "`--alternator-write-isolation`"
option to choose one. There is currently no default for this option: Trying
to run Scylla with Alternator enabled without passing this option will
result in an error asking you to set it.

DynamoDB clients usually specify a single "endpoint" address, e.g.,
`dynamodb.us-east-1.amazonaws.com`, and a DNS server and/or load balancers
distribute the connections to many different backend nodes. Alternator
does not provide such a load-balancing setup, so you set one up - having
it forward each request to one of the live Scylla nodes at random.

Alternator tables are stored as Scylla tables, each in a separate keyspace.
Each keyspace is initialized when the corresponding Alternator table is
created (with a CreateTable request). The replication factor (RF) for this
keyspace is chosen at that point, depending on the size of the cluster:
RF=3 is used on clusters with three or more nodes, and RF=1 is used for
smaller clusters. Such smaller clusters are, of course, only recommended
for tests because of the risk of data loss.

## Current compatibility with DynamoDB

Our goal is that any application written to use Amazon DynamoDB could
be run, unmodified, against Scylla with Alternator enabled. However, at this
stage the Alternator implementation is incomplete, and some of DynamoDB's
API features are not yet supported. This section documents the extent of
Alternator's compatibility with DynamoDB, and will be updated as the work
progresses and compatibility continues to improve.

### API Server
* Transport: HTTP and HTTPS are mostly supported, but small features like CRC
  header and compression are still missing.
* Authorization (verifying the originator of the request): implemented
  on top of system\_auth.roles table. The secret key used for authorization
  is the salted\_hash column from the roles table, selected with:
  SELECT salted\_hash from system\_auth.roles WHERE role = USERNAME;
  By default, authorization is not enforced at all. It can be turned on
  by providing an entry in Scylla configuration:
    alternator\_enforce\_authorization: true
* Load balancing: Not a part of Alternator. One should use an external load
  balancer or DNS server to balance the requests between the live Scylla
  nodes. We plan to publish a reference example soon.
### Table Operations
* CreateTable and DeleteTable: Supported. Note our implementation is synchronous.
* DescribeTable: Partial implementation. Missing creation date and size estimate.
* UpdateTable: Not supported.
* ListTables: Supported.
### Item Operations
* GetItem: Support almost complete except that projection expressions can
  only ask for top-level attributes.
* PutItem: Support almost complete except that condition expressions can
  only refer to to-level attributes.
* UpdateItem: Nested documents are supported but updates to nested attributes
  are not (e.g., `SET a.b[3].c=val`), and neither are nested attributes in
  condition expressions.
* DeleteItem: Mostly works, but again does not support nested attributes
  in condition expressions.
### Batch Operations
* BatchGetItem: Almost complete except that projection expressions can only
  ask for top-level attributes.
* BatchWriteItem: Supported. Doesn't limit the number of items (DynamoDB
  limits to 25) or size of items (400 KB) or total request size (16 MB).
### Scans
Scan and Query are mostly supported, with the following limitations:
* As above, projection expressions only support top-level attributes.
* The ScanFilter/QueryFilter parameter for filtering results is fully
  supported, but the newer FilterExpression syntax is not yet supported.
* The "Select" options which allows to count items instead of returning them
  is not yet supported.
### Secondary Indexes
Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI) are
implemented, with the following limitations:
* GSIs and LSIs can be added only at CreateTable time: GSIs cannot be added
  or removed at a later time (UpdateTable is not yet supported).
* DescribeTable lists the indexes for the table, but is missing some
  additional information on each index.
* Projection of only a subset of the base-table attributes to the index is
  not respected: All attributes are projected.
### Time To Live (TTL)
* Not yet supported. Note that this is a different feature from Scylla's
  feature with the same name.
### Replication
* Supported, with RF=3 (unless running on a cluster of less than 3 nodes).
  Writes are done in LOCAL_QURUM and reads in LOCAL_ONE (eventual consistency)
  or LOCAL_QUORUM (strong consistency).
### Global Tables
* Currently, *all* Alternator tables are created as "global" tables and can
  be accessed from all the DCs existing at the time of the table's creation.
  If a DC is added after a table is created, the table won't be visible from
  the new DC and changing that requires a CQL "ALTER TABLE" statement to
  modify the table's replication strategy.
* We do not yet support the DynamoDB API calls that control which table is
  visible from what DC: CreateGlobalTable, UpdateGlobalTable,
  DescribeGlobalTable, ListGlobalTables, UpdateGlobalTableSettings,
  DescribeGlobalTableSettings, and UpdateTable.
### Backup and Restore
* On-demand backup: the DynamoDB APIs are not yet supported: CreateBackup,
  DescribeBackup, DeleteBackup, ListBackups, RestoreTableFromBackup.
  Users can use Scylla's [snapshots](https://docs.scylladb.com/operating-scylla/procedures/backup-restore/)
  or [Scylla Manager](https://docs.scylladb.com/operating-scylla/manager/2.0/backup/).
* Continuous backup: Not yet supported: UpdateContinuousBackups,
  DescribeContinuousBackups, RestoreTableToPoinInTime.
### Transactions
* Not yet supported: TransactWriteItems, TransactGetItems.
  Note that this is a new DynamoDB feature - these are more powerful than
  the old conditional updates which were "lightweight transactions".
### Streams
* Scylla has experimental support for [CDC](https://docs.scylladb.com/using-scylla/cdc/)
  (change data capture), but the "DynamoDB Streams" API is not yet supported.
### Encryption at rest
* Supported by Scylla Enterprise (not in open-source). Needs to be enabled.
### Tags
* Tagging tables is fully supported, at CreateTable time (with the "Tags"
  parameter) and later using TagResource, UntagResource and ListTagsOfResource.
### Accounting and capping
* Not yet supported. Mainly for multi-tenant cloud use, we need to track
  resource use of individual requests (the API should also optionally
  return this use), and be able to sum this use for different tenants and/or
  tables, and possible cap use according to reservation.
### Multi-tenant support
* Not yet supported (related to authorization, accounting, etc.)
### DAX (cache)
* Not required. Scylla cache is rather advanced and there is no need to place
  a cache in front of the database: https://www.scylladb.com/2017/07/31/database-caches-not-good/
### Metrics
* Several metrics are available through the Grafana/Prometheus stack:
  https://docs.scylladb.com/operating-scylla/monitoring/
  Those are different from the current DynamoDB metrics, but Scylla's
  monitoring is rather advanced and provide more insights to the internals.

## Alternator-specific API

### Write isolation policies
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
isolation policy for a specific table can be overriden by tagging the table
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

### Accessing system tables from Scylla
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

## Alternator design and implementation

This section provides only a very brief introduction to Alternator's
design. A much more detailed document about the features of the DynamoDB
API and how they are, or could be, implemented in Scylla can be found in:
https://docs.google.com/document/d/1i4yjF5OSAazAY_-T8CBce9-2ykW4twx_E_Nt2zDoOVs

Almost all of Alternator's source code (except some initialization code)
can be found in the alternator/ subdirectory of Scylla's source code.
Extensive functional tests can be found in the alternator-test/
subdirectory. These tests are written in Python, and can be run against
both Alternator and Amazon's DynamoDB; This allows verifying that
Alternator's behavior matches the one observed on DynamoDB.
See alternator-test/README.md for more information about the tests and
how to run them.

With Alternator enabled on port 8000 (for example), every Scylla node
listens for DynamoDB API requests on this port. These requests, in
JSON format over HTTP, are parsed and result in calls to internal Scylla
C++ functions - there is no CQL generation or parsing involved.
In Scylla terminology, the node receiving the request acts as the the
*coordinator*, and often passes the request on to one or more other nodes -
*replicas* which hold copies of the requested data.

DynamoDB supports two consistency levels for reads, "eventual consistency"
and "strong consistency". These two modes are implemented using Scylla's CL
(consistency level) feature: All writes are done using the LOCAL_QUORUM
consistency level, then strongly-consistent reads are done with
LOCAL_QUORUM, while eventually-consistent reads are with just LOCAL_ONE.

Each table in Alternator is stored as a Scylla table in a separate
keyspace. The DynamoDB key columns (hash and sort key) have known types,
and become partition and clustering key columns of the Scylla table.
All other attributes may be different for each row, so are stored in one
map column in Scylla, and not as separate columns.

In Scylla (and its inspiration, Cassandra), high write performance is
achieved by ensuring that writes do not require reads from disk.
The DynamoDB API, however, provides many types of requests that need a read
before the write (a.k.a. RMW requests - read-modify-write). For example,
a request may copy an existing attribute, increment an attribute,
be conditional on some expression involving existing values of attribute,
or request that the previous values of attributes be returned. These
read-modify-write transactions should be _isolated_ from each other, so
by default Alternator implements every write operation using Scylla's
LWT (lightweight transactions). This default can be overridden on a per-table
basis, by tagging the table as explained above in the "write isolation
policies" section.

DynamoDB allows attributes to be **nested** - a top-level attribute may
be a list or a map, and each of its elements may further be lists or
maps, etc. Alternator currently stores the entire content of a top-level
attribute as one JSON object. This is good enough for most needs, except
one DynamoDB feature which we cannot support safely: we cannot modify
a non-top-level attribute (e.g., a.b[3].c) directly without RMW. We plan
to fix this in a future version by rethinking the data model we use for
attributes, or rethinking our implementation of RMW (as explained above).
