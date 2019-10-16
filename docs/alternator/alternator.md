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

DynamoDB clients usually specify a single "endpoint" address, e.g.,
`dynamodb.us-east-1.amazonaws.com`, and a DNS server hosted on that address
distributes the connections to many different backend nodes. Alternator
does not yet provide such a DNS server, so you should either supply your
own (having it return one of the live Scylla nodes at random, with a TTL
of a few seconds), or you should use a different mechanism to distribute
different DynamoDB requests to different Scylla nodes, to balance the load.

Alternator tables are stored as Scylla tables in the "alternator" keyspace.
This keyspace is initialized when the first Alternator table is created
(with a CreateTable request). The replication factor (RF) for this keyspace
and all Alternator tables is chosen at that point, depending on the size of
the cluster: RF=3 is used on clusters with three or more live nodes, and
RF=1 is used for smaller clusters. Such smaller clusters are, of course,
only recommended for tests because of the risk of data loss.

## Current compatibility with DynamoDB

Our goal is that any application written to use Amazon DynamoDB could
be run, unmodified, against Scylla with Alternator enabled. However, at this
stage the Alternator implementation is incomplete, and some of DynamoDB's
API features are not yet supported. This section documents the extent of
Alternator's compatibility with DynamoDB, and will be updated as the work
progresses and compatibility continues to improve.

### API Server
* Transport: HTTP mostly supported, but small features like CRC header and
  compression are still missing. HTTPS supported on top of HTTP, so small
  features may still be missing.
* Authorization (verifying the originator of the request): implemented
  on top of system\_auth.roles table. The secret key used for authorization
  is the salted\_hash column from the roles table, selected with:
  SELECT salted\_hash from system\_auth.roles WHERE role = USERNAME;
  By default, authorization is not enforced at all. It can be turned on
  by providing an entry in Scylla configuration:
    alternator\_enforce\_authorization: true
* DNS server for load balancing: Not yet supported. Client needs to pick
  one of the live Scylla nodes and send a request to it.
### Table Operations
* CreateTable: Supported. Note our implementation is synchronous.
* UpdateTable: Not supported.
* DescribeTable: Partial implementation. Missing creation date and size esitmate.
* DeleteTable: Supported. Note our implementation is synchronous.
* ListTables: Supported.
### Item Operations
* GetItem: Support almost complete except that projection expressions can
  only ask for top-level attributes.
* PutItem: Does not yet support conditional expressions (to only add an item
  if some condition is true), nor return values (optional return of pre-put
  content).
* UpdateItem: Like PutItem does not yet support conditional expression nor
  return values. Read-modify-write operations such as `SET a=b`,
  `SET a=if_not_exist(a,bal)`, or `SET a=a+1, are supported but not protected
  against concurrent operations. Nested documents are supported but updates
  to nested attributes are not (e.g., `SET a.b[3].c=val`).
* DeleteItem: Mostly works, but again does not support conditional expression
  or return values.
### Batch Operations
* BatchGetItem: Almost complete except that projection expressions can only
  ask for top-level attributes.
* BatchWriteItem: Supported. Doesn't yet verify that there are no duplicates
  in the list of items. Doesn't limit the number of items (DynamoDB limits to
  25) or size of items (400 KB) or total request size (16 MB).
### Scans
* Scan: As usual, projection expressions only support top-level attributes.
  Filter expressions (to filter some of the items) partially supported:
  The ScanFilter syntax is supported but FilterExpression is not yet, and
  only equality operator is supported so far.
  The "Select" options which allows to count items instead of returning them
  is not yet supported. Parallel scan is not yet supported.
* Query: Same issues as Scan above. Additionally, missing support for
  KeyConditionExpression (an alternative syntax replacing the older
  KeyConditions parameter which we do support).
### Secondary Indexes
Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI) are
implemented, with the following limitations:
* GSIs and LSIs can be added only at CreateTable time: GSIs cannot be added
  or removed at a later time (UpdateTable is not yet supported).
* Marking a read from an index as strongly-consistent currently changes
  nothing. Such reads ought to be forbidden for GSI, and be strongly-
  consistent for LSI (see https://github.com/scylladb/scylla/issues/4365)
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
* Not yet supported: CreateGlobalTable, UpdateGlobalTable,
  DescribeGlobalTable, ListGlobalTables, UpdateGlobalTableSettings,
  DescribeGlobalTableSettings. Implementation will use Scylla's multi-DC
  features.
### Backup and Restore
* On-demand backup: Not yet supported: CreateBackup, DescribeBackup,
  DeleteBackup, ListBackups, RestoreTableFromBackup. Implementation will
  use Scylla's snapshots
* Continuous backup: Not yet supported: UpdateContinuousBackups,
  DescribeContinuousBackups, RestoreTableToPoinInTime.
### Transations
* Not yet supported: TransactWriteItems, TransactGetItems.
  Note that this is a new DynamoDB feature - these are more powerful than
  the old conditional updates which were "lightweight transactions".
### Streams (CDC)
* Not yet supported
### Encryption at rest
* Supported natively by Scylla, but needs to be enabled by default.
### ARNs and tags
* Various features use ARN (Amazon Resource Names) which we don't support.
* Not yet supported: ListTagsOfResource, TagResource, UntagResource.
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
* Several metrics are available through the Grafana/Promethues stack: https://docs.scylladb.com/operating-scylla/monitoring/   It is different than the expectations of the current DynamoDB implementation. However, our
  monitoring is rather advanced and provide more insights to the internals.

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

Each table in Alternator is stored as a Scylla table in the "alternator"
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
or request that the previous values of attributes be returned.
Alternator currently implements all these RMW operations naively - it simply
performs a read before the write. This naive approach is **not safe** when
there are concurrent operations on the same attributes, so it will be revised
in the future. We will probably use lightweight transactions - a feature
which already exists in Cassandra and is planned to soon reach Scylla.

DynamoDB allows attributes to be **nested** - a top-level attribute may
be a list or a map, and each of its elements may further be lists or
maps, etc. Alternator currently stores the entire content of a top-level
attribute as one JSON object. This is good enough for most needs, except
one DynamoDB feature which we cannot support safely: we cannot modify
a non-top-level attribute (e.g., a.b[3].c) directly without RMW. We plan
to fix this in a future version by rethinking the data model we use for
attributes, or rethinking our implementation of RMW (as explained above).

For reasons explained above, the data model used by Alternator to store
data on disk is still in a state of flux, and may change in future versions.
Therefore, in this early stage it is not recommended to store important
production data using Alternator.
