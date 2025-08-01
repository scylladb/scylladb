# ScyllaDB Alternator for DynamoDB users

Scylla supports the DynamoDB API (this feature is codenamed "Alternator").
Our goal is to support any application written for Amazon DynamoDB.
Nevertheless, there are a few differences between DynamoDB and Scylla, and
and a few DynamoDB features that have not yet been implemented in Scylla.
The purpose of this document is to inform users of these differences.

## Provisioning

The most obvious difference between DynamoDB and Scylla is that while
DynamoDB is a shared cloud service, Scylla is a dedicated service running
on your private cluster. Whereas DynamoDB allows you to "provision" the
number of requests per second you'll need - or at an extra cost not even
provision that - Scylla requires you to provision your cluster. You need
to reason about the number and size of your nodes - not the throughput.

Moreover, DynamoDB's per-table provisioning (`BillingMode=PROVISIONED`) is
not yet supported by Scylla. The BillingMode and ProvisionedThroughput options
on a table need to be valid but are ignored, and Scylla behaves like DynamoDB's
`BillingMode=PAY_PER_REQUEST`: All requests are accepted without a per-table
throughput cap.

## Load balancing

DynamoDB applications specify a single "endpoint" address, e.g.,
`dynamodb.us-east-1.amazonaws.com`. Amazon's cloud service distributes
request for this single URL to many different backend nodes. Such a
load-balancing setup is *not* included inside Alternator. You should either
set one up, or configure the client library to do the load balancing itself.
Instructions for doing this can be found in:
<https://github.com/scylladb/alternator-load-balancing/>

## Write isolation policies

Scylla was designed to optimize the performance of pure write operations -
writes which do not need to read the previous value of the item.
In CQL, writes which do need the previous value of the item must explicitly
use the slower LWT ("LightWeight Transaction") feature to be correctly
isolated from each other. It is not allowed to mix LWT and non-LWT writes
to the same item.

In contrast, in the DynamoDB API every write operation may need the previous
value of the item. So without making further assumptions, Alternator would
need to use the slower LWT for all writes - to correctly isolate concurrent
writes. However, if Alternator is told that a certain workload does not have
any read-modify-write operations, it can do all writes with the faster
non-LWT write. Furthermore, if Alternator is told that a certain workload
does have do both write-only and read-modify-write, but to *different* items,
it could use LWT only for the read-modify-write operations.

Therefore, Alternator must be explicitly configured to tell it which of the
above assumptions it may make on the write workload. This configuration is
mandatory, and described in the "Write isolation policies" section of
[Alternator-specific APIs](new-apis.md). One of the options, `always_use_lwt`,
is always safe, but the other options result in significantly better write
performance and should be considered when the workload involves pure writes
(e.g., ingestion of new data) or if pure writes and read-modify-writes go
to distinct items.

## Avoiding write reordering

When a DynamoDB application writes twice to the same item, it expects "last-
write-wins" behavior: The later write should overwrite the earlier write.
When writes use LWT (the `always_use_lwt` policy described above), this is
indeed guaranteed. However, for other write isolation policies, Scylla
does not guarantee that writes are not reordered. In some sense, the "last"
write does still win, but the meaning of which write is "last" is different
from what most users expect:

In this case (write isolation policy is not `always_use_lwt`), each write
request gets a _timestamp_ which is the current time on the server which
received this request. If two write requests arrive at _different_ Alternator
nodes, and if the local clocks on these two nodes are _not_ accurately
synchronized, then the two timestamps generated independently on the two
nodes may have the opposite order as intended - the _earlier_ write may get
a _higher_ timestamp - and this will be the "last write" that wins.

To avoid or mitigate this write reordering issue, users may consider
one or more of the following:

1. Use NTP to keep the clocks on the different Scylla nodes synchronized.
   If the delay between the two writes is longer than NTP's accuracy,
   they will not be reordered.
2. If an application wants to ensure that two specific writes are not
   reordered, it should send both requests to the same Scylla node.
   Care should be taken when using a load balancer - which might redirect
   two requests to two different nodes.
3. Consider using the `always_use_lwt` write isolation policy.
   It is slower, but has better guarantees.

Another guarantee that `always_use_lwt` can make and other write
isolation modes do not is that writes to the same item are _serialized_:
Even if the two write are sent at exactly the same time to two different
nodes, the result will appear as if one write happened first, and then
the other. But in other modes (with non-LWT writes), two writes can get
exactly the same microsecond-resolution timestamp, the result may be
a mixture of both writes - some attributes from one and some from the
other - instead of being just one or the other.

## Authentication and Authorization

By default, Alternator does not enforce authentication or authorization,
and any request from any connected client will be allowed. To enforce
client authentication, and authorization of which client is allowed
to do what, configure the following in ScyllaDB's configuration:

```
    alternator_enforce_authorization: true
```

Alternator implements the same [signature protocol](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
as DynamoDB and the rest of AWS. Clients use, as usual, an access key ID and
a secret access key to prove their identity and the authenticity of their
request. Alternator can then validate the authenticity and authorization of
each request using a known list of authorized key pairs.

To create a user for authentication, use the CQL "CREATE ROLE" command.
When a client signs a request, it uses the name of this role as the access
key ID, and the _salted hash_ of the role's password is the secret key.
This secret key for role XYZ can be retrieved by the CQL request
`SELECT salted_hash from system.roles WHERE role = XYZ;`.

Alternator also implements authorization, or _access control_ - defining
which authenticated user is allowed to do which operation, such as reading
or writing to a specific table. The way this is supported in Alternator is
currently _not_ compatible with DynamoDB's APIs (IAM or PutResourcePolicy).
Instead, one needs to grant permissions to specific roles using CQL, with
the `GRANT` command. For example, an Alternator table "xyz" is visible to
CQL as `alternator_xyz.xyz`, and the following command will allow requests
from user "myrole" to read this table (with GetItem and other read operations):
`GRANT SELECT ON alternator_xyz.xyz TO myrole`. Refer to the CQL documentation
on how to use GRANT and REVOKE to control permissions:
<https://docs.scylladb.com/stable/operating-scylla/security/authorization.html>

The following permissions are needed to run the following API operations:

 * `SELECT`:      GetItem, Query, Scan, BatchGetItem, GetRecords
 * `MODIFY`:      PutItem, DeleteItem, UpdateItem, BatchWriteItem
 * `CREATE`:      CreateTable
 * `DROP`:        DeleteTable
 * `ALTER`:       UpdateTable, TagResource, UntagResource, UpdateTimeToLive
 * _none needed_: ListTables, DescribeTable, DescribeEndpoints,
                  ListTagsOfResource, DescribeTimeToLive,
                  DescribeContinuousBackups, ListStreams, DescribeStream,
                  GetShardIterator

Note that the required permissions depend on the type of operation, not on
what it does. For example, even though the PutItem operation can read the
value of an item (when used with `ReturnValues=ALL_OLD`), it still requires
the MODIFY permission, not the SELECT permission.

Permissions are separate for a base table and each of its GSI/LSI and CDC
log, so it's possible to give a role permissions to read one index and
not the base, or vice versa, and so on. To build the GRANT command, you
need to know the CQL name of each of these objects. For example, the CQL
name of GSI "abc" of Alternator table "xyz" is `alternator_xyz.xyz:abc`.
If you don't know the name of the table, you can try a forbidden operation
and the AccessDeniedException error will contain the name of the table
that was lacking permissions.

## Workload Isolation

In DynamoDB read/write capacity of each table can be defined either to a fixed
value (provisioned mode) or to be adaptive (on-demand). On top of that requests
are also subject to per table and per account quotas.

Due to the nature of Alternator deployment the whole cluster is available to serve
user requests and underlying hardware can be utilized to its full capacity. When
there is a need to allow more resources to given workload on the expense of some competing
one we offer feature called **Workload Prioritization**.

To use this feature define service level with a fixed amount of shares
(higher value means proportionally more capacity) and attach it to a role
which then will be used to authorize requests. This can be currently done
only via CQL API, here is an example on how to do that:
```cql
CREATE ROLE alice WITH PASSWORD = 'abcd' AND LOGIN = true;
CREATE ROLE bob WITH PASSWORD = 'abcd' AND LOGIN = true;

CREATE SERVICE_LEVEL IF NOT EXISTS olap WITH SHARES = 100;
CREATE SERVICE_LEVEL IF NOT EXISTS oltp WITH SHARES = 1000;

ATTACH SERVICE_LEVEL olap TO alice;
ATTACH SERVICE_LEVEL oltp TO bob;
```
Note that `alternator_enforce_authorization` has to be enabled in Scylla configuration.

See [Authorization](##Authorization) section to learn more about roles and authorization.
See [Workload Prioritization](../features/workload-prioritization)
to read about Workload Prioritization in detail.

## Metrics

Scylla has an advanced and extensive monitoring framework for inspecting
and graphing hundreds of different metrics of Scylla's usage and performance.
Scylla's monitoring stack, based on Grafana and Prometheus, is described in
<https://docs.scylladb.com/operating-scylla/monitoring/>.
This monitoring stack is different from DynamoDB's offering - but Scylla's
is significantly more powerful and gives the user better insights on
the internals of the database and its performance.

## Time To Live (TTL)

Like in DynamoDB, Alternator items which are set to expire at a certain
time will not disappear exactly at that time, but only after some delay.
DynamoDB guarantees that the expiration delay will be less than 48 hours
(though for small tables the delay is often much shorter).

In Alternator, the expiration delay is configurable - it can be set
with the `--alternator-ttl-period-in-seconds` configuration option.
The default is 24 hours.

One thing the implementation is missing is that expiration
events appear in the Streams API as normal deletions - without the
distinctive marker on deletions which are really expirations.
See <https://github.com/scylladb/scylla/issues/5060>.

## Scan ordering

In DynamoDB, scanning the _entire_ table returns the partitions sorted by
some undocumented hash function of the partition key - which is why this key
is also sometimes called the _hash key_. Alternator uses a different hash
function, Cassandra's variant of the 128-bit Mumur3 hash function.
So `Scan`ing the same data on DynamoDB and Alternator will return the same
data in different partition order. Applications mustn't rely on that
undocumented order.

Note that inside each partition, the individual items will be sorted the same
in DynamoDB and Scylla - determined by the _sort key_ defined for that table.

---

## Configurable or different limits

Some features have fixed limits in DynamoDB, but the limit does not exist,
is different, or can be configured in Alternator:

* DynamoDB limits each BatchWriteItem request to 25 items. In Alternator,
  this limit defaults to 100 but can be changed with 
  the `alternator_max_items_in_batch_write` configuration parameter.

* DynamoDB limits the name of tables, GSIs and LSIs, to 255 characters each.
  In Alternator, the limit is different:
    * A table's name is limited to 192 characters.
    * For a GSI, the sum of the length of the table name and the GSI name,
      plus one, is limited to 222 characters.
    * For an LSI, the sum of the length of the table name and the LSI name,
      plus two, is limited to 222 characters.
  So for example, if you create a table whose name is 192 characters, you
  can't create a GSI whose name is longer than 29 characters.

## Experimental API features

Some DynamoDB API features are supported by Alternator, but considered
**experimental** in this release. An experimental feature in Scylla is a
feature whose functionality is complete, or mostly complete, but it is not
as thoroughly tested or optimized as regular features. Also, an experimental
feature's implementation is still subject to change and upgrades may not be
possible if such a feature is used. For these reasons, experimental features
are not recommended for mission-critical uses, and they need to be
individually enabled with the "--experimental-features" configuration option.
See [Enabling Experimental Features](../operating-scylla/admin.rst#enabling-experimental-features) for details.

In this release, the following DynamoDB API features are considered
experimental:

* The DynamoDB Streams API for capturing change is supported, but still
  considered experimental so needs to be enabled explicitly with the
  `--experimental-features=alternator-streams` configuration option.

  Alternator streams also differ in some respects from DynamoDB Streams:
  * The number of separate "shards" in Alternator's streams is significantly
    larger than is typical on DynamoDB.
    <https://github.com/scylladb/scylla/issues/13080>
  * While in DynamoDB data usually appears in the stream less than a second
    after it was written, in Alternator Streams there is currently a 10
    second delay by default.
    <https://github.com/scylladb/scylla/issues/6929>
  * Some events are represented differently in Alternator Streams. For
    example, a single PutItem is represented by a REMOVE + MODIFY event,
    instead of just a single MODIFY or INSERT.
    <https://github.com/scylladb/scylla/issues/6930>
    <https://github.com/scylladb/scylla/issues/6918>
  * The optional ShardFilter parameter to DescribeStream, added to DynamoDB
    in July 2025 to optimize shard discovery, is not yet implemented in
    Alternator.
    <https://github.com/scylladb/scylla/issues/25160>

## Unimplemented API features

In general, every DynamoDB API feature available in Amazon DynamoDB should
behave the same in Alternator. However, there are a few features which we have
not implemented yet. Unimplemented features return an error when used, so
they should be easy to detect. Here is a list of these unimplemented features:

* GSI (Global Secondary Index) and LSI (Local Secondary Index) may be
  configured to project only a subset of the base-table attributes to the
  index. This option is not yet respected by Alternator - all attributes
  are projected. This wastes some disk space when it is not needed.
  <https://github.com/scylladb/scylla/issues/5036>

* DynamoDB's multi-item transaction feature (TransactWriteItems,
  TransactGetItems) is not supported. Note that the older single-item
  conditional updates feature are fully supported.
  This feature was added to DynamoDB in November 2018.
  <https://github.com/scylladb/scylla/issues/5064>

* Alternator does not yet support the DynamoDB API calls that control which
  table is available in which data center (DC): CreateGlobalTable,
  UpdateGlobalTable, DescribeGlobalTable, ListGlobalTables,
  UpdateGlobalTableSettings, DescribeGlobalTableSettings, and UpdateTable.
  Currently, *all* Alternator tables are created as global tables and can
  be accessed from all the DCs existing at the time of the table's creation.
  If a DC is added after a table is created, the table won't be visible from
  the new DC and changing that requires a CQL "ALTER TABLE" statement to
  modify the table's replication strategy.
  <https://github.com/scylladb/scylla/issues/5062>

* Recently DynamoDB added support, in addition to the DynamoDB Streams API,
  also for the similar Kinesis Streams. Alternator doesn't support this yet,
  and the related operations DescribeKinesisStreamingDestination,
  DisableKinesisStreamingDestination, and EnableKinesisStreamingDestination.
  This feature was added to DynamoDB in November 2020.
  <https://github.com/scylladb/scylla/issues/8786>

* The on-demand backup APIs are not supported: CreateBackup, DescribeBackup,
  DeleteBackup, ListBackups, RestoreTableFromBackup.
  For now, users can use Scylla's existing backup solutions such as snapshots
  or Scylla Manager.
  <https://github.com/scylladb/scylla/issues/5063>

* Continuous backup (the ability to restore any point in time) is also not
  supported: UpdateContinuousBackups, DescribeContinuousBackups,
  RestoreTableToPointInTime

* DynamoDB's encryption-at-rest settings are not supported. The Encryption-
  at-rest feature is available in ScyllaDB, but needs to be
  enabled and configured separately, not through the DynamoDB API.

* No support for throughput accounting or capping. As mentioned above, the
  BillingMode option is ignored by Alternator, and if a provisioned throughput
  is specified, it is ignored. Requests which are asked to return the amount
  of provisioned throughput used by the request do not return it in Alternator.
  <https://github.com/scylladb/scylla/issues/5068>

* DAX (DynamoDB Accelerator), an in-memory cache for DynamoDB, is not
  available in for Alternator. Anyway, it should not be necessary - Scylla's
  internal cache is already rather advanced and there is no need to place
  another cache in front of the it. We wrote more about this here:
  <https://www.scylladb.com/2017/07/31/database-caches-not-good/>

* The DescribeTable is missing information about size estimates, and 
  also part of the information about indexes enabled on the table.
  <https://github.com/scylladb/scylla/issues/5320>
  <https://github.com/scylladb/scylla/issues/7550>
  <https://github.com/scylladb/scylla/issues/7551>

* The PartiQL syntax (SQL-like SELECT/UPDATE/INSERT/DELETE expressions)
  and the operations ExecuteStatement, BatchExecuteStatement and
  ExecuteTransaction are not yet supported.
  A user that is interested in an SQL-like syntax can consider using Scylla's
  CQL protocol instead.
  This feature was added to DynamoDB in November 2020.
  <https://github.com/scylladb/scylla/issues/8787>

* As mentioned above, Alternator has its own powerful monitoring framework,
  which is different from AWS's. In particular, the operations
  DescribeContributorInsights, ListContributorInsights and
  UpdateContributorInsights that configure Amazon's "CloudWatch Contributor
  Insights" are not yet supported. Scylla has different ways to retrieve the
  same information, such as which items were accessed most often.
  <https://github.com/scylladb/scylla/issues/8788>

* Alternator does not support the DynamoDB feature "export to S3",
  and its operations DescribeExport, ExportTableToPointInTime, ListExports.
  This feature was added to DynamoDB in November 2020.
  <https://github.com/scylladb/scylla/issues/8789>

* Alternator does not support the DynamoDB feature "import from S3",
  and its operations ImportTable, DescribeImport, ListImports.
  This feature was added to DynamoDB in August 2022.
  <https://github.com/scylladb/scylla/issues/11739>

* Alternator does not support the TableClass table option choosing between
  several storage options with different cost/performance characteristics.
  All Alternator tables are stored the same way. This table option was added
  to DynamoDB in December 2021.
  <https://github.com/scylladb/scylla/issues/10431>

* Alternator does not support the table option DeletionProtectionEnabled
  that can be used to forbid table deletion. This table option was added to
  DynamoDB in March 2023.
  <https://github.com/scylladb/scylla/issues/14482>

* Alternator does not support the table option WarmThroughput that can be
  used to check or guarantee that the database has "warmed" to handle a
  particular throughput. This table option was added to DynamoDB in
  November 2024.
  <https://github.com/scylladb/scylladb/issues/21853>

* Alternator does not support the table option MultiRegionConsistency
  that can be used to achieve consistent reads on global (multi-region) tables.
  This table option was added as a preview to DynamoDB in December 2024.
  <https://github.com/scylladb/scylladb/issues/21852>
