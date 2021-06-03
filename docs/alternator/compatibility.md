# Scylla Alternator for DynamoDB users

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

When creating a table, the BillingMode and ProvisionedThroughput options
are ignored by Scylla.

## Load balancing

DynamoDB applications specify a single "endpoint" address, e.g.,
`dynamodb.us-east-1.amazonaws.com`. Amazon's cloud service distributes
request for this single URL to many different backend nodes. Such a
load-balancing setup is *not* included inside Alternator. You should either
set one up, or configure the client library to do the load balancing itself.
Instructions for doing this can be found in:
https://github.com/scylladb/alternator-load-balancing/

## Write isolation policies

Scylla was designed to optimize the performance of pure write operations -
writes which do not need to read the the previous value of the item.
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
alternator.md. One of the options, `always_use_lwt`, is always safe, but the
other options result in significantly better write performance and should be
considered when the workload involves pure writes (e.g., ingestion of new
data) or if pure writes and read-modify-writes go to distinct items.

## Authorization

Alternator implements the same [signature protocol](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
as DynamoDB and the rest of AWS. Clients use, as usual, an access key ID and
a secret access key to prove their identity and the authenticity of their
request. Alternator can then validate the authenticity and authorization of
each request using a known list of authorized key pairs.

In the current implementation, the user stores the list of allowed key pairs
in the `system_auth.roles` table: The access key ID is the `role` column, and
the secret key is the `salted_hash`, i.e., the secret key can be found by
`SELECT salted_hash from system_auth.roles WHERE role = ID;`.

By default, authorization is not enforced at all. It can be turned on
by providing an entry in Scylla configuration:
    `alternator_enforce_authorization: true`

Although Alternator implements DynamoDB's authentication, including the
possibility of listing multiple allowed key pairs, there is currently no
implementation of _access control_. All authenticated key pairs currently get
full access to the entire database. This is in contrast with DynamoDB which
supports fine-grain access controls via "IAM policies" - which give each
authenticated user access to a different subset of the tables, different
allowed operations, and even different permissions for individual items.
All of this is not yet implemented in Alternator.

### Metrics

Scylla has an advanced and extensive monitoring framework for inspecting
and graphing hundreds of different metrics of Scylla's usage and performance.
Scylla's monitoring stack, based on Grafana and Prometheus, is described in
https://docs.scylladb.com/operating-scylla/monitoring/.
This monitoring stack is different from DynamoDB's offering - but Scylla's
is significantly more powerful and gives the user better insights on
the internals of the database and its performance.

## Unimplemented API features

In general, every DynamoDB API feature available in Amazon DynamoDB should
behave the same in Alternator. However, there are a few features which we have
not implemented yet. Unimplemented features return an error when used, so
they should be easy to detect. Here is a list of these unimplemented features:

* Currently in Alternator, a GSI (Global Secondary Index) can only be added
  to a table at table creation time. Unlike DynamoDB which also allows adding
  a GSI (but not an LSI) to an existing table using an UpdateTable operation.
  https://github.com/scylladb/scylla/issues/5022

* GSI (Global Secondary Index) and LSI (Local Secondary Index) may be
  configured to project only a subset of the base-table attributes to the
  index. This option is not yet respected by Alternator - all attributes
  are projected. This wastes some disk space when it is not needed.
  https://github.com/scylladb/scylla/issues/5036

* DynamoDB's TTL (per-item expiration) feature is not supported. Note that
  this is a different feature from Scylla's feature with the same name.
  https://github.com/scylladb/scylla/issues/5060

* DynamoDB's new multi-item transaction feature (TransactWriteItems,
  TransactGetItems) is not supported. Note that the older single-item
  conditional updates feature are fully supported.
  https://github.com/scylladb/scylla/issues/5064

* The "Select" option of Scan and Query operations, which allows to only
  read parts of items or to just count them, is not yet supported.
  https://github.com/scylladb/scylla/issues/5058

* The ScanIndexForward=false of the Query operation - to read a partition
  in reverse order - is currently implemented inefficiently, and limited
  by default to partitions of size 100 MB.
  https://github.com/scylladb/scylla/issues/7586

* Alternator does not yet support the DynamoDB API calls that control which
  table is available in which data center (DC): CreateGlobalTable,
  UpdateGlobalTable, DescribeGlobalTable, ListGlobalTables,
  UpdateGlobalTableSettings, DescribeGlobalTableSettings, and UpdateTable.
  Currently, *all* Alternator tables are created as global tables and can
  be accessed from all the DCs existing at the time of the table's creation.
  If a DC is added after a table is created, the table won't be visible from
  the new DC and changing that requires a CQL "ALTER TABLE" statement to
  modify the table's replication strategy.
  https://github.com/scylladb/scylla/issues/5062

* The DynamoDB Streams API for capturing change is supported, but still
  considered experimental so needs to be enabled explicitly with the
  `--experimental-features=alternator-streams` configuration option.
  Alternator streams also differ in some respects from DynamoDB Streams:
  * The number of separate "shards" in Alternator's streams is significantly
    larger than is typical on DynamoDB.
  * While in DynamoDB data usually appears in the stream less than a second
    after it was written, in Alternator Streams there is currently a 10
    second delay by default.
    https://github.com/scylladb/scylla/issues/6929
  * Some events are represented differently in Alternator Streams. For
    example, a single PutItem is represented by a REMOVE + MODIFY event,
    instead of just a single MODIFY or INSERT.
    https://github.com/scylladb/scylla/issues/6930
    https://github.com/scylladb/scylla/issues/6918

* Recently DynamoDB added support, in addition to the DynamoDB Streams API,
  also for the similar Kinesis Streams. Alternator doesn't support this yet,
  and the related operations DescribeKinesisStreamingDestination,
  DisableKinesisStreamingDestination, and EnableKinesisStreamingDestination.
  https://github.com/scylladb/scylla/issues/8786

* The on-demand backup APIs are not supported: CreateBackup, DescribeBackup,
  DeleteBackup, ListBackups, RestoreTableFromBackup.
  For now, users can use Scylla's existing backup solutions such as snapshots
  or Scylla Manager.
  https://github.com/scylladb/scylla/issues/5063

* Continuous backup (the ability to restore any point in time) is also not
  supported: UpdateContinuousBackups, DescribeContinuousBackups,
  RestoreTableToPoinInTime

* DynamoDB's encryption-at-rest settings are not supported. The Encryption-
  at-rest feature is available in Scylla Enterprise, but needs to be
  enabled and configured separately, not through the DynamoDB API.

* No support for throughput accounting or capping. As mentioned above, the
  BillingMode option is ignored by Alternator, and if a provisioned throughput
  is specified, it is ignored. Requests which are asked to return the amount
  of provisioned throughput used by the request do not return it in Alternator.
  https://github.com/scylladb/scylla/issues/5068

* DAX (DynamoDB Accelerator), an in-memory cache for DynamoDB, is not
  available in for Alternator. Anyway, it should not be necessary - Scylla's
  internal cache is already rather advanced and there is no need to place
  another cache in front of the it. We wrote more about this here:
  https://www.scylladb.com/2017/07/31/database-caches-not-good/

* The DescribeTable is missing information about creation data and size
  estimates, and also part of the information about indexes enabled on 
  the table.
  https://github.com/scylladb/scylla/issues/5013
  https://github.com/scylladb/scylla/issues/5026
  https://github.com/scylladb/scylla/issues/7550
  https://github.com/scylladb/scylla/issues/7551 

* The recently-added PartiQL syntax (SQL-like SELECT/UPDATE/INSERT/DELETE
  expressions) and the new operations ExecuteStatement, BatchExecuteStatement
  and ExecuteTransaction is not yet supported.
  A user that is interested in an SQL-like syntax can consider using Scylla's
  CQL protocol instead.
  https://github.com/scylladb/scylla/issues/8787

* As mentioned above, Alternator has its own powerful monitoring framework,
  which is different from AWS's. In particular, the operations
  DescribeContributorInsights, ListContributorInsights and
  UpdateContributorInsights that configure Amazon's "CloudWatch Contributor
  Insights" are not yet supported. Scylla has different ways to retrieve the
  same information, such as which items were accessed most often.
  https://github.com/scylladb/scylla/issues/8788

* Alternator does not support the new DynamoDB feature "export to S3",
  and its operations DescribeExport, ExportTableToPointInTime, ListExports.
  https://github.com/scylladb/scylla/issues/8789
