# Alternator: DynamoDB API in Scylla

## Introduction
Alternator is a Scylla feature adding compatibility with Amazon DynamoDB(TM).
DynamoDB's API uses JSON-encoded requests and responses which are sent over
an HTTP or HTTPS transport. It is described in detail on Amazon's site:
  https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/

Our goal is that any application written to use Amazon DynamoDB could
be run, unmodified, against Scylla with Alternator enabled. However, at this
stage the Alternator implementation is incomplete, and some of DynamoDB's
API features are not yet supported. The following section documents the
extent of Alternator's compatibility with DynamoDB, and will be updated
as the work progresses and compatibility continues to improve.

## Current compatibility with DynamoDB

### API Server
* Transport: HTTP mostly supported, but small features like CRC header and
  compression are still missing. HTTPS not tested.
* Authorization (verifying the originator of the request): Not yet supported.
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
  Filter expressions (to filter some of the items) partially supported.
  The "Select" options which allows to count items instead of returning them
  is not yet supported. Parallel scan is not yet supported.
* Query: Same issues as Scan above. Additionally, missing support for
  KeyConditionExpression (an alternative syntax replacing the older
  KeyConditions parameter which we do support).
### Secondary Indexes
* Global Secondary Indexes (GSI): Not yet supported.
* Local Secondary Indexes (LSI): Not yet supported.
### Time To Live (TTL)
* Not yet supported. Note that this is a different feature from Scylla's
  feature with the same name.
### Replication (one availability zone)
* Most of the code is already correct, including writes  done in LOCAL_QURUM
  and reads in LOCAL_ONE (eventual consistency) or LOCAL_QUORUM (strong
  consistency). However, executor::start() currently creates a keyspace with
  just RF=1 and no concern for racks or number of nodes. This should be
  fixed.
### Global Tables
* Not yet supported: CreateGlobalTable, UpdateGlobalTable,
  DescribeGlobalTable, ListGlobalTables, UpdateGlobalTableSettings,
  DescribeGlobalTableSettings. Implementation will use Scylla's multi-DC
  features.
### Backup and Restore
* On-demand backup: Not yet supported: CreateBackup, DescribeBackup,
  DeleteBackup, ListBackups, RestoreTableFromBackup.
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
* Not yet supported
### Metrics
* Several metrics are available internally but need more and make them
  more similar to what AWS users are used to.
