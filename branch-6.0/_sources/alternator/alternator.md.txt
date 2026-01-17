# Alternator: DynamoDB API in Scylla

## Introduction
Alternator is a Scylla feature adding compatibility with Amazon DynamoDB(TM).
DynamoDB's API uses JSON-encoded requests and responses which are sent over
an HTTP or HTTPS transport. It is described in detail in Amazon's [DynamoDB
API Reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/).

Our goal is that any application written to use Amazon DynamoDB could
be run, unmodified, against Scylla with Alternator enabled. Alternator's
compatibility with DynamoDB is fairly complete, but users should be aware
of some differences and some unimplemented features. The extent of
Alternator's compatibility with DynamoDB is described in the
[Scylla Alternator for DynamoDB users](compatibility.md) document,
which is updated as the work on Alternator progresses and compatibility
continues to improve.

## Running Alternator
By default, Scylla does not listen for DynamoDB API requests. To enable
this API in Scylla you must set at least two configuration options,
**alternator_port** and **alternator_write_isolation**. For example in the
YAML configuration file:
```yaml
alternator_port: 8000
alternator_write_isolation: only_rmw_uses_lwt # or always, forbid or unsafe
```
or, equivalently, via command-line arguments: `--alternator-port=8000
--alternator-write-isolation=only_rmw_uses_lwt.

the **alternator_port** option determines on which port Scylla listens for
DynamoDB API requests. By default, it listens on this port on all network
interfaces. To listen only on a specific interface, configure also the
**alternator_address** option.

The meaning of the **alternator_write_isolation** option is explained in detail
in the "Write isolation policies" below. Alternator has four different choices
for the implementation of writes, each with different advantages. You should
carefully consider which of the options makes more sense for your intended
use case and configure alternator_write_isolation accordingly. There is
currently no default for this option: Trying to run Scylla with an Alternator
port selected but without configuring write isolation will result in an error message,
asking you to set it.

In addition to (or instead of) serving HTTP requests on alternator_port,
Scylla can accept DynamoDB API requests over HTTPS (encrypted), on the port
specified by **alternator_https_port**. As usual for HTTPS servers, the
operator must specify certificate and key files. By default these should
be placed in `/etc/scylla/scylla.crt` and `/etc/scylla/scylla.key`, but
these default locations can overridden by specifying
`--alternator-encryption-options keyfile="..."` and
`--alternator-encryption-options certificate="..."`.

By default, Scylla saves a snapshot of deleted tables. But Alternator does
not offer an API to restore these snapshots, so these snapshots are not useful
and waste disk space - deleting a table does not recover any disk space.
It is therefore recommended to disable this automatic-snapshotting feature
by configuring the **auto_snapshot** option to `false`.
See also <https://github.com/scylladb/scylladb/issues/5283>.

DynamoDB applications specify a single "endpoint" address, e.g.,
`dynamodb.us-east-1.amazonaws.com`. Behind the scenes, a DNS server and/or
load balancers distribute the connections to many different backend nodes.
Alternator does not provide such a load-balancing setup, so you should
either set one up, or set up the client library to do the load balancing
itself. Instructions, code and examples for doing this can be found in the
[Alternator Load Balancing project](https://github.com/scylladb/alternator-load-balancing/).

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
<https://docs.google.com/document/d/1i4yjF5OSAazAY_-T8CBce9-2ykW4twx_E_Nt2zDoOVs>

Almost all of Alternator's source code (except some initialization code)
can be found in the alternator/ subdirectory of Scylla's source code.
Extensive functional tests can be found in the test/alternator
subdirectory. These tests are written in Python, and can be run against
both Alternator and Amazon's DynamoDB; This allows verifying that
Alternator's behavior matches the one observed on DynamoDB.
See test/alternator/README.md for more information about the tests and
how to run them.

With Alternator enabled on port 8000 (for example), every Scylla node
listens for DynamoDB API requests on this port. These requests, in
JSON format over HTTP, are parsed and result in calls to internal Scylla
C++ functions - there is no CQL generation or parsing involved.
In Scylla terminology, the node receiving the request acts as the the
*coordinator*, and often passes the request on to one or more other nodes -
*replicas* which hold copies of the requested data.

Alternator tables are stored as Scylla tables, each in a separate keyspace.
Each keyspace is initialized when the corresponding Alternator table is
created (with a CreateTable request). The replication factor (RF) for this
keyspace is chosen at that point, depending on the size of the cluster:
RF=3 is used on clusters with three or more nodes, and RF=1 is used for
smaller clusters. Such smaller clusters are, of course, only recommended
for tests because of the risk of data loss.

Each table in Alternator is stored as a Scylla table in a separate
keyspace. The DynamoDB key columns (hash and sort key) have known types,
and become partition and clustering key columns of the Scylla table.
All other attributes may be different for each row, so are stored in one
map column in Scylla, and not as separate columns.

DynamoDB supports two consistency levels for reads, "eventual consistency"
and "strong consistency". These two modes are implemented using Scylla's CL
(consistency level) feature: All writes are done using the `LOCAL_QUORUM`
consistency level, then strongly-consistent reads are done with
`LOCAL_QUORUM`, while eventually-consistent reads are with just `LOCAL_ONE`.

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
attribute as one JSON object. This means that UpdateItem requests which
want modify a non-top-level attribute directly (e.g., a.b[3].c) need RMW:
Alternator implements such requests by reading the entire top-level
attribute a, modifying only a.b[3].c, and then writing back a.

```{eval-rst}
.. toctree::
    :maxdepth: 2

    getting-started
    compatibility
```
