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

## Service discovery
As explained in [Scylla Alternator for DynamoDB users](compatibility.md),
Alternator requires a load-balancer or a client-side load-balancing library
to distribute requests between all Scylla nodes. This load-balancer needs
to be able to _discover_ the Scylla nodes. Alternator provides two special
requests, `/` and `/localnodes`, to help with this service discovery, which
we will now explain.

Some setups know exactly which Scylla nodes were brought up, so all that
remains is to periodically verify that each node is still functional. The
easiest way to do this is to make an HTTP (or HTTPS) GET request to the node,
with URL `/`. This is a trivial GET request and does **not** need to be
authenticated like other DynamoDB API requests. Note that Amazon DynamoDB
also supports this unauthenticated `/` request.

For example:
```
$ curl http://localhost:8000/
healthy: localhost:8000
```

In other setups, the load balancer might not know which Scylla nodes exist.
For example, it may be possible to add or remove Scylla nodes without a
client-side load balancer knowing. For these setups we have the `/localnodes`
request that can be used to discover which Scylla nodes exist: A load balancer
that already knows at least one live node can discover the rest by sending
a `/localnodes` request to the known node. It's again an unauthenticated
HTTP (or HTTPS) GET request:

```
$ curl http://localhost:8000/localnodes
["127.0.0.1","127.0.0.2"]
```

The response is a list of all functioning nodes in this data center, as a
list of IP addresses in JSON format. Note that these are just IP addresses,
not full URLs - they do not include the protocol and the port number.

This request is called "localnodes" because it returns the _local_ nodes -
the nodes in the same data center as the known node. This is usually what
we need - we will have a separate load balancer per data center, just like
Amazon DynamoDB has separate endpoint per AWS region.

The `/localnodes` GET request can also take two optional parameters to
list the nodes in a specific _data center_ or _rack_. These options are
useful for certain use cases:

* A `dc` option (e.g., `/localnodes?dc=dc1`) can be passed to list the
  nodes in a specific Scylla data center, not the data center of the node
  being contacted. This is useful when a client knowns of _some_ Scylla
  node belonging to an unknown DC, but wants to list the nodes in _its_
  DC, which it knows by name.

* A `rack` option (e.g., `/localnodes?rack=rack1`) can be passed to list
  only nodes in a given rack instead of an entire data center. This is useful
  when a client in a multi-rack DC (e.g., a multi-AZ region in AWS) wants to
  send requests to nodes in its own rack (which it knows by name), to avoid
  cross-rack networking costs.

Both `dc` and `rack` options can be specified together to list the nodes
of a specific rack in a specific data center: `/localnodes?dc=dc1&rack=rack1`.

If a certain data center or rack has no functional nodes, or doesn't even
exist, an empty list (`[]`) is returned by the `/localnodes` request.
A client should be prepared to consider expanding the node search to an
entire data center, or other data centers, in that case.
