# Alternator-specific APIs

Alternator's primary goal is to be compatible with Amazon DynamoDB(TM)
and its APIs, so that any application written to use Amazon DynamoDB could
be run, unmodified, against ScyllaDB with Alternator enabled. The extent of
Alternator's compatibility with DynamoDB is described in the
[ScyllaDB Alternator for DynamoDB users](compatibility.md) document.

But Alternator also adds several features and APIs that are not available in
DynamoDB. These Alternator-specific APIs are documented here.

## Request timeout

Alternator uses `alternator_timeout_in_ms` as the default timeout for backend
operations. A client can override this timeout for an individual request with
the `X-Scylla-Timeout` HTTP header on either HTTP or HTTPS requests.

The override applies to `GetItem`, `BatchGetItem`, `Query`, `Scan`, `PutItem`,
`UpdateItem`, `DeleteItem`, `BatchWriteItem`, `GetRecords`, and `SearchVectors`.
Other APIs, including table/schema changes, tags, TTL settings, stream
configuration and export jobs, validate the header but ignore a valid value.

For example:

```text
X-Scylla-Timeout: 30s
```

If the header is absent, `alternator_timeout_in_ms` retains its existing
behavior. The header can increase or decrease the configured timeout. Its name
is case-insensitive. Values use CQL duration syntax, such as `500ms`, `5s`, or
`1m30s`, and must be non-negative with millisecond granularity, at most
`4294967295ms` (the largest unsigned 32-bit millisecond value). Days and months
are not accepted, matching CQL's `USING TIMEOUT`. Invalid values return
`ValidationException`. A zero timeout expires immediately.

With a header, backend work shares one deadline measured from the start of HTTP
request handling. Query and Scan, batch operations, read-before-write
operations, and LWTs reuse this deadline instead of starting a fresh timeout at
each step. The deadline also follows requests between shards. A completed
operation keeps its successful response even if response processing crosses the
deadline. A preimage read for a stream with old or new images still has CDC's
10-second limit, so a longer header does not extend that read.

Expired operations return HTTP 500 `InternalServerError`, the same error class
as existing Alternator backend timeouts. A write timeout does not prove that the
write failed: it may already have reached replicas or committed through LWT.
The header does not roll back writes. Retrying an increment or another
non-idempotent operation after an uncertain result can apply it more than once.
`BatchGetItem` can return successful reads together with `UnprocessedKeys` for
timed-out reads; retry the unprocessed keys.

The interval starts when the API handler receives the request headers, before
waiting for request memory, reading and parsing the body, and authentication.
It excludes connection establishment and TLS negotiation. The header is parsed
after authentication, but its deadline uses that original handler start time.

This is a backend budget, not a bound on HTTP response latency. Memory
admission, body reception and parsing, signature verification (including
fetching the signing key), and permission checks retain their existing wait
policies. These waits are not interrupted by the header. Their elapsed time
reduces the budget available to backend work. If the deadline has already
expired when dispatching the operation, the server returns a timeout without
dispatching it. Backend work receives the same deadline after later permission
checks.

The header does not persist on the HTTP connection. To configure a Boto3 client
default once in application code, register a hook that adds the header to each
request before signing:

```python
def set_timeout(request, **kwargs):
    if 'X-Scylla-Timeout' not in request.headers:
        request.headers['X-Scylla-Timeout'] = '5s'

service = client.meta.service_model.service_id.hyphenize()
client.meta.events.register(f'before-sign.{service}', set_timeout)
```

Register this hook on each client that needs the default. The signing event
uses `dynamodb` for DynamoDB clients and `dynamodb-streams` for DynamoDB Streams
clients.

This server timeout is separate from the SDK's socket timeout and retry budget;
an SDK retry is another HTTP request with its own deadline. SDKs can retry HTTP
500 errors automatically, so the total call can take much longer than the header
interval. To observe one attempt in Boto3, use
`Config(retries={'total_max_attempts': 1})`. Choose a socket read timeout longer
than the server budget; a shorter socket timeout can leave the outcome unknown.

Client-side header optimization can remove `X-Scylla-Timeout` before it reaches
the server. Configure it to preserve the header, or disable it for affected
clients. Removing a header after signing can also invalidate the signature.
The following settings disable filtering or extend its allowlist:

| Client | Setting |
| --- | --- |
| Rust | `AlternatorConfig::builder().optimize_headers(false)` |
| Go | `shared.WithOptimizeHeaders(false)`, or a `shared.WithCustomOptimizeHeaders` callback that retains `X-Scylla-Timeout` along with the required headers |
| JavaScript | `headerOptimization: false`, or `headerOptimization: { additionalAllowedHeaders: ['X-Scylla-Timeout'] }` |

Check the options supported by the client version in use. In particular, Rust's
optimizer is enabled by default; versions that retain signed headers can still
strip a custom header from unsigned requests. Any other client or proxy using a
header allowlist must preserve this header too.
During a rolling upgrade, older nodes can ignore the extension, so use endpoints
on upgraded nodes when relying on the deadline.

## Vector search extensions
Alternator implements Amazon DynamoDB's own Vector Search API
(`VectorIndexes`, `VectorIndexUpdates`, `SearchVectors`), so this core
functionality is *not* Alternator-specific and is documented separately in
[Alternator Vector Search](vector-search.md). That document does, however,
also cover the handful of genuine ScyllaDB-only extensions layered on top
of the standard API - most notably the `FLOAT32VECTOR` attribute type, a
more compact representation for vectors than DynamoDB's own encoding, and
the `BaseRead`/`FilterExpression` parameters of `SearchVectors` for reading
data straight from the base table when needed.

## Write isolation policies
DynamoDB API update requests may involve a read before the write - e.g., a
_conditional_ update or an update based on the old value of an attribute.
The read and the write should be treated as a single transaction - protected
(_isolated_) from other parallel writes to the same item.

Alternator could do this isolation by using ScyllaDB's LWT (lightweight
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
    without a read - are performed using standard ScyllaDB writes, not LWT,
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

## Accessing system tables from ScyllaDB
ScyllaDB exposes lots of useful information via its internal system tables,
which can be found in system keyspaces: 'system', 'system\_auth', etc.
In order to access to these tables via alternator interface,
Scan and Query requests can use a special table name:
`.scylla.alternator.KEYSPACE_NAME.TABLE_NAME`
which will return results fetched from corresponding ScyllaDB table.

This interface can be used only to fetch data from system tables.
Attempts to read regular tables via the virtual interface will result
in an error.

Example: in order to query the contents of ScyllaDB's `system.large_rows`,
pass `TableName='.scylla.alternator.system.large_rows'` to a Query/Scan
request.

Note that currently only `Scan` and `Query` on system tables is supported -
`GetItem` is not (so use `Query` even to read a single item).

If the `alternator_allow_system_table_write` configuration option is set to
true (by default, it is false), system tables can also be written to. This
can be useful for, for example, modifying configuration options. Even when
writing system tables is enabled, the role sending the command must be a
superuser or the write will be denied.

### Listing ongoing requests
One useful system table to read is `.scylla.alternator.system.clients`,
which lists the currently active Alternator clients. Reading from this
virtual table produces an item for each request currently being handled.
Each item has the following attributes:

  * `client_type` is always `alternator` for Alternator requests.
  * `address` and `port` say where the request came from.
  * `ssl_enabled` is `true` for an HTTPS request, `false` for HTTP.
  * `shard_id` is the shard (CPU core) handling this request on the server.
  * `driver_name` is the User-Agent HTTP header sent by the driver.
     This string usually begins with the driver's name and version, such
     as `Boto3/1.38.46`, `aws-sdk-java/1.11.919` or `aws-sdk-java/2.25.31`,
     and followed by additional information sent by the driver.
  * `username` is the username used to sign this request.
  * `scheduling_group` is the scheduling group handling this request.

The same system table also lists CQL connections if there are any - those
have `client_type` set to CQL. Note that for CQL, each item describes a
connection (either active or idle), not necessarily an active request as
in Alternator.

## Service discovery
As explained in [ScyllaDB Alternator for DynamoDB users](compatibility.md),
Alternator requires a load-balancer or a client-side load-balancing library
to distribute requests between all ScyllaDB nodes. This load-balancer needs
to be able to _discover_ the ScyllaDB nodes. Alternator provides two special
requests, `/` and `/localnodes`, to help with this service discovery, which
we will now explain.

Some setups know exactly which ScyllaDB nodes were brought up, so all that
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

In other setups, the load balancer might not know which ScyllaDB nodes exist.
For example, it may be possible to add or remove ScyllaDB nodes without a
client-side load balancer knowing. For these setups we have the `/localnodes`
request that can be used to discover which ScyllaDB nodes exist: A load balancer
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
  nodes in a specific ScyllaDB data center, not the data center of the node
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

## Tablets
"Tablets" are ScyllaDB's new approach to replicating data across a cluster.
It replaces the older approach which was named "vnodes". See
[Data Distribution with Tablets](../architecture/tablets.rst) for details.

In this version, tablet support is complete, so new
Alternator tables default to following what the global configuration flag
[tablets_mode_for_new_keyspaces](../reference/configuration-parameters.rst#confval-tablets_mode_for_new_keyspaces)
tells them to.

If you want to influence whether a specific Alternator table is created with tablets or vnodes,
you can do this by specifying the `system:initial_tablets` tag
(in earlier versions of ScyllaDB the tag was `experimental:initial_tablets`)
in the CreateTable operation. The value of this tag can be:

* Any valid integer as the value of this tag enables tablets.
  Typically the number "0" is used - which tells ScyllaDB to pick a reasonable
  number of initial tablets. But any other number can be used, and this
  number overrides the default choice of initial number of tablets.

* Any non-integer value - e.g., the string "none" - creates the table
  without tablets - i.e., using vnodes. However, when vnodes are asked for by the tag value,
  but tablets are `enforced` by the `tablets_mode_for_new_keyspaces` configuration flag,
  an exception will be thrown.

The `system:initial_tablets` tag only has any effect while creating
a new table with CreateTable - changing it later has no effect.

