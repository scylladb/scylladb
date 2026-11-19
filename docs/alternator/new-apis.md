# Alternator-specific APIs

Alternator's primary goal is to be compatible with Amazon DynamoDB(TM)
and its APIs, so that any application written to use Amazon DynamoDB could
be run, unmodified, against ScyllaDB with Alternator enabled. The extent of
Alternator's compatibility with DynamoDB is described in the
[ScyllaDB Alternator for DynamoDB users](compatibility.md) document.

But Alternator also adds several features and APIs that are not available in
DynamoDB. These Alternator-specific APIs are documented here.

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


## Tablet routing hints (HTTP protocol 2)

Clients may opt in to tablet-routing V2 metadata on `GetItem`, `PutItem`,
`UpdateItem`, `DeleteItem`, and base-table `Query`. Ordinary tablet-backed
Alternator tables, including LWT writes, return regular placement metadata
without enabling experimental strongly consistent tables. Global Raft-backed
strongly consistent (SC) tables return leader-first SC routing metadata when
their current serving group and leader are available. Such tables require
the existing `strongly-consistent-tables` experimental feature. Local SC
mode is not currently supported by ScyllaDB.

This extension reports routing metadata only. It preserves Alternator's
existing data execution and consistency behavior; it does not dispatch data
operations through the Raft SC coordinator or add SC conditional or
read-modify-write support. `mode=strong` identifies the table's routing
metadata, not a new execution guarantee. `ConsistentRead=True` and LWT on an
ordinary table still use regular routing. Index queries, vnode tables,
scans, and batches are outside this extension's scope.

### Request grammar

`X-Scylla-T-V` is a short alias for `X-Scylla-Tablet-Version`. Both names
accept the same values and are case-insensitive:

```text
X-Scylla-T-V: 2:-
```

```text
X-Scylla-Tablet-Version: 2:-
```

`2:-` requests current routing information without a cached version. A client
with a cached version sends `2:` followed by exactly two hexadecimal digits:

```text
X-Scylla-Tablet-Version: 2:dc
```

The high nibble (`d`, decimal 13) selects one of the 16 four-bit blocks of the
cached 64-bit version. The low nibble (`c`, decimal 12) is that block's expected
value. Blocks are numbered from the least significant nibble (index 0) to the
most significant nibble (index 15). The server returns information only when
the requested block differs from its current version. Uppercase hex is also
accepted.

The prefix is an unsigned decimal protocol number followed by `:`. Leading
zeroes are accepted. Unsupported numeric protocol versions are ignored, and
their suffixes are left uninterpreted. For version 2, the suffix must be `-`
or two hex digits. Surrounding HTTP whitespace (space and tab) is trimmed;
internal whitespace and trailing data are invalid. If neither header name is
present, the extension is disabled; a present empty value is invalid.
Malformed syntax on one of the five operations produces `ValidationException`
before executing it. Other operations ignore the extension, including
malformed values. Header names are case-insensitive. The setting applies to
one request, never to its HTTP connection.

If both names are present, they must specify the same request after parsing.
Equivalent whitespace, hexadecimal case and leading protocol zeroes are
accepted; malformed or conflicting values produce `ValidationException`
before executing the operation. If both values use unsupported protocols,
both are ignored as usual.

Valid requests on unsupported table types or index queries execute normally
and return no routing header. Older servers can ignore the request header.

### Response grammar

A successful eligible operation can include:

```text
X-Scylla-Tablet-Info: 2;start=-1000;end=2000;version=9f32a761bc0051de;mode=regular;replicas="11111111-1111-4111-8111-111111111111/3 22222222-2222-4222-8222-222222222222/1"
```

Fields always appear in this order:

- `2` is the HTTP hint protocol version.
- `start` and `end` are signed decimal token bounds for `(start, end]`, using
  the same conventions as CQL routing V2. The first range starts at
  `-9223372036854775808`.
- `version` is the complete unsigned 64-bit tablet version, exactly 16
  lowercase hexadecimal digits, including leading zeroes.
- `mode=regular` describes ordinary tablet replication, including LWT and
  `ConsistentRead=True` on ordinary tables. `mode=strong` describes global
  Raft SC routing metadata.
- `replicas` is a quoted, space-separated list of canonical node UUIDs and
  unsigned decimal shard numbers, separated by `/`. It preserves the exact
  order of the shared routing V2 implementation. Regular metadata contains
  the complete placement set. SC metadata contains the current serving set,
  including the replicas eligible to serve during tablet transitions, with
  the current Raft leader first. Both retain replicas across data centers.
  There is no separate `leader` field. No addresses or ports are included.

Only successful responses carry this header, including missing-item reads
and empty queries. Authentication, authorization, validation, conditional
check, and backend errors carry none. Buffered, streamed, gzip, and deflate
responses use the same header. CORS exposes it to browser clients.

Hints are advisory: topology can change after metadata is collected, and a
late streaming failure cannot retract headers already sent. Absence of the
header must never cause the data operation to fail.

SC metadata comes only from the receiving node's own state; it never
contacts another node. A serving replica uses its group's current Raft
leader, possibly from another local shard. A node that is not a serving
replica returns a hint only if it already knows the group's leader from an
earlier CQL redirect handled on the same shard; otherwise it omits the hint.
Collection overlaps authorized
data execution; metadata is attached only after that operation succeeds and
the table is checked again. It performs no extra data read,
Raft read barrier, or wait for group startup. An unavailable or restarting
group, unknown leader, stale forwarding destination, or lookup
deadline suppresses the hint while preserving the data operation's result.
The backend lookup deadline is one second, or `alternator_timeout_in_ms` if
that is shorter. Once data execution succeeds,
the response waits by default at most another 50 milliseconds for unfinished metadata;
then it omits the hint. Metadata that is already available adds no wait.
The live configuration option `alternator_tablet_routing_wait_timeout_in_ms`
controls that additional wait (default: `50`). Set it to `0` to include only
metadata already available when data execution completes. This can improve
latency when metadata is slow, at the cost of omitting more routing hints.
Each requesting shard admits at most 32 concurrent SC metadata lookups. When this
advisory capacity is occupied, another operation omits its hint immediately
rather than queueing its completed response. Capacity stays occupied until
the underlying lookup finishes, even if the HTTP caller's wait times out.
The ordinary-table metadata path remains local and synchronous.

### Discovering UUIDs and addresses

Use the existing authenticated Alternator `Scan` API on both:

```text
.scylla.alternator.system.local
.scylla.alternator.system.peers
```

Read `host_id`, `rpc_address`, `data_center`, and `rack`, following
`LastEvaluatedKey` pagination until exhausted. Combine the local row and peer
rows into a map from UUID (`host_id`) to connection information. Use the
client's configured Alternator scheme and port, together with its address
translation policy. CQL discovery metadata does not specify an Alternator
port. Refresh discovery when a hinted UUID is unknown or its connection
fails. For regular metadata, clients can choose replicas in their own data
center while retaining the complete replica list in the routing cache.
For strong metadata, leader-first selection can name a replica in another
data center.

Shard numbers identify tablet placement; this extension does not establish
shard-specific HTTP connections. For `mode=strong`, the first replica names
the current Raft leader without a separate leader field. This routing hint
does not change how Alternator executes a data operation.

### Client algorithm

Token calculation must use Alternator's partition-key encoding followed by
ScyllaDB's partitioner. String keys use UTF-8; binary keys use their decoded
bytes, not their base64 JSON representation. Numeric keys use Alternator's
serialized decimal representation, not their JSON text. Reuse a compatible
Alternator token-calculation implementation; hashing JSON or CQL's textual
representation is incorrect. Sort keys do not affect the partition token.
This HTTP extension does not define a second token-calculation algorithm.
Keep routing caches separate for each cluster and base table.

```text
identities = scan_all_pages(system.local) + scan_all_pages(system.peers)

for each supported data request:
    routing_cache = caches[(cluster_identity, base_table_name)]
    token = alternator_partition_token(partition_key)
    entry = routing_cache.find(token)
    if entry is absent:
        request_header = "2:-"
    else:
        i = entry.next_block_index
        block = (i << 4) | ((entry.version >> (4 * i)) & 15)
        request_header = "2:" + two_hex_digits(block)
        entry.next_block_index = (i + 1) % 16

    if entry exists and entry.mode == "strong":
        endpoint = resolve(entry.replicas[0], identities)
    else:
        endpoint = choose_replica(entry, identities, configured_dc)
    if endpoint is unavailable:
        endpoint = existing_endpoint_selection_policy()
    response = execute(endpoint, request_header)

    if response succeeded and has a valid tablet-info header:
        hint = parse_header(response)
        remove every cached range overlapping (hint.start, hint.end]
        insert hint, with next_block_index = 0
    if a hinted UUID is unknown or its connection failed:
        refresh identities through the system-table scans
```

Discard uncovered fragments of removed cache entries and rediscover them on
demand. Do not retain fragments with the removed entry's version and replica
list. If discovery or direct routing is unavailable, keep the client's
existing endpoint-selection policy.

A matching four-bit block can temporarily conceal stale routing: different
versions can have the same nibble at a particular index. Cycling through all
16 indices checks the full version over successive requests. The version
hash for regular metadata describes replica identities and shards; split
and merge operations can preserve it when replicas remain unchanged even
though bounds change.
Clients may request `2:-` to refresh bounds unconditionally. For SC metadata,
the leader-first order participates in the hash: a leadership change can
change the version even when replica identities, shards, and bounds remain
unchanged. Clients must preserve this order when caching or comparing
entries. A forced refresh can still have no SC hint while no usable leader
is known.
