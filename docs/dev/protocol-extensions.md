# Protocol extensions to the Cassandra Native Protocol

This document specifies extensions to the protocol defined
by Cassandra's native_protocol_v4.spec and native_protocol_v5.spec.
The extensions are designed so that a driver supporting them can
continue to interoperate with Cassandra and other compatible servers
with no configuration needed; the driver can discover the extensions
and enable them conditionally.

An extension can be discovered by the client driver by using the OPTIONS
request; the returned SUPPORTED response will have zero or more options
beginning with `SCYLLA` indicating extensions defined in this document, in
addition to options documented by Cassandra. How to use the extension
is further explained in this document.

## Extending protocol extensions support

As mentioned above, in order to use a protocol extension feature by both
server and client, they need to negotiate the used feature set when establishing
a connection.

The negotiation procedure has the following steps:
  - Client sends the OPTIONS request to the Scylla instance to get a list of
    protocol extensions that the server understands.
  - Server sends the SUPPORTED message in reply to the OPTIONS request. The
    message body is a string multimap, in which keys describe different
    extensions and possibly one or more additional values specific to a
    particular extension (specified as distinct values under a feature key in
    the following form: `ARG_NAME=VALUE`).
  - The client determines the set of compatible extensions which it is going
    to use in the current connection by intersecting known capabilities list
    with what it has received in SUPPORTED response.
  - Client driver sends the STARTUP request with additional payload consisting
    of key-value pairs, each describing a negotiated extension.
  - Server determines the set of compatible extensions by intersecting known
    list of protocol extensions with what it has received in STARTUP request.

Both client and server use the same string identifiers for the keys to determine
negotiated extension set, judging by the presence of a particular key in the
SUPPORTED/STARTUP messages.

## Intranode sharding

This extension allows the driver to discover how Scylla internally
partitions data among logical cores. It can then create at least
one connection per logical core, and send queries directly to the
logical core that will serve them, greatly improving load balancing
and efficiency.

To use the extension, send the OPTIONS message. The data is returned
in the SUPPORTED message, as a set of key/value options. Numeric values
are returned as their base-10 ASCII representation.

The keys and values are:
  - `SCYLLA_SHARD` is an integer, the zero-based shard number this connection
    is connected to (for example, `3`).
  - `SCYLLA_NR_SHARDS` is an integer containing the number of shards on this
    node (for example, `12`). All shard numbers are smaller than this number.
  - `SCYLLA_PARTITIONER` is a the fully-qualified name of the partitioner in use (i.e.
    `org.apache.cassandra.partitioners.Murmur3Partitioner`).
  - `SCYLLA_SHARDING_ALGORITHM` is the name of an algorithm used to select how
    partitions are mapped into shards (described below)
  - `SCYLLA_SHARDING_IGNORE_MSB` is an integer parameter to the algorithm (also
    described below)
  - `SCYLLA_SHARD_AWARE_PORT` is an additional port number where Scylla is listening
    for CQL connections. If present, it works almost the same way as port 9042 typically
    does; the difference is that client-side port number is used as an indicator to which
    shard client wants to connect. The desired shard number is calculated as:
    `desired_shard_no = client_port % SCYLLA_NR_SHARDS`. Its value is a decimal
    representation of type `uint16_t`, by default `19042`.
  - `SCYLLA_SHARD_AWARE_PORT_SSL` is an additional port number where Scylla is
    listening for encrypted CQL connections. If present, it works almost the same way
    as port 9142 typically does; the difference is that client-side port number is used
    as an indicator to which shard client wants to connect. The desired shard number
    is calculated as: `desired_shard_no = client_port % SCYLLA_NR_SHARDS`.
    Its value is a decimal representation of type `uint16_t`, by default `19142`.

Currently, one `SCYLLA_SHARDING_ALGORITHM` is defined,
`biased-token-round-robin`. To apply the algorithm,
perform the following steps (assuming infinite-precision arithmetic):

  - subtract the minimum token value from the partition's token
    in order to bias it: `biased_token = token - (-2**63)`
  - shift `biased_token` left by `ignore_msb` bits, discarding any
    bits beyond the 63rd:
      `biased_token = (biased_token << SCYLLA_SHARDING_IGNORE_MSB) % (2**64)`
  - multiply by `SCYLLA_NR_SHARDS` and perform a truncating division by 2**64:
    `shard = (biased_token * SCYLLA_NR_SHARDS) / 2**64`

(this apparently convoluted algorithm replaces a slow division instruction with
a fast multiply instruction).

in C with 128-bit arithmetic support, these operations can be efficiently
performed in three steps:

```c++
    uint64_t biased_token = token + ((uint64_t)1 << 63);
    biased_token <<= ignore_msb;
    int shard = ((unsigned __int128)biased_token * nr_shards) >> 64;
```

In languages without 128-bit arithmetic support, use the following (this example
is for Java):

```Java
    private int scyllaShardOf(long token) {
        token += Long.MIN_VALUE;
        token <<= ignoreMsb;
        long tokLo = token & 0xffffffffL;
        long tokHi = (token >>> 32) & 0xffffffffL;
        long mul1 = tokLo * nrShards;
        long mul2 = tokHi * nrShards;
        long sum = (mul1 >>> 32) + mul2;
        return (int)(sum >>> 32);
    }
```

It is recommended that drivers open connections until they have at
least one connection per shard, then close excess connections.

## LWT prepared statements metadata mark

This extension allows the driver to discover whether LWT statements have a
special bit set in prepared statement metadata flags, which indicates that
the driver currently deals with an LWT statement.

Having a designated flag gives the ability to reliably detect LWT statements
and remove the need to execute custom parsing logic for each query, which is not
only costly but also error-prone (e.g. parsing the prepared query with regular
expressions).

The feature is meant to be further utilized by client drivers to use primary
replicas consistently when dealing with conditional statements.

Choosing primary replicas in a predefined order ensures that in case of multiple
LWT queries that contend on a single key, these queries will queue up at the
replica rather than compete: choose the primary replica first, then, if the
primary is known to be down, the first secondary, then the second secondary, and
so on.
This will reduce contention over hot keys and thus increase LWT performance.

The feature is identified by the `SCYLLA_LWT_ADD_METADATA_MARK` key that is
meant to be sent in the SUPPORTED message along with the following additional
parameters:
  - `LWT_OPTIMIZATION_META_BIT_MASK` is a 32-bit unsigned integer that represents
    the bit mask that should be used by the client to test against when checking
    prepared statement metadata flags to see if the current query is conditional
    or not.

## Rate limit error

This extension allows the driver to send a new type of error in case the operation
goes over the allowed per-partition rate limit. This kind of error does not fit
other existing error codes well, hence the need for the protocol extension.

On receiving this error, the driver should not retry the request; instead,
the error should be propagated to the user so that they can decide what to do
with it - sometimes it might make sense to propagate the error, in other cases
it might make sense to retry with backoff.

The body of the error consists of the usual error code, error message and then
the following fields: `<op_type><rejected_by_coordinator>`, where:

- `op_type` is a byte which identifies the operation which is the origin
  of the rate limit.
  - 0: read
  - 1: write
- `rejected_by_coordinator` is a byte which is 1 if the operation was rejected
  on the coordinator and 0 if it was rejected by replicas.

If the driver does not understand this extension and does not enable it,
the Config_error will be used instead of the new error code.

In order to be forward compatible with error codes added in the future protocol
versions, this extension doesn't reserve a fixed error code - instead, it
advertises the integer value used as the error code in the SUPPORTED response.

This extension is identified by the `SCYLLA_RATE_LIMIT_ERROR` key.
The string map in the SUPPORTED response will contain the following parameters:

  - `ERROR_CODE`: a 32-bit signed decimal integer which Scylla
    will use as the error code for the rate limit exception.

## Sending tablet info to the drivers

This extension adds support for sending tablet info to the drivers if the 
request was routed to the wrong node/shard.

There is a need for sending tablet info to the drivers so they can be 
tablet aware.
For the best performance we want to get this info lazily only when it is 
needed.

The info is send when driver asks about the information that the specific 
tablet contains and it is directed to the wrong node/shard so it could 
use that information for every subsequent query.
If we send the query to the wrong node/shard, we want to send the RESULT 
message with additional information about the tablet in `custom_payload`:

  - `tablets-routing-v1` - tablets routing information, which contains info about token
    range (in format `(first_token, last_token]`) and tablet replicas, for every replica
    there is information about the host and shard.

The driver has to be able to receive `custom_payload` and deserialise its field
from `bytes` to:

  - for `tablets-routing-v1` - `TupleType(LongType, LongType, ListType(TupleType(UUIDType, Int32Type)))`,
    two `LongType` represent first and last token, `ListType(TupleType(UUIDType, Int32Type))`
    contains information about replicas (for every replica there is a tuple with two elements
    `UUIDType` and `Int32Type` representing host and shard ids).

When the driver receives information about the tablet, it has to check if any of
the previously received tablets has an overlapping token range.
The group of tablets that meets this criterion has to be deleted, and the new
tablet should replace them.

## Negotiate sending tablets info to the drivers

This extension allows the driver to inform the database that it is aware of
tablets and is able to interpret the tablet information sent in `custom_payload`.

Having a designated flag gives the ability to skip tablet metadata generation
(which is quite expensive) if driver is not aware of tablets. 

The feature is identified by the `TABLETS_ROUTING_V1` key, which is meant to be sent
in the SUPPORTED message.
