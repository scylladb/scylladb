# Ports and protocols in Scylla

Scylla is a distributed system, with multiple Scylla nodes communicating
with each other and with client nodes making requests. All these different
nodes use many different protocols and TCP ports (currently, all use TCP)
to communicate with each other. Some of these protocols have an external
specification - for example Scylla supports Cassandra's CQL protocol and
Amazon's DynamoDB protocol for client requests. Other protocols, both
client-facing and internal (between Scylla nodes), are Scylla-defined.

The goal of this document is to survey all these different protocols -
what goes over which protocol, which port each protocol uses (and how this
can be configured), how to disable or enable some of the protocols, and what
part of the Scylla source code handles which protocol.

Unfortunately, the long evolution of some of these protocols resulted in
some of them getting confusing names, and often different names in Scylla's
code, documentation and configuration. This document aims to clear up some
of this confusion.

The Wireshark tool can be used to inspect and understand the messages sent
over many of the following protocols. This includes even Scylla's internal
inter-node protocol - for instructions and examples check out the blog post:
https://www.scylladb.com/2020/05/21/dissecting-scylla-packets-with-wireshark/

## Internal communication

All inter-node communiction - communication between the Scylla nodes
themselves - happen over one protocol and one TCP port - by default port 7000
(unencrypted) or 7001 (encrypted). These are the same TCP ports used by
Cassandra for this purpose, but Scylla's internal protocol is completely
different from Cassandra, and incompatible with it (one cannot mix Scylla
and Cassandra nodes in a single cluster).

Scylla's internal protocol is built on top of Seastar's "RPC" messaging
mechanism, itself built on top of TCP. This protocol includes messages of
different types ("verbs") - which are all listed in the source file
`message/messaging_service.cc`.

Here is a non-exhaustive list of things that Scylla does with these messages:

   * "Gossip" between the nodes to maintain knowledge of the cluster topology:
     Which nodes belong to the cluster, their state and token ownership.

   * Read and write (mutation) messages sent from a node receiving a client's
     request (a so-called coordinator) to a replica which holds relevant data.

   * In particular, mutations to special system tables are used to maintain
     cluster-wide agreement on schemas (table definitions).

   * Streaming data to new nodes and repairing data between nodes.

   * Messages used to implement LWT (lightweight transactions).

Although all these different types of messages use the same RPC protocol and
the same destination port on the receiver, we don't want to multiplex all of
these messages over a _single_ TCP connection. If we do, this would allow
messages of one type to severely delay messages of other types (head-of-line
blocking).  So Scylla opens several sockets to the same destination port
and uses a different socket for different message types.  The function
`do_get_rpc_client_idx` determines which types of messages get bunched
together over one socket. As of this writing, the different messages are
split into four sockets.

As everything in Scylla, the messaging service (providing this internal
communication) is a sharded service, so each shard keeps its own sockets
to each remote node. The remote node is also sharded. We cannot know for
sure which shard on the remote node will handle the messages, but we make
an effort (which is not a guarantee!) that if the two nodes have the same
number of shards, messages from shard N in the source node arrive to shard
N in the destination node.

Port 7000 is the default port for Scylla's internal communication. This choice
can be overriden by the `storage_port` configuration option. This awkward
name, `storage_port`, was kept for backward compatibility with Cassandra's
YAML configuration file. In very early Cassandra versions, this port was
used _only_ for "storage" messages (read and write), and other messages such
as gossip were sent to a different port. So today we are still stuck with
this outdated name of this configuration option.

There is also a `listen_address` configuration option to set the IP address
(and therefore network interface) on which Scylla should listen for the
internal protocol. This address defaults to `localhost`, but in any
setup except a one-node test, should be overriden.

TODO: there is also `listen_interface` option... Which wins? What's the default?
TODO: mention SSL, how it is configured, and `ssl_storage_port` (default 7001).

## CQL client protocol

The CQL binary protocol is Cassandra's and Scylla's main client-facing
protocol. Scylla supports several Scylla-only extensions to this protocol,
described in [protocol-extensions.md](protocol-extensions.md).

By default, Scylla listens to the CQL protocol on port 9042, which can be
configured via the `native_transport_port` configuration option. If set
to 0, the OS will choose the port randomly (which also applies to every CQL
port discussed later). To explicitly disable listening on a CQL port one
should set its value to `~` , or `null`.

Scylla also supports the CQL protocol via TLS/SSL encryption which is
disabled by default and can be enabled via the `native_transport_port_ssl`
configuration option. Traditional choice for the port for secure
connections is 9142, but if `client_encryption_options` are specified and
`native_transport_port_ssl` is not, then `native_transport_port` will
handle encrypted connections only. The same thing happens when
`native_transport_port` and `native_transport_port_ssl` are set to the same
value. The rules governing port assignment/encryption are summed up in
the table below:

```
np  := native_transport_port is set
nps := native_transport_port_ssl is set
ceo := client_encryption_options are enabled
eq  := native_transport_port_ssl == native_transport_port

+-----+-----+-----+-----+
|  np | nps | ceo |  eq |
+-----+-----+-----+-----+
|  0  |  0  |  0  |  *  |   =>   listen on native_transport_port, unencrypted
|  0  |  0  |  1  |  *  |   =>   listen on native_transport_port, encrypted
|  0  |  1  |  0  |  *  |   =>   don't listen
|  0  |  1  |  1  |  *  |   =>   listen on native_transport_port_ssl, encrypted
|  1  |  0  |  0  |  *  |   =>   listen on native_transport_port, unencrypted
|  1  |  0  |  1  |  *  |   =>   listen on native_transport_port, encrypted
|  1  |  1  |  0  |  *  |   =>   listen on native_transport_port, unencrypted
|  1  |  1  |  1  |  0  |   =>   listen on native_transport_port, unencrypted + native_transport_port_ssl, encrypted
|  1  |  1  |  1  |  1  |   =>   listen on native_transport_port(_ssl - same thing), encrypted
+-----+-----+-----+-----+
```

To allow "advanced shard-awareness" Scylla can accept CQL connections on
additional port(s): `native_shard_aware_transport_port` (by default 19042)
and `native_shard_aware_transport_port_ssl` (encrypted, disabled by default
just like `native_transport_port_ssl`). The typical choice for
`native_shard_aware_transport_port_ssl` is 19142.

Both shard-aware ports work almost identically as their non-shard-aware
counterparts, with only one difference: client connections arriving on
"shard-aware" ports are routed to specific shards, determined by the
client-side (local) port numbers. This feature is enabled by default and can
be disabled by setting `enable_shard_aware_drivers: false`.

The CQL protocol support can be disabled altogether by setting the
`start_native_transport` option to `false`.

These option names were chosen for backward-compatibility with Cassandra
configuration files: they refer to CQL as the "native transport", to
contrast with the older Thrift protocol (described below) which wasn't
native to Cassandra.

There is also a `rpc_address` configuration option to set the IP address
(and therefore network interface) on which Scylla should listen for the
CQL protocol. This address defaults to `localhost`, but in any setup except
a one-node test, should be overriden. Note that the same option `rpc_address`
applies to both CQL and Thrift protocols.

TODO: there is also `rpc_interface` option... Which wins? What's the default?

## Thrift client protocol

The Apache Thrift protocol was early Cassandra's client protocol, until
it was superceded in Cassandra 1.2 with the binary CQL protocol. Thrift
was still nominally supported by both Cassandra and Scylla for many years,
but was recently dropped in Cassandra (version 4.0) and is likely to be
dropped by Scylla in the future as well, so it is not recommended for new
applications.

By default, Scylla does not enable the Thrift server. In order to use it,
it must be explicitly enabled by setting the `start_rpc` configuration option
to true.

When Thrift is enabled, by default scylla listens to the Thrift protocol on port 9160,
which can be configured via the `rpc_port` configuration option. Again, this confusing
name was used for backward-compatibility with Cassandra's configuration files.
Cassandra used the term "rpc" because Apache Thrift is a remote procedure
call (RPC) framework. In Scylla, this name is especially confusing, because
as mentioned above, Scylla's internal communication protocol is based on
Seastar's RPC, which has nothing to do with the "`rpc_port`" described here.

There is also a `rpc_address` configuration option to set the IP address
(and therefore network interface) on which Scylla should listen for the
Thrift protocol. This address defaults to `localhost`, but in any
setup except a one-node test, should be overriden. Note that the same
option `rpc_address` applies to both CQL and Thrift protocols.

TODO: there is also `rpc_interface` option... Which wins? What's the default?
TODO: is there an SSL version of Thrift?

## DynamoDB client protocol

Scylla also supports Amazon's DynamoDB API. The DynamoDB API is a JSON over
HTTP (unencrypted) or HTTPS (encrypted) protocol. Support for this protocol
is not turned on by default, and must be turned on manually by setting
the `alternator_port` and/or `alternator_https_port` configuration options.
"Alternator" is the codename of Scylla's DynamoDB API support, and is
documented in more detail in [alternator.md](../alternator/alternator.md).

The standard ports that DynamoDB uses are the standard HTTP and HTTPS
ports (80 and 443, respectively), but in tests we usually use the
unprivileged port numbers 8000 and 8043 instead.

There is also an `alternator_address` configuration option to set the IP
address (and therefore network interface) on which Scylla should listen
for the DynamoDB protocol. This address defaults to 0.0.0.0.

When the HTTPS-based protocol is enabled, the server also needs to know
the certificate and key files to use. The default locations of these files
are `/etc/scylla/scylla.crt` and `/etc/scylla/scylla.key` respectively, but
can be overridden by specifying in `alternator_encryption_options` the
`keyfile` and `certificate` options. For example,
`--alternator-encryption-options keyfile="..."`.

## Redis client protocol

Scylla also has partial and experimental support for the Redis API.
Because this support is experimental, it is not turned on by
default, and must be turned on manually by setting the `redis_port`
and/or `redis_ssl_port` configuration option.

The traditional port used for Redis is 6379. Regular Redis does not
support SSL, so there is no traditional choice of port for it.

See [redis.md](redis.md) for more information about Scylla's
support for the Redis protocol.

## Metrics protocol

Scylla provides an HTTP-based protocol to fetch performance and activity
metrics from Scylla, which is described in detail in [metrics.md](metrics.md).

Scylla listens by default on port 9180 for metric requests. This port number
can be configured with the `prometheus_port` configuration option, named
after the Prometheus protocol - and the Prometheus server which is usually
used to collect these metrics.

There is also a `prometheus_address` configuration option to set the IP
address (and therefore network interface) on which Scylla should listen
for the metrics protocol. This address defaults to 0.0.0.0.

## REST API protocol

The CQL client protocol mentioned above is useful mostly for data and table
requests, but does not offer commands for _administrative_ operations such
as repair, compact, adding or removing nodes, and so on. Cassandra uses
nodetool and JMX for these (see below), but Scylla's native approach is a
RESTful API (HTTP requests).

Scylla listens for this REST API by default on port 10000, which can be
configured with the `api_port` configuration option.

The REST API has no notion of autentication of authorization, and allows
anyone connecting to it to perform destructive operations. Therefore, it
only listens for connection on the localhost (127.0.0.1) interface. This
default can be overridden by the `api_address` option - but shouldn't.

The available REST API commands are listed in JSON files in the `api/api-doc/` 
directory of the Scylla source. A user can explore this API interactively
by pointing a browser to http://localhost:10000/ui/.

There is an ongoing, but incomplete, effort to replace this REST API by
a newer, "v2", API. See [api_v2.md](../guides/api_v2.md). When complete, this v2
API will be available on the same port. You can explore the little it
offers now in the aforementioned UI by replacing the URL in the box with
"http://localhost:10000/v2".

## JMX

The "nodetool" management command connects to Scylla using Java's JMX
protocol, not the REST API described above. This protocol was kept for
backward compatibility with Cassandra and its unmodified "nodetool" command.
To implement the JMX protocol, we have a separate project,
[scylla-jmx](https://github.com/scylladb/scylla-jmx), which runs a Java
program which accepts the JMX requests supported by Cassandra, and translates
them to requests to our own REST API. These REST API requests are sent
to Scylla's REST API port over the loopback (localhost) interface.

The port on which scylla-jmx listens is by default port 7199. This port,
and the listen address, can be overridden with the `-jp` and `-ja` options
(respectively) of the `scylla-jmx` script.
