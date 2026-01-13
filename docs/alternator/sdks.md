# Alternator client libraries

## Introduction

ScyllaDB Alternator is fully (or [almost fully](compatibility.md)) compatible
with the Amazon DynamoDB™ HTTP- and JSON-based API. Applications that
use this API typically use one of Amazon's SDK libraries available for many
programming languages. These SDKs can connect well to a ScyllaDB Alternator
cluster just like they connect to Amazon DynamoDB.

However, there is one fundamental difference between how DynamoDB and
a Scylla cluster appear to an application:

- In DynamoDB, the entire service is presented to the application as a
  **single endpoint**, for example
  `https://dynamodb.us-east-1.amazonaws.com`.
- Scylla is not a single endpoint - it is a _distributed_ database - a
  cluster of **many nodes** across many racks (_availability zones_)
  and sometimes data centers (_regions_).

If we configure the application to use just one of the Scylla nodes as the
single endpoint, this specific node will become a performance bottleneck
as it gets more work than the other nodes. Moreover, this node will become
a single point of failure - if it fails, the entire service is unavailable.

This is why Alternator needs a **load balancing** solution, which distributes
the client's requests over all Scylla nodes. There are many ways to implement
such load balancing, on the server side (e.g., using DNS and HTTP load
balancers) or on the client side. In this document we will focus on the
client-side option, and introduce **Alternator client libraries**. These
libraries _add features_ to the AWS SDK that you are already using, not
replacing the original SDK. They are available for different programming
languages (see list below). Beyond load balancing and high availability,
the different Alternator client libraries add additional features and
optimizations over the standard AWS SDKs. See the [feature matrix](#feature-matrix)
below describing these capabilities, and which client library supports each.

Our goal is to require _as few as possible_ changes to the client to use
the Alternator client library. Usually, all that needs to be changed in an
application is to have it load an additional library, or initialize the
existing library a bit differently. From there on, the usual unmodified
AWS SDK functions will automatically use the entire Alternator cluster.

## List of Alternator client libraries

Alternator client libraries are available for the following programming
languages, at the following links.

* [C# Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/csharp)
  for [aws-sdk-net](https://github.com/aws/aws-sdk-net).
* [C++ Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/cpp)
  for [aws-sdk-cpp](https://github.com/aws/aws-sdk-cpp).
* [GoLang Alternator client library](https://github.com/scylladb/alternator-client-golang)
  for [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)
  and the deprecated [aws-sdk-go](https://github.com/aws/aws-sdk-go).
* [Java Alternator client library](https://github.com/scylladb/alternator-client-java)
  for [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/).
* [JavaScript Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/javascript)
  for the deprecated [aws-sdk-js](https://github.com/aws/aws-sdk-js).
  We don't yet have support for [aws-sdk-js-v3](https://github.com/aws/aws-sdk-js-v3).
* [Python Alternator client library](https://github.com/scylladb/alternator-client-python)
  for [boto3](https://github.com/boto/boto3).
* [Rust Alternator client library](https://github.com/scylladb/alternator-client-rust)
  for [aws-sdk-rust](https://github.com/awslabs/aws-sdk-rust/tree/main/sdk/dynamodb).

## Feature matrix

The different Alternator client libraries support - or not - the following
extensions over the AWS SDK:

* **Load balancing** and high-availability: The ability to continuously learn
  which ScyllaDB nodes are alive, and distribute requests between all of them.
* **Rack awareness**: The ability for a client in a specific rack (a.k.a.
  _availability zone_) to send its requests only to ScyllaDB nodes on this
  rack, if possible. This is useful when traffic between different racks is
  more expensive than traffic inside a rack.
* **Token awareness**: The ability to recognize requests that access a single
  item (namely `PutItem`, `UpdateItem`, and `GetItem`), learn which nodes (or
  shards) hold a replica of this item, and send the request directly to one of
  them. This can reduce the number of hops and therefore reduce the latency of
  these requests and increase overall throughput of the cluster. The biggest
  performance boost is for eventually-consistent `GetItem` - such requests can
  be directed to the right node immediately, and not involve any other nodes.
* **LWT awareness**: Writes that use LWT (this can be either all writes, or
  just those involving a read-before-write, depending on the configuration
  `alternator_write_isolation`) can become "contended" and very slow if the
  same partition is written concurrently and directed to multiple ScyllaDB
  nodes. So the "LWT awareness" feature recognizes write requests that use
  LWT and makes sure that they are sent to a ScyllaDB node chosen consistently
  based on the written partition key.
* **Compression**: The ability to configure compression of requests, responses,
  or both. This can help [reduce network costs](network.md) when the network
  traffic is expensive, but when the network is free and plentiful, it may be
  a waste of CPU time.
* Request **header stripping**: The ability to strip unnecessary HTTP request
  headers that the AWS SDK adds by default but which Alternator does not use.
  For example, the `User-Agent` header contains verbose SDK name and version
  information that serves no purpose in Alternator but adds bytes to every
  request. Removing such headers can reduce network costs.
* **Vector search**: Support for this Alternator-only feature that does not
  exist in DynamoDB.

The **Rack awareness** and **LWT awareness** features conflict: Rack awareness
wants writes from different racks to reach different nodes (on each rack,
a client will reach the node in its own rack), but LWT awareness wants these
writes to reach the same node. When both features are enabled, LWT writes
use the LWT-aware routing, not the rack-aware one - but this combination is
not yet functional in any of the client libraries.


| Library/Feature  | Load balancing | Rack awareness | Token awareness | LWT awareness | Compression  | Header stripping | Vector search |
| ---------------- | :------------: | :------------: | :-------------: | :-----------: | :----------: | :--------------: | :-----------: |
| C#               |       +        |       +        |                 |               |      +       |                  |               |
| C++              |       +        |                |                 |               |              |                  |               |
| GoLang v1        |       +        |       +        |                 |               |      +       |        +         |               |
| GoLang v2        |       +        |       +        |                 |       +       |      +       |        +         |               |
| Java v2          |       +        |       +        |                 |       +       |      +       |        +         |       +       |
| JavaScript v2    |       +        |                |                 |               |      +       |                  |               |
| Python (boto3)   |       +        |       +        |                 |       +       |      +       |        +         |       +       |
| Rust             |       +        |       +        |                 |       +       | request only |        +         |               |
