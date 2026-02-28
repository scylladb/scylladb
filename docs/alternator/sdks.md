# Alternator client libraries

## Introduction

ScyllaDB Alternator is fully (or [almost fully](compatibility.md)) compatible
with the Amazon DynamoDB&trade;'s HTTP- and JSON-based API. Applications that
use this API typically use one of Amazon's [SDK libraries](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/sdk-general-information-section.html)
available for many programming languages. These SDKs can connect well to a
ScyllaDB Alternator cluster just like they connect to Amazon DynamoDB.

However, there is one fundamental difference between how DynamoDB and how
a Scylla cluster appear to an application:

- In DynamoDB, the entire service is presented to the application as a
  **single endpoint**, for example
  `http://dynamodb.us-east-1.amazonaws.com`.
- Scylla is not a single endpoint - it is a _distributed_ database - a
  cluster of **many nodes** across many racks (_availability zones_).

If we configure the application to use just one of the Scylla nodes as the
single endpoint, this specific node will become a performance bottleneck
as it gets more work than the other nodes. Moreover, this node will become
a single point of failure - if it fails, the entire service is unavailable.

In our blog post [Load Balancing in Scylla Alternator](https://www.scylladb.com/2021/04/13/load-balancing-in-scylla-alternator/)
we explained the need for load balancing in Alternator and the various
server-side and client-side options that are available. In this document
we will focus on the client-side option, specially on **Alternator client
libraries**. These libraries _add features_ to the AWS SDK you are already
using, not replacing the original SDK. They are available for different
programming languages (see list below)

Our goal is to require _as little as possible_ changes to the client to use
the Alternator client library. Usually, all that needs to be changed in an
application is to have it load an additional library, or initialize the
existing library a bit differently. From there on, the usual unmodified
AWS SDK functions will automatically use the entire Alternator cluster.

The Alternator client libraries add the following capabilities to the AWS SDK:

* Load balancing - send requests to all Scylla nodes, ensuring that they
  are all loaded equally.
* High availability - ensure that the service continues normally even if
  some of Scylla nodes go down, or new nodes join the cluster.
* Request routing - when possible and beneficial, send each request to the
  best node to handle it.
  Request routing may have rack awareness (send to a node on the same rack
  as the client), token awareness (send to a node that holds the requested
  partition), and LWT awareness (send different writes to the same partition
  always to the same node).
* Compression of requests and/or responses, and sending fewer headers, to
  [reduce network costs in Alternator](network.md).

Currently, not all client libraries support all of these new capabilities,
so please consult the feature matrix below to see which library for which
language supports which feature.

## List of Alternator client libraries

Alternator client libraries are available for the following programming
languages, at the following links.

* [C# Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/csharp)
  for [aws-sdk-net](https://github.com/aws/aws-sdk-net).
* [C++ Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/cpp)
  for [aws-sdk-cpp](https://github.com/aws/aws-sdk-cpp).
* [GoLang Alternator client library](https://github.com/scylladb/alternator-client-golang)
  for [aws-sdk-go](https://github.com/aws/aws-sdk-go) (deprecated)
  and [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2).
* [Java Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/java)
  for [aws-sdk-java](https://github.com/aws/aws-sdk-java) (deprecated)
  and [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2/).
* [Javascript Alternator client library](https://github.com/scylladb/alternator-load-balancing/tree/master/javascript)
  for [aws-sdk-js](https://github.com/aws/aws-sdk-js) (deprecated)
* [Python Alternator client library](https://github.com/scylladb/alternator-client-python)
  for [boto3](https://github.com/boto/boto3)

## Feature matrix

The different Alternator client libraries support - or not - the following
extensions over the AWS SDK:

* **Load**: Basic load balancing and high-availability: The ability to
  continuously learn which ScyllaDB nodes are alive and send requests to
  all of them - not just one.
* **Rack**: Rack awareness: The ability for a client in a specific rack
  (a.k.a. _availability zone_) to send its requests only to ScyllaDB nodes
  on this rack. This is useful when traffic between different racks is more
  expensive than traffic inside a rack.
* **Token**: Token awareness: The ability to recognize requests that access
  a single item (namely `PutItem`, `UpdateItem`,`GetItem`), learn which nodes
  (or shards) hold a replica to this item, and send the request directly
  to it. This can reduce the number of hops and therefore reduce the latency
  of these requests and increase overall throughput of the cluster. The
  biggest performance boost is for eventually-consistent `GetItem` -
  where the request will be directed to the right node immediately, and
  not involve any other nodes.
* **LWT**: LWT awareness: Writes that use LWT (this can be either all writes,
  or just those involving a read-before-write, depending on the
  `alternator_write_isolation` option) can become "contended" and very slow
  if the same partition is written concurrently and directed to multiple
  ScyllaDB nodes. So the "LWT awareness" feature recognizes write that use
  LWT, and makes sure that they are sent to a ScyllaDB node chosen
  consistently based on the writen partition key.

* **Compress**: The ability to configure compression of requests, responses,
  or both. This can be beneficial if the network traffic is expensive, but
  when the network is free and plentiful, it may be a waste of CPU time.

Note that if LWT awareness is enabled in the client, it overrides the rack
awareness feature: Rack awareness wants writes from different racks to reach
different nodes (on each rack, a client will reach the node in its own rack),
but LWT awareness wants these writes to reach the same node.


| Library/Feature  | Load | Rack | Token | LWT | Compress |
| ---------------- | :--: | :--: | :---: | :-: | :------: |
| C#               |      |      |       |     |          |
| C++              |      |      |       |     |          |
| GoLang v1        |      |      |       |     |          |
| GoLang v2        |      |      |       |  +  |    +     |
| Java v1          |  +   |      |       |     |          |
| Java v2          |  +   |      |       |     |          |
| Javascript v2    |      |      |       |     |          |
| Javascript v3    |      |      |       |     |          |
| Python (boto3)   |      |      |       |     |          |
