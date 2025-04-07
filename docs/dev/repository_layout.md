# ScyllaDB repository layout


This document is meant to provide a helicopter view over the ScyllaDB repository.

The document has the following objectives:
* Shorten the ramp up time of an onboarding core engineer
* Improve code search locality during development of the first few tasks
* Map the theoretical knowledge one acquires during the first few days to actual source files on disk
* Get a better chance of understanding the project top-down

Please note the source code is in an ever changing state with continuous effort being put in to refactor, decouple, isolate components. The source code hierarchy still looks flat with many source files located in the root directory, so some of these files might move in the future, some component dirs might get split, moved or removed altogether, we’ll do our best to keep the doc accurate.


[alternator/](https://github.com/scylladb/scylladb/tree/master/alternator)

The location of the Alternator project which provides interoperability with Amazon DynamoDB, here’s a Scylla Univerisity [course](https://university.scylladb.com/courses/scylla-alternator/) on it.
Also check out the [docs/alternator](https://github.com/scylladb/scylladb/blob/master/docs/alternator/alternator.md) directory for lots of documentation on this topic.

[abseil/](https://github.com/scylladb/abseil-cpp/tree/d7aaad83b488fd62bd51c81ecf16cd938532cc0a)

Bundled abseil library. ScyllaDB uses this for containers such a flat_hash_map, btrees, etc.

[api/](https://github.com/scylladb/scylladb/tree/master/api)

This location contains source files managing Scylla REST APIs. ScyllaDB is being managed using the APIs defined here, the [nodetool](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool.html) utility for instance uses such APIs to expose operations. The precursor of these APIs was Cassandra’s [JMX](https://en.wikipedia.org/wiki/Java_Management_Extensions).

See [the protocols md](protocols.md) for a more detailed explanation of the protocols Scylla uses and implements.

[auth/](https://github.com/scylladb/scylladb/tree/master/auth)

Everything related to authorization and authentication between the clients and Scylla nodes.

[bin/](https://github.com/scylladb/scylladb/tree/master/bin)

Scripts/symlinks like nodetool, cqlsh. 

[cdc/](https://github.com/scylladb/scylladb/tree/master/cdc)

Location for the [Change Data Capture](
https://opensource.docs.scylladb.com/stable/features/cdc/) feature which offers mechanism
for looking at the history of updates to tables.

[cmake/](https://github.com/scylladb/scylladb/tree/master/cmake)

Here we have cmake files files that help external dependencies to be used, and set global parameters for cmake. At this moment there is an ongoing effort to migrate from configure.py based build to cmake based builds.

[compaction/](https://github.com/scylladb/scylladb/tree/master/compaction)

Everything related to ScyllaDB compaction. There are files defining the supported compaction strategies, the compaction_manager deciding when compaction is triggered and some other infrastructure code.

[conf/](https://github.com/scylladb/scylladb/tree/master/conf)

This directory contains configuration files used as defaults for running ScyllaDB locally.

[cql3/](https://github.com/scylladb/scylladb/tree/master/cql3)

That’s the location of the [Cassandra's Query Language (CQL)](https://cassandra.apache.org/doc/4.0/cassandra/cql/) based frontend. This location contains the grammar defining CQL, the parser, extensions, etc.

For the historically curious, the number 3 in "cql3" refers to version 3 of the language, which is the third and last major revision of this language, introduced in Cassandra 1.1 in [2012](https://www.datastax.com/blog/whats-new-cql-30). CQL continued to evolve afterwards, but not in any major way,
The name Cassandra gives to its most recent language specification is
```java
public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.8");
```
(last updated in June 2024).


[data_dictionary/](https://github.com/scylladb/scylladb/tree/master/data_dictionary)

In Scylla, nodes can play either a `coordinator` or `replica` role in the context of a single query and are symmetric with regards to the roles they perform.
A very high level explanation is that `data_dictionary` is an abstraction on top of these role classes so don't have to use monster classes like e.g. replica `database` class directly.

An alternative explanation is that `data_dictionary` are light-weight objects referring to the schema (keyspaces, tables, etc.) without access to the data itself (the access to data is what a `coordinator` and `replica` indeed do differently).


[db/](https://github.com/scylladb/scylladb/tree/master/db)

This is a mixed bag of multiple components. There is `replica` code, there is `snapshot` code, `commit log` code, etc. The `db/view` subdirectory contains code for replicating from a base table into a view.
 
[dht/](https://github.com/scylladb/scylladb/tree/master/dht)

Distributed HashTable. This is where most of the logic for partitioning lives and where things like the hash algorithm for obtaining the token can be found.

[direct_failure_detector/](https://github.com/scylladb/scylladb/tree/master/direct_failure_detector)

This component is responsible for detecting whether a node is reachable or not, it keeps some sort of heartbeat channel open with a node to see when it goes down.
Its main user at the moment is Raft.

[exceptions/](https://github.com/scylladb/scylladb/tree/master/exceptions)

This is a place that gathers all exception types related mostly to csql3, but others too.

[gms/](https://github.com/scylladb/scylladb/tree/master/gms)

The Gossip protocol implementation used for distributing information between nodes within a Scylla cluster. Whenever you want to make a piece of information available to everyone in the cluster, you put it on gossip and eventually everybody will get to know it.
Rumour has it that the protocol is not as reliable as we’d want it to be, so it might get replaced sometime in the future.

[idl/](https://github.com/scylladb/scylladb/tree/master/idl)

Interface definition language, it’s used for defining the message body of inter-node communication within the cluster. It’s very flexible as it allows extensions of the message bodies without breaking compatibility. The idl-compiler.py does the code generation based on specification.

[index/](https://github.com/scylladb/scylladb/tree/master/index)

This location defines the secondary_index class, closely related to the materialized views concept in ScyllaDB, see this [secondary_index.md](secondary_index.md) for an explanation on how that works.

[lang/](https://github.com/scylladb/scylladb/tree/master/lang)

Experimental feature for supporting UDFs. There’re Lua and Wasm files implementing that support.

[licenses/](https://github.com/scylladb/scylladb/tree/master/licenses)

Licenses for ScyllaDB and bundled packages.

[locator/](https://github.com/scylladb/scylladb/tree/master/locator)

The location for most of the code related to replication, replication strategies, RF, CL.
This is where the replication part of the new tablets feature is implemented as well, find a description of them in this [link](https://opensource.docs.scylladb.com/stable/architecture/tablets.html). Basically they allow faster and more efficient scaling of the ScyllaDB clusters.

[message/](https://github.com/scylladb/scylladb/tree/master/message)

This is the layer of communication between Scylla nodes. It is highly related to `idl/`, it defines high-level methods for serialization/deserialization for writing RPCs.

[mutation/](https://github.com/scylladb/scylladb/tree/master/mutations)

This is where the code belonging to the datamodel lives. Any write operation is a mutation and a mutation is basically a diff. Mutations can be combined out of order because cells are timestamped.

[mutation_writer/](https://github.com/scylladb/scylladb/tree/master/mutation_writer)

Code related to mutations, but more on the front on how to split mutations, how to make data from a mutation before a timestamp to fit in a particular bucket and also how to distribute mutation on shards.

[node_ops/](https://github.com/scylladb/scylladb/tree/master/node_ops)

Support code for operations that change the topology of a ScyllaDB cluster.
These are legacy operations. Modern code uses raft-based topology changes that supersede the (repair-based) node operations.

[repair/](https://github.com/scylladb/scylladb/tree/master/repair)

Implementation for the node repair process. As write operations are eventual consistent, sometimes node might fall out of sync, especially when new nodes join the cluster. Node repair is a process scheduled to run in the background which goes over all the nodes and make sure their data is in sync.
The old way of doing this is via the streaming processes, but that was slower and had the drawback of not being resumable in case of failure.
More on this topic [here](https://www.scylladb.com/2023/12/07/faster-safer-node-operations-with-repair-vs-streaming/),

[raft/](https://github.com/scylladb/scylladb/tree/master/raft)

Implementation for the Raft distributed state machine which helps store the topology, schema, metadata, etc. Raft is now the default and Gossip topology changes are on their way out. Raft has formal proofs for correctness or, as @denesb likes to say, Gossip is like “trust me bro!” compared to a lawyer-drafted contract from Raft.

[readers/](https://github.com/scylladb/scylladb/tree/master/readers)

ScyllaDB library for readers. There is a reader interface defined and implementations for SSTable readers, Memtable readers, network readers, etc
All of these readers can be combined to produce a common output stream.

[redis/](https://github.com/scylladb/scylladb/tree/master/redis)

This is a (partial) implementation of the Redis API, a third front-end after CQL and [DynamoDB API (Alternator)](https://github.com/scylladb/scylladb/tree/master/alternator).

More details about this API in [redis.md](redis.md) developer documentation.

[reloc/](https://github.com/scylladb/scylladb/tree/master/reloc)

Code related to packaging ScyllaDB as rpm, deb, etc. This is called reloc because ScyllaDB is packaged as a relocatable package with all dependencies bundled in.

[replica/](https://github.com/scylladb/scylladb/tree/master/replica)

Most of the code related to replica lives here. As modularization work evolves, even more replica code will be moved here. Giants like the database and table classes live here.

[rust/](https://github.com/scylladb/scylladb/tree/master/rust)

Code related to UDFs, closely related to lang/

[schema/](https://github.com/scylladb/scylladb/tree/master/schema)

This location contains code related to [schema](https://docs.scylladb.com/stable/get-started/query-data/schema.html) and metadata.
The schema defines how the user's data is represented within ScyllaDB, tables definition, keyspaces, etc. ['Here'](https://opensource.docs.scylladb.com/stable/cql/ddl)'s more on the topic of ScyllaDB schema.

[scripts/](https://github.com/scylladb/scylladb/tree/master/scripts)

Development and maintenance scripts, most notably being open-coredump.sh, a convenience script for obtaining a debugging environment with frozen toolchain.

[seastar/](https://github.com/scylladb/seastar)

A git submodule for the [Seastar](/seastar) library repository.
This library deserves a separate subdocument to describe its layout.

[service/](https://github.com/scylladb/scylladb/tree/master/service)

Coordinator node code. There is also code using Raft for consistent topology changes, consistent schema changes, etc.

[sstables/](https://github.com/scylladb/scylladb/tree/master/sstables)

This is where the code for reading and writing SSTables (Sorted Strings Tables) on disk lives. ScyllaDB stores the data on persistent storage in SSTables.
Here's a high-level [description](https://www.scylladb.com/glossary/sstable/) of ScyllaDB SSTables.

[streaming/](https://github.com/scylladb/scylladb/tree/master/streaming)

This location contains the implementation for the streaming algorithm used to provision and sync nodes. To be replaced by the algorithm defined in repair/ as described above.

[swagger-ui/](https://github.com/scylladb/scylla-swagger-ui)

This is closely related to api/. It's used to generated documentation from Swagger-compliant REST APIs.

[tasks/](https://github.com/scylladb/scylladb/tree/master/tasks)

Task manager implementation for the internal background tasks in Scylla.

[test/](https://github.com/scylladb/scylladb/tree/master/test)

All unit tests of ScyllaDB live under this path. The `test.py` script is used to run those tests, although there is an ongoing effort to move away from this script to pytest.
`test/perf` contains the microbenchmarks we frequently ran to look for regressions.
Apart from these tests, we also have Scylla Distributed Tests (dtests) and Scylla Cluster Tests ([SCT](https://github.com/scylladb/scylla-cluster-tests) tests) in separate repositories
which are meant to test Scylla cluster functionalities in more realistic scenarios and in some cases under high load.

[tools/](https://github.com/scylladb/scylladb/tree/master/tools)

Various user tools like notedool, tools for inspecting the content of an SSTable or dumping an SSTable to JSON.

[tracing/](https://github.com/scylladb/scylladb/tree/master/tracing)

Diagnosis capabilities integrated in CQL.

[transport/](https://github.com/scylladb/scylladb/tree/master/transport)

This is the networking layer implementation for the cql3 clients, it defines the packets, format, etc.

[types/](https://github.com/scylladb/scylladb/tree/master/types)

The type system of Scylla, Alternator, etc. All data types, casting operations are defined here.

[unified/](https://github.com/scylladb/scylladb/tree/master/unified)

Packaging utilities, install/uninstall scripts.

[utils/](https://github.com/scylladb/scylladb/tree/master/utils)

A mixed bag of everything. In-house containers, allocation strategies, utility function implementations, etc.
