# Performance Isolation in Scylla
A Scylla shard does many different things in parallel. For example, as a coordinator it processes the user's CQL requests and sends them to replicas. As a replica, it executes read and write requests. Write requests are stored in memory (memtables) which is eventually flushed to sstables in a background process. When these sstables become too numerous, we compact them. And there are various other background operations, such as repair, gossip, etc.

The performance isolation mechanisms described in this document have two goals. The first goal is to **isolate** the performance of each component from the others. In other words, the throughput and latency of one component should not depend on implementation details of another component such as its degree of parallelism or the amount of I/O it does. The second goal is that Scylla be able to **control** this isolation, and determine how big a share of the resources is given to each of these components.

As an example of such control consider compaction. Scylla wants to ensure that the background compaction work receives enough resources to complete previous compaction tasks before new ones appear (as additional data gets written). On the other hand, it doesn't want to do compactions faster than necessary, because this will cause performance fluctuations for user queries (query performance is low while a compaction is proceeding quickly, and then becomes high when there is nothing left to compact).

To implement performance isolation, Scylla uses existing Seastar isolation features such as scheduling groups (for CPU-time and IO isolation). These Seastar features will not be described in detail here, and the reader is assumed to be familiar with them or to refer to Seastar's documentation. The focus of this document is how Scylla chose to use these features to implement isolation.

## Scheduling groups (CPU scheduler)
Scylla defines the following Seastar scheduling groups. The groups have a global scope (i.e., not per-table, per-keyspace, etc.). They are created in `main.cc`, and saved in a `database_config` structure of the database object (see `database.hh`).

The groups currently defined are:

| Name (`database_config::*_scheduling_group`) | Default shares
| -------------------------------------------- | --------------
| default (a.k.a system)                       | 1000
| memtable                                     | 1000
| memtable_to_cache                            | 1000
| compaction                                   | 1000
| memory_compaction                            | 1000
| statement                                    | 1000
| gossip                                       | 1000
| streaming (a.k.a maintenance)                | 200

TODO: explain the purpose each of each of these scheduling groups, and what they are used for. E.g., "streaming" is also called maintenance and used also used for repair. memtable is used for memtable flushes (?). default is used for gossip, etc.

The "Default shares" is the initial number of shares given to each scheduling group. They can be later modified by controllers, which aim to discover when a certain component needs to run faster because it is not keeping up - or run slower because it is finishing more quickly than it needs and causing performance to fluctuate. See the "Controllers" section below.

## Additional notes

TODO: mention that the more groups we have, the higher worst-case latency.
To mitigate this, we should switch to nested groups, where two nested groups in statement compete with each other but not with compaction (for example). 

## Controllers

TODO: list the controllers we have, and which shares each one changes, and how.

See also: [compaction_controller.md]

## Per user performance isolation
TODO

## Multi-tenancy
We do not yet support multi-tenancy, in the sense that different tenants of the same server get isolated performance guarantees. When we do support this, it will need to be documented here.

## Preserving isolation across RPC calls
ScyllaDB uses RPC verbs heavily to communicate with other nodes. RPC is based on "verbs". Each RPC call will invoke a verb, which is associated with a C++ function. Most verbs have a fixed scheduling groups associated with them (see `messaging/messaging_service.cc` `do_get_rpc_client_idx()`). The remote node, when receiving that RPC verb, will execute it with the scheduling group associated with said verb.
There is one group of RPC verbs -- called the statement group -- whose verbs can be invoked from the context of multiple scheduling groups. To support this, statement RPC verbs support "tenants". The list of these tenants are configured in `main.cc`. Each tenant has a scheduling groups and an identifier. Each tenant has its own RPC connection. The tenant identifier is used during connection setup to communicate to the remote node, which tenant is associated with this connection, and therefore which scheduling group RPC verb should be executed in.
