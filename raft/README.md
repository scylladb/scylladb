# Raft consensus algorithm implementation for Seastar

Seastar is a high performance server-side application framework
written in C++. Please read more about Seastar at http://seastar.io/

This library provides an efficient, extensible, implementation of
Raft consensus algorithm for Seastar.
For more details about Raft see https://raft.github.io/

## Implementation status
---------------------
- log replication, including throttling for unresponsive
  servers
- leader election
- configuration changes using joint consensus

## Usage
-----

In order to use the library the application has to provide implementations
for RPC, persistence and state machine APIs, defined in raft/raft.hh. The
purpose of these interfaces is:
- provide a way to communicate between Raft protocol instances
- persist the required protocol state on disk,
a pre-requisite of the protocol correctness,
- apply committed entries to the state machine.

While comments for these classes provide an insight into
expected guarantees they should provide, in order to provide a complying
implementation it's necessary to understand the expectations
of the Raft consistency protocol on its environment:
- RPC should implement a model of asynchronous, unreliable network,
  in which messages can be lost, reordered, retransmitted more than
  once, but not corrupted. Specifically, it's an error to
  deliver a message to a Raft server which was not sent to it.
- persistence should provide a durable persistent storage, which
  survives between state machine restarts and does not corrupt
  its state.
- Raft library calls `state_machine::apply_entry()` for entries
  reliably committed to the replication log on the majority of
  servers. While `apply_entry()` is called in the order
  entries are serialized in the distributed log, there is
  no guarantee that `apply_entry()` is called exactly once.
  E.g. when a protocol instance restarts from the persistent state,
  it may re-apply some already applied log entries.

Seastar's execution model is that every object is safe to use
within a given shard (physical OS thread). Raft library follows
the same pattern. Calls to Raft API are safe when they are local
to a single shard. Moving instances of the library between shards
is not supported.

### First usage.

For an example of first usage see `replication_test.cc` in test/raft/.

In a nutshell:
- create instances of RPC, persistence, and state machine
- pass them to an instance of Raft server - the facade to the Raft cluster
  on this node
- repeat the above for every node in the cluster
- use `server::add_entry()` to submit new entries
  on a leader, `state_machine::apply_entries()` is called after the added
  entry is committed by the cluster.

### Subsequent usages

Similar to the first usage, but `persistence::load_term_and_vote()`
`persistence::load_log()`, `persistence::load_snapshot()` are expected to
return valid protocol state as persisted by the previous incarnation
of an instance of class server.

## Architecture bits

### Joint consensus based configuration changes

Seastar Raft implementation provides arbitrary configuration
changes: it is possible to add and remove one or multiple
nodes in a single transition, or even move Raft group to an
entirely different set of servers. The implementation adopts
the two-step algorithm described in the original Raft paper:
- first, an entry in the Raft log with joint configuration is
committed. The joint configuration contains both old and
new sets of servers. Once a server learns about a new
configuration, it immediately adopts it.
- once a majority of servers persists the joint
entry, a final entry with new configuration is appended
to the log.

If a leader is deposed during a configuration change,
the new leader carries out the transition from joint
to final configuration for it.
it carries out the transition for the prevoius leader.

No two configuration changes could happen concurrently. The leader
refuses a new change if the previous one is still in progress.

### Multi-Raft

One of the design goals of Seastar Raft was to support multiple Raft
protocol instances. The library takes the following steps to address
this:
- `class server_address`, used to identify a Raft server instance (one
  participant of a Raft cluster) uses globally unique identifiers, while
  provides an extra `server_info` field which can then store a network
  address or connection credentials.
  This makes it possible to share the same transport (RPC) layer
  among multiple instances of Raft. But it is then the responsibility
  of this shared RPC layer to correctly route messages received from
  a shared network channel to a correct Raft server using server
  UUID.
- Raft group failure detection, instead of sending Raft RPC every 0.1 second
  to each follower, relies on external input. It is assumed
  that a single physical server may be a container of multiple Raft
  groups, hence failure detection RPC could run once on network peer level,
  not sepately for each Raft instance. The library expects an accurate
  `failure_detector` instance from a complying implementation.

### RPC module address mappings

Raft instance needs to update RPC subsystem on changes in
configuration, so that RPC can deliver messages to the new nodes
in configuration, as well as dispose of the old nodes.
I.e. the nodes which are not the part of the most recent
configuration anymore.

New nodes are added to the RPC configuration after the
configuration change is committed but before the instance
sends messages to the peers.

Until the messages are successfully delivered to at least
the majority of "old" nodes and we have heard back from them,
the mappings should be kept intact. After that point the RPC
mappings for the removed nodes are no longer of interest
and thus can be immediately disposed.

There is also another problem to be solved: in Raft an instance may
need to communicate with a peer outside its current configuration.
This may happen, e.g., when a follower falls out of sync with the
majority and then a configuration is changed and a leader not present
in the old configuration is elected.

The solution is to introduce the concept of "expirable" updates to
the RPC subsystem.

When RPC receives a message from an unknown peer, it also adds the
return address of the peer to the address map with a TTL. Should
we need to respond to the peer, its address will be known.

An outgoing communication to an unconfigured peer is impossible.
