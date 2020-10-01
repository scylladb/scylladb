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

## Usage
-----

In order to use the library the application has to provide implementations
for RPC, storage and state machine APIs, defined in raft/raft.hh. The
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
- storage should provide a durable persistent storage, which
  survives between state machine restarts and does not corrupt
  its state.
- Raft library calls `state_machine::apply_entry()` for entries
  reliably committed to the replication log on the majority of
  servers. While `apply_entry()` is called in the order
  entries are serialized in the distributed log, there is
  no guarantee that `apply_entry()` is called exactly once.
  E.g. when a protocol instance restart from persistent state,
  it may re-apply some already applied log entries.

Seastar's execution model is that every object is safe to use
within a given shard (physical OS thread). Raft library follows
the same pattern. Calls to Raft API are safe when they are local
to a single shard. Moving instances of the library between shards
is not supported.

### First usage.

For an example of first usage see `replication_test.cc` in test/raft/.

In a nutshell:
- create instances of RPC, storage, and state machine
- pass them to an instance of Raft server - the facade to the Raft cluster
  on this node
- repeat the above for every node in the cluster
- use `server::add_entry()` to submit new entries
  on a leader, `state_machine::apply_entries()` is called after the added
  entry is committed by the cluster.

### Subsequent usages

Similar to the first usage, but `storage::load_term_and_vote()`
`storage::load_log()`, `storage::load_snapshot()` are expected to
return valid protocol state as persisted by the previous incarnation
of an instance of class server.

