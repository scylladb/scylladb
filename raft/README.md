# Raft consensus algorithm implementation for Seastar

Seastar is a high performance server-side application framework
written in C++. Please read more about Seastar at http://seastar.io/

This library provides an efficient, extensible, implementation of
Raft consensus algorithm for Seastar.
For more details about Raft see https://raft.github.io/

## Terminology

Raft PhD is using a set of terms which are widely adopted in the
industry, including this library and its documentation. The
library provides replication facilities for **state machines**.
Thus the **state machine** here and in the source is a user
application, distributed by means of Raft.
The library is implemented in a way which allows to replace/
plug in its key components:
- communication between peers by implementing **rpc** API
- persisting the library's private state on disk,
  via **persistence** class
- shared failure detection by supplying a custom
  **failure detector** class
- user state machine, by passing an instance of **state
  machine** class.

Please note that the library internally implements its own
finite state machine for protocol state - class fsm. This class
shouldn't be confused with the user state machine.

## Implementation status
---------------------
- log replication, including throttling for unresponsive
  servers
- managing of the user's state machine snapshots
- leader election, including the pre-voting algorithm
- non-voting members (learners) support
- configuration changes using joint consensus
- read barriers
- forwarding commands to the leader

## Usage
-----

In order to use the library, the application has to provide implementations
for RPC, persistence and state machine APIs, defined in `raft/raft.hh`,
namely:
- `class rpc`, provides a way to communicate between Raft protocol instances,
- `class persistence` persists the required protocol state on disk,
- class `state_machine` is the actual state machine being replicated.

A complete description of expected semantics and guarantees
is maintained in the comments for these classes and in sample
implementations. Let's list here key aspects the implementer should
bear in mind:
- RPC should implement a model of asynchronous, unreliable network,
  in which messages can be lost, reordered, retransmitted more than
  once, but not corrupted. Specifically, it's an error to
  deliver a message to a wrong Raft server.
- persistence should provide a durable persistent storage, which
  survives between state machine restarts and does not corrupt
  its state. The storage should contain an efficient mostly-appended-to
  part containing Raft log, thousands and hundreds of thousands of entries,
  and a small register-like memory are to contain Raft
  term, vote and the most recent snapshot descriptor.
- Raft library calls `state_machine::apply()` for entries
  reliably committed to the replication log on the majority of
  servers. While `apply()` is called in the order
  the entries were serialized in the distributed log, there is
  no guarantee that `apply()` is called exactly once.
  E.g. when a server restarts from the persistent state,
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
- call server::start() to start the server
- repeat the above for every node in the cluster
- use `server::add_entry()` to submit new entries
  `state_machine::apply()` is called after the added
  entry is committed by the cluster.

### Subsequent usages

Similar to the first usage, but internally `start()` calls
`persistence::load_term_and_vote()` `persistence::load_log()`,
`persistence::load_snapshot()` to load the protocol and state
machine state, persisted by the previous incarnation of this
server instance.

## Architecture bits

### Joint consensus based configuration changes

Seastar Raft implementation provides arbitrary configuration
changes: it is possible to add and remove one or multiple
nodes in a single transition, or even move Raft group to an
entirely different set of servers. The implementation adopts
the two-step algorithm described in the original Raft paper:
- first, a log entry with joint configuration is
committed. The "joint" configuration contains both old and
new sets of servers. Once a server learns about a new
configuration, it immediately adopts it, so as soon as
the joint configuration is committed, the leader will require two
majorities - the old one and the new one - to commit new entries.
- once a majority of servers persists the joint
entry, a final entry with new configuration is appended
to the log.

If a leader is deposed during a configuration change,
a new leader carries out the transition from joint
to the final configuration.

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
- Since Raft leader no longer sends RPC every  0.1 second there can be a
  situation when a follower may not know who the leader is for a long time
  (if the leader is idle). Add an extension that allows a follower to actively
  search for a leader by sending specially crafted append reply RPC to all voters.
  A leader will reply with an empty append message to such a message.

### Pre-voting and protection against disruptive leaders

tl;dr: do not turn pre-voting OFF

The library implements the pre-voting algorithm described in Raft PHD.
This algorithms adds an extra voting step, requiring each candidate to
collect votes from followers before updating its term. This prevents
"term races" and unnecessary leader step downs when e.g. a follower that
has been isolated from the cluster increases its term, becomes a candidate
and then disrupts an existing leader.
The pre-voting extension is ON by default. Do not turn it OFF unless
testing or debugging the library itself.

Another extension suggested in the PhD is protection against disruptive
leaders. It requires followers to withhold their vote within an election
timeout of hearing from a valid leader. With pre-voting ON and use of shared
failure detector we found this extension unnecessary, and even leading to
reduced liveness. It was thus removed from the implementation.

As a downside, with pre-voting *OFF* servers outside the current
configuration can disrupt cluster liveness if they stay around after having
been removed.

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

## Snapshot API

A snapshot is a compact representation of the user state machine
state. The structure of the snapshot and details of taking
a snapshot are not known to the library. It uses instances
of class `snapshot_id` (essentially UUIDs) to identify
state machine snapshots.

The snapshots are used in two ways:
- to manage Raft log length, i.e. be able to truncate
  the log when it grows too big; to truncate a log,
  the library takes a new state machine snapshot and
  erases most log entries older than the snapshot;
- to bootstrap a new member of the cluster or
  catch up a follower that has fallen too much behind
  the leader and can't use the leader's log alone; in
  this case the library instructs the user state machine
  on the leader to transfer its own snapshot (identified by snapshot
  id) to the specific follower, identified by `raft::server_id`.
  It's then the responsibility of the user state machine
  to transfer its compact state to the peer in full.

`snapshot_descriptor` is a library container for snapshot id
and associated metadata. This class has the following structure:

```
struct snapshot_descriptor {
    // Index and term of last entry in the snapshot
    index_t idx = index_t(0);
    term_t term = term_t(0);
    // The committed configuration in the snapshot
    configuration config;
    // Id of the snapshot.
    snapshot_id id;
};

// The APIs in which the snapshot descriptor is used:

future<snapshot_id> state_machine::take_snapshot()
void state_machine::drop_snapshot(snapshot_id id)
future<> state_machine::load_snapshot(snapshot_id id)

future<snpashot_reply> rpc::send_snapshot(server_id server_id, const install_snapshot& snap, seastar::abort_source& as)

future<> persistence::store_snapshot_descriptor(const snapshot& snap, size_t preserve_log_entries);
future<> persistence::load_snapshot_descriptor()
```

The state machine must save its snapshot either
when the library calls `state_machine::take_snapshot()`, intending
to truncate Raft log length afterwards, or when the snapshot
transfer from the leader is initiated via `rpc::send_snapshot()`.

In the latter case the leader's state machine is expected
to contact the follower's state machine and send its snapshot to
it.

When Raft wants to initialize a state machine with a snapshot
state it calls `state_machine::load_snapshot()` with appropriate
snapshot id.

When raft no longer needs a snapshot it uses
`state_machine::drop_snapshot()` to inform the state machine it
can drop the snapshot with a given id.

Raft persists the currently used snapshot descriptor by calling
`persistence::store_snapshot_descriptor()`. There is no separate
API to explicitly drop the previous stored descriptor, the
call is allowed to overwrite it. On success, this call is followed
by `state_machine::drop_snapshot()` to drop the snapshot referenced
by the previous descriptor in the state machine.

The snapshot state must survive restarts, so it should be put to
disk either in `take_snapshot()` or when with persisting the
snapshot descriptor, in `persistence::store_snapshot_descriptor()`.


It is possible that a crash or stop happens soon after creating a
new snapshot and before dropping the old one. In that case
`persistence` contains only the latest snapshot descriptor.
The library never uses more than one snapshot, so when the state
machine is later restarted all snapshots except the one with its
id persisted in the snapshot descriptor can be
safely dropped.

Calls to `state_machine::take_snapshot()` and snapshot transfers are not
expected to run instantly. Indeed, respective API returns
`future<>`, so these calls may take a while.  Imagine the
state machine has a snapshot and is asked by the library to
take a new one. A leader change happens while snapshot-taking is in
progress and the new leader starts a snapshot transfer to the
follower. Even less likely, but still possible, that a yet another
leader is elected and it also starts an own snapshot transfer to the
follower. And another one. Thus a single server may be taking a
local state machine snapshot and running multiple transfers. When
all of this is done, the library will automatically select the
snapshot with the latest term and index, persist its id in the
snapshot descriptor and call `state_machine::load_snapshot()` with this
id. All the extraneous snapshots will be dropped
by the library, unless the server crashes.
Once again, to cleanup any garbage after a crash, the complying
implementation is expected to delete all snapshots except the one
which id is persisted in the snapshot descriptor upon restart.
