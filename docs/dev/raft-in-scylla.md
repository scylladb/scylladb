Date: 2022-05-06

# Intro

When we began implementing Raft we wanted to create a reusable and
well tested component which we could utilize for data, schema and topology
operations. This is why the Raft library in raft/ has the only dependency
- on seastar. It provides its own design documents and a README.

# Raft application in Scylla

In order to use the library, the client (Scylla server) needs to
provide implementations for three key interfaces:
    - persistence - to persist Raft state
    - rpc - to exchange messages with instances of the library
      on other machines
    - the client state machine - to execute commands once
    they were replicated and committed on a majority of nodes.

Depending on the application (data, topology, or schema) Scylla can use
separate instantiations of the library with different parameters.
The term we use commonly for these instantiations is Raft groups.

Some applications may require multiple instantiations (groups) which share
the implementations of persistence, rpc, and client state machine. An
example is data replication where we partition the entire key range,
and use a separate replication group for one or several partitions.

Each group must be identified with a UUID based group id, which
works as a key both internally, when persisting the group state
or loading it at boot time, and externally, when communicating
with Raft peers of the group on other nodes.

For example, to persist the changes to schema and topology, Scylla
can (and does) use node-local system tables.
There are three such tables:
    - `raft`, the main table which stores the Raft log for each
       group. The table partition key is group id, so each log
       forms its own partition. Since the table is local, this
       works fine with many groups.
    - `raft_snapshots`, a supporting table storing the so-called
       snapshot descriptors,
    - `raft_snapshot_config`, a normalized part of raft
       `raft_snapshots`, storing the cluster configuration
       at the time of taking the snapshot. May be out of date with
       the real cluster configuration, e.g. when configuration
       change happens after a snapshot and is only present  in
       `raft` log table.

The system tables are capable of storing the state of an arbitrary
number of groups, provided the group is happy with the relatively
low performance the system-table based persistence can provide.

Raft RPC implementation is the same for all groups. It runs on top
of the standard Scylla messaging service and uses group id as key
to route messages to the right group on a given host.
Raft persists peer addresses in its configuration, right next to
peer identifiers, and the format of the address is not restricted.
Currently IP addresses are used, just like in the rest of Scylla.

An own client state machine is expected to exist for each group. Right
now Scylla implements only one client state machine - the so-called
"schema state machine", created for the only group that contains
all cluster members - the group 0.

A helper service which can be used to quickly get from a group id
to its Raft instance is the group registry. It's
a sharded service which starts early at Scylla boot
and runs on all shards. The purpose of this component is to
isolate Raft service consumers within Scylla (e.g. the migration
manager for schema changes) from the logic of establishing and
maintaining the group at node start or during topology changes.

# The group 0

The main and the only group running in Scylla now is Group 0. It
maintains the schema state machine - as stored in the system
keyspace and the schema cache. Each cluster node is a member of
this group. The group's Raft server runs on shard 0 on each node.
Joining and leaving the group is integrated into topology
operations, and the group is started whenever a node starts.

## Establishing group 0 in a fresh cluster

When a Scylla node starts for the first time, it must join
group 0. If there is an existing cluster, joining can be done by
sending a request to add the starting node to the group
configuration.

But what should the node do if there is no running cluster yet?

A special distributed algorithm which persists its state locally
on each node is responsible for establishing an initial Raft
configuration in a fresh cluster. We often refer to it as
"discovery" algorithm.

Raft group 0 has an id (UUID) just like any other group. After a
node boots, this id is persisted in `scylla_local` system table.
If this id is present, the node can start a Raft instance for
the group using the last saved state in `raft`, `raft_snapshots`
and `raft_snapshot_config` system tables, which are all retrievable
by group id.
If a persisted id is missing, it means the node is bootstrapping
and haven't joined Raft yet.

To find out an existing cluster, or possibly create a new one,
an instance of 'discovery' state machine is created. This state
machine stores its intermediate state in `discovery` system table.

The machine is initialized with initial contact points: the seeds
parameter of `scylla.yaml` file. Then the machine runs a discovery
loop, during which it contacts all initial seeds, and, by
induction, all nodes known to the contacted nodes. The nodes
exchange the peer information until a transitive closure of all
peers is built - i.e. there are no new nodes discovered in the
last loop iteration and all previously discovered nodes have been
contacted.

In the process, the discovery state machine may find a node
with an existing Raft group, in which case it sends a configuration
change request to join. Otherwise the node with the lowest Raft id
establishes a new group.

As you can see from this description, Raft in Scylla supports
speedy, concurrent bootstraps of multiple nodes. As soon as
streaming is ready to support it, all Scylla topology changes
can be switched to a concurrent algorithm.

# Using Group 0 to perform schema changes

The main goal of adding Raft to Scylla is providing linearizability
of schema changes. The group 0 Raft log is used to share all schema
changes information across all cluster nodes, and perform schema changes
in the same order on every node. However, while Raft guarantees
that if an operation succeeds, it's stored in the logs on the
majority of nodes, it doesn't provide an API for strictly ordered
schema changes out of the box, for two reasons:
    - Raft only stores a log of the operations. The operations
      themselves - the schema changes - are applied to the client
      state machine once they are committed to the log. In order
      to construct a new operation it's necessary to read the
      latest state of the client state machine.
      E.g. if a client wants to create a table, it's necessary to
      learn first that the table hasn't been created already. We
      could store entire CQL statements in the log and read
      the state of the schema when the command is already committed
      to the Raft log and is being applied: but that would make
      reporting errors back to the client difficult. Besides, that
      would require that each participant of the cluster performs
      the same reads and checks, that would be an unwanted
      overhead.
      Instead, we read the schema state when constructing the raft
      command, on the node which received the DDL statement from
      the client. With this approach, two nodes could read their
      local state machines, decide that the table does not exist
      yet, and append identical commands for creating the table to
      the Raft log. The second command will fail to apply to the
      client state machine.
    - if a leader or network fails when committing an operation to
      Raft log, the client has no way of knowing its status. E.g. a
      network can time out, and establishing whether or not the
      majority of Raft nodes store the command in its log or have applied it
      may be difficult.

For the two reasons above, Scylla uses an additional
algorithm for all schema changes which it propagates through Raft
which provides an ordered, at-most-once semantics of command
application.

This algorithm is using an own system table - `group0_history` - to
store its state.

The algorithm is adding a unique, monotonic, persistent state id
to each command that is committed to Raft log of the group 0 state
machine. This id is used
to ensure that no two concurrent commands based on the same (and
outdated) state of the client state machine are applied, and to
verify that a command is committed to Raft log in a situation of
uncertainty and retry the command if it failed.

Here's how it works.

Each change to the schema state machine is "signed", or augmented, with a
state id - a unique monotonic identifier of the change. The state
id is based on TIMEUUID. When the command is applied, the state id is
persisted in `group0_history` table. In addition to the new state id,
the previous state id is also saved in the command to check that
the command is applied against the correct state of the client
state machine.

In order to perform a schema change, the client, which can be any Scylla
node, performs the following steps:

1. Takes a lock around its local client state machine, to protect against
   concurrent operations of the state machine by different threads of
   this node.
1. Issues a raft read barrier. This ensures this local client
   state machine has all the commands added to Group 0 on other
   nodes, up to the time of the barrier.
1. Reads `group0_history` table to find out the latest existing state id.
1. Creates a new state id, which would identify the new command. It
   is strictly greater than the previous state id.
1. Reads the local state machine, validates and constructs a new command.
   E.g. at this point, if we're creating a new keyspace, we may find
   that the keyspace already exists and return an error to the user.
   Otherwise we'll create a new mutation which adds the keyspace
   to the system table.
1. Creates a command to store in Raft log, which bundles together
   the mutation of the client state machine, the previous and the new
   state ids.
1. When Raft commits the command to its log, each node will apply it
   to its local schema state machine. At this point, verify that
   the previous state id of the command matches the latest state
   id of the local state machine, i.e. there was no race with another
   Raft command from a different client, and if there is a match,
   apply the command and append the new state id to `group0_history`,
   otherwise turn the command into a no-op.
1. The client which generated the command waits for the
   command to apply locally. If there was a timeout or error,
   it can retry the command with the same state id until it gets
   committed. If the state id is out of order, e.g. because there
   were another state id committed since, or this state id is
   itself already committed, applying the command turns into a
   no-op. The state ids make the commands idempotent.
   Once the command applies successfully, we can read the local
   state machine and find out what happened with it using
   `group0_history`. If the state id is recorded in the history,
   the command is really executed, otherwise it turned into a no-op,
   so the whole procedure needs to be restarted.

# Group 0 schema versioning

Historically Scylla was using hashes (called "digests") of schema mutations
calculated on each node to ensure that schema is in sync between nodes.
There was a global schema digest calculated from schema mutations of all
non-system tables, gossiped by each node as an application state
(`gms::application_state::SCHEMA`). Furthermore, each distributed table had its
own table schema version, calculated from schema mutations for this table
(`schema::version()`).

When a node noticed that another node is gossiping a different global digest
than its own, it would pull all schema mutations from that other node.

Whenever a replica receives a write or read to a table, the operation comes
with a version attached for this table by the operation's coordinator. If the
version is different or unknown, the replica would ensure that its schema
is at least as up-to-date as the coordinator's schema for this table before
handling the operation, pulling mutations for this table if necessary
(`get_schema_for_write`).

This hash-based versioning had its place in an eventually consistent world of
schema changes where different nodes might apply schema changes out of order.
But it has downsides, some of them described in issues scylladb/scylladb#7620,
scylladb/scylladb#13957. In particular, the more schema changes we performed,
the longer it would take to calculate the hash of entire schema (due to
tombstones), and at some point schema changes would slow down significantly.

With schema changes on group 0, we decided to replace these hashes with values
that are calculated in a single place -- by the sender of the schema change.
Other nodes persist the obtained global schema version and table versions when
applying the resulting group 0 command.

For the global schema version, each schema change command -- which is a vector
of mutations -- is extended with a mutation for the `system.scylla_local`
table, which is a string-string key-value store. This mutation writes the
version under `group0_schema_version` key. Whenever a node updates its
in-memory schema version (`update_schema_version_and_announce`), in particular
after each schema change, it uses the version obtained from this table if it's
present, instead of calculating a hash.

For each table creation or alteration schema change command, the vector of
mutations contains a mutation for the `system_schema.scylla_tables` table
that adds/modifies a row corresponding to the created/altered table. This row
contains a `version` cell that contains the version and a boolean
`committed_by_group0` cell that is set to `true`. Whenever a node updates its
in-memory version for this table, in particular after each schema change
touching this table, if it sees that `committed_by_group0 == true`, it will use
the provided version instead of calculating a hash.

When performing schema changes in Raft Recovery mode we're writing a tombstone
for the `system.scylla_local` entry and we write `committed_by_group0 == false`
for the `system_schema.scylla_tables` entries, forcing the old behavior.
