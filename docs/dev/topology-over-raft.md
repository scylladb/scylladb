# Topology state machine

The topology state machine tracks all the nodes in a cluster,
their state, properties (topology, tokens, etc) and requested actions.

Node state can be one of those:
- `none`             - the new node joined group0 but did not bootstraped yet (has no tokens and data to serve)
- `bootstrapping`    - the node is currently in the process of streaming its part of the ring
- `decommissioning`  - the node is being decomissioned and stream its data to nodes that took over
- `removing`         - the node is being removed and its data is streamed to nodes that took over from still alive owners
- `replacing`        - the node replaces another dead node in the cluster and it data is being streamed to it
- `rebuilding`       - the node is being rebuild and is streaming data from other replicas
- `normal`           - the node does not do any streaming and serves the slice of the ring that belongs to it
- `left`             - the node left the cluster and group0

Nodes in state left are never removed from the state.

State transition diagram for nodes:
```
{none} ------> {bootstrapping|replacing} ------> {normal} <---> {rebuilding}
 |                   |                              |
 |                   |                              |
 |                   V                              V
 ----------------> {left}  <--------  {decommissioning|removing}
```


A node state may have additional parameters associated with it. For instance
'replacing' state has host id of a node been replaced as a parameter.

Additionally to specific node states, there entire topology can also be in a transitioning state:

- `commit_cdc_generation` - a new CDC generation data was written to internal tables earlier
    and now we need to commit the generation - create a timestamp for it and tell every node
    to start using it for CDC log table writes.
- `publish_cdc_generation` - a new CDC generation was committed and now we need to publish it
    to user-facing description tables.
- `write_both_read_old` - one of the nodes is in a bootstrapping/decommissioning/removing/replacing state.
    Writes are going to both new and old replicas (new replicas means calculated according to modified
token ring), reads are using old replicas.
- `write_both_read_new` - as above, but reads are using new replicas.

When a node bootstraps, we create new tokens for it and a new CDC generation
and enter the `commit_cdc_generation` state. After committing the generation we
move to `publish_cdc_generation`. Once the generation is published, we enter
`write_both_read_old` state. After the entire cluster learns about it,
streaming starts. When streaming finishes, we move to `write_both_read_new`
state and again the whole cluster needs to learn about it and make sure that no
reads that started before this point exist in the system. Finally we remove the
transitioning state.

Decommission, removenode and replace work similarly, except they don't go through
`commit_cdc_generation` and `publish_cdc_generation`.

The state machine may also go only through `commit_cdc_generation` and
`publish_cdc_generation` states after getting a request from the user to create
a new CDC generation if the current one is suboptimal (e.g. after a
decommission).

The state machine also maintains a map of topology requests per node.
When a request is issued to a node the entry is added to the map. A
request is one of the topology operation currently supported: `join`,
`leave`, `replace`, `remove` and `rebuild`. A request may also have parameters
associated with it which are also stored in a separate map.

Note that some nodes may require work but the topology as a whole does not
transition. An example of this is the `rebuilding` state which does not change
the topology but requires streaming data.

Separately from the per-node requests, there is also a 'global' request field
for operations that don't affect any specific node but the entire cluster,
such as `check_and_repair_cdc_streams`.

# Load balancing

If there is no work for the state machine, tablet load balancer is invoked to
check if we need to rebalance. If so, it computes an incremental tablet migration
plan, persists it by moving tablets into transitional states, and moves the state machine
into the tablet migration track. All this happens atomically form the perspective
of group0 state machine.
The tablet migration track excludes with other topology changes, so node operations
will have to wait for the plan to finish before they can take over the state machine.

The tablet balancing track migrates a small bunch of tablets, decided by the
loaded balancer, and then moves back the state machine to the idle state.
This gives other topology changes a chance to start, and if there aren't any, the
load balancer will be called again to check the conditions. This way
we can avoid blocking topology changes for too long, but also drive the cluster
to eventually achieve balance in the absence of other requests.

The load balancer is always invoked with no pending tablet migrations. This
allows for simplicity in the implementation, but may lead to underutilization
of cluster resources if different tablets migrate with different speeds,
and thus limit the speed of load balancing.

The reason why the load balancer is part of the main state machine and excludes with other topology
changes is that we want to share the infrastructure for fencing between vnode-based topology
changes and tablet migration. This calls for some way to mutually exclude the two so that they
don't interfere with each other. The simplest is to make them part of the same state machine.

When the topology state machine is not in the tablet_migration track, it is guaranteed
that there are no tablet transitions in the system.

Currently, all tablets in a batch decided by the load balancer are migrated in parallel and
their state machines are advanced at the same time. This means that streaming has to complete
for all tablets in a batch before any of them can move to the next phase. This is suboptimal
and will be changed later to allow for independent transitions.

# Tablet migration

Each tablet has its own migration state machine stored in group0 which is part of the tablet state. It involves
these properties of a tablet:
  - replicas: the old replicas of a table
  - new_replicas: the new replicas of a tablet
  - stage: determines which replicas should be used by requests on the coordinator side, and which
           action should be taken by the state machine executor.

Currently, the tablet state machine is driven forward by the tablet balancing track of the
topology state machine.

The "stage" serves two major purposes:

1. Firstly, it determines which action should be taken by the topology change coordinator on behalf
   of the tablet before it can move to the next step. When stage is advanced, it means that
   expected invariants about cluster-wide state relevant to the tablet, associated with the next stage, hold.

2. Also, stage affects which replicas are used by the coordinator for reads and writes.
   Replica selectors are stored in tablet_transition_info::writes and tablet_transition_info::reads,
   which are directly derived from the stage stored in system tables.

The invariants of stages, which hold as soon as the stage is committed to group0:

1. allow_write_both_read_old

    Precondition: transition info in group0 is filled with information about migration.

2. write_both_read_old

    Precondition: All old and new replicas:

    1. see the transition info from step 1 via local token metadata and effective replication maps.

    2. are prepared for receiving writes for the local tablet replica.

3. streaming

    Precondition: All writes that will be stored by any replica in the old or the new set,
    which are executed on behalf of a successful request, will reach CL in both old and new replica sets.
    This ensures that when step 4 is reached, the new replica set will reflect all successful writes
    either by the means of coordinator replication or by the means of streaming.

4. write_both_read_new

    Precondition: New tablet replicas contain all the writes which reached the matching leaving tablet replicas
    before step 3.

5. use_new

    Precondition: All read requests started after this, and which complete successfully, use the new replica set.

6. cleanup

    Precondition: No write request will reach tablet replica in the database layer which does not belong to the new replica set.

When tablet is not in transition, the following invariants hold:

1. The storage layer (database) on any node contains writes for keys which belong to the tablet only if
    that shard is one of the current tablet replicas.


# Topology state persistence table

The in memory state's machine state is persisted in a local table `system.topology`.
The schema of the table is:
```
CREATE TABLE system.topology (
    key text,
    host_id uuid,
    datacenter text,
    ignore_msb int,
    node_state text,
    num_tokens int,
    rack text,
    rebuild_option text,
    release_version text,
    replaced_id uuid,
    shard_count int,
    tokens set<text>,
    topology_request text,
    transition_state text static,
    current_cdc_generation_timestamp timestamp static,
    current_cdc_generation_uuid uuid static,
    global_topology_request text static,
    new_cdc_generation_data_uuid uuid static,
    PRIMARY KEY (key, host_id)
)
```
This is a single-partition table, with `key = 'topology'`.

Each node has a clustering row in the table where its `host_id` is the clustering key. The row contains:
- `host_id`            -  id of the node
- `datacenter`         -  a name of the datacenter the node belongs to
- `rack`               -  a name of the rack the node belongs to
- `ignore_msb`         -  the value of the node's `murmur3_partitioner_ignore_msb_bits` parameter
- `shard_count`        -  the node's `smp::count`
- `release_version`    -  the node's `version::current()` (corresponding to a Cassandra version, used by drivers)
- `node_state`         -  current state of the node (as described earlier)
- `topology_request`   -  if set contains one of the supported node-specific topology requests
- `tokens`             -  if set contains a list of tokens that belongs to the node
- `replaced_id`        -  if the node replacing or replaced another node here will be the id of that node
- `rebuild_option`     -  if the node is being rebuild contains datacenter name that is used as a rebuild source
- `num_tokens`         -  the requested number of tokens when the node bootstraps

There are also a few static columns for cluster-global properties:
- `transition_state` - the transitioning state of the cluster (as described earlier), may be null
- `current_cdc_generation_timestamp` - the timestamp of the last introduced CDC generation
- `current_cdc_generation_uuid` - the UUID of the last introduced CDC generation (used to access its data)
- `global_topology_request` - if set, contains one of the supported global topology requests
- `new_cdc_generation_data_uuid` - used in `commit_cdc_generation` state, the UUID of the generation to be committed
