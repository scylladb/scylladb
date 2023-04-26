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
