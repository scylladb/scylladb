# Topology state machine

The topology state machine tracks all the nodes in a cluster,
their state, properties (topology, tokens, etc) and requested actions.

Node state can be one of those:
 none             - the new node joined group0 but did not bootstraped yet (has no tokens and data to serve)
 bootstrapping    - the node is currently in the process of streaming its part of the ring
 decommissioning  - the node is being decomissioned and stream its data to nodes that took over
 removing         - the node is being removed and its data is streamed to nodes that took over from still alive owners
 replacing        - the node replaces another dead node in the cluster and it data is being streamed to it
 rebuilding       - the node is being rebuild and is streaming data from other replicas
 normal           - the node does not do any streaming and serves the slice of the ring that belongs to it
 left             - the node left the cluster and group0

Nodes in state left are never removed from the state.

State transition diagram:

{none} ------> {bootstrapping|replacing} ------> {normal} <---> {rebuilding}
 |                   |                              |
 |                   |                              |
 |                   V                              V
 ----------------> {left}  <--------  {decommissioning|removing}


A state may have additional parameters associated with it. For instance
'replacing' state has host id of a node been replaced as a parameter.

Tokens also can be in one of the states:

write_both_read_old - writes are going to new and old replica, but reads are from
             old replicas still
write_both_read_new - writes still going to old and new replicas but reads are
             from new replica
owner      - tokens are owned by the node and reads and write go to new
             replica set only

Tokens that needs to be move start in 'write_both_read_old' state. After entire
cluster learns about it streaming start. After the streaming tokens move
to 'write_both_read_new' state and again the whole cluster needs to learn about it
and make sure no reads started before that point exist in the system.
After that tokens may move to the 'owner' state.

The state machine also maintains a map of topology requests per node.
When a request is issued to a node the entry is added to the map. A
request is one of the topology operation currently supported: join,
leave, replace, remove and rebuild. A request may also have parameters
associated with it which are also stored in a separate map.
