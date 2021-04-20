# Repair based node operations

Here below is a simple introduction to the node operations Scylla support and
some of the issues that streaming based node operations have.

- Replace operation

   It is used to replace a dead node. The token ring does not change. Replacing
   node pulls data from only one of the replicas which might not be the latest
   copy.

- Rebuild operation

   It is used to get all the data this node owns form other existing nodes. It
   pulls data from only one of the replicas which might not be the latest copy.

- Removenode operation

   It is used to remove a dead node out of the cluster. Existing nodes pull
   data from other existing nodes for the new ranges it owns. It pulls from one
   of the replicas which might not be the latest copy.

- Bootstrap operation

   It is used to add a new node into the cluster. The token ring changes. It
   does not suffer from the "latest replica” issue. New node pulls data
   from existing nodes that are losing the token ranges.

   It suffers from failed streaming. We split the ranges in 10 groups and we
   stream one group at a time. Restream the group if failed, causing
   unnecessary data transmission on wire.

   Bootstrap is not resumable. With failure after 99.99% of data is streamed,
   if we restart the node again, we need to stream all the data again even if
   the node already has 99.99% of the data.

- Decommission operation

   It is used to remove a live node form the cluster. Token ring changes. It
   does not suffer from the “latest replica” issue. The leaving node pushes data
   to existing nodes.

   It suffers from resumable issue like bootstrap operation.

To solve all the issues above. We use repair based node operations. The idea
behind repair based node operations is simple: use repair to synchronize data
between replicas instead of using streaming.

The benefits:

- Latest copy is guaranteed

- Resumable in nature

- No extra data is streamed on wire
  E.g., rebuild twice, will not stream the same data twice

- Unified code path for all the node operations

- Free repair operation during bootstrap, replace operation and so on.

Select nodes to synchronize with:

- Replace operation
  Synchronize with all replica nodes from the local DC

- Rebuild operation
  Synchronize with all replica nodes from the local DC or from the DC specified by the user

- Removenode operation
  Synchronize with all replica nodes from local DC

- Bootstrap operation
  0) If everywhere_topology is used, synchronize with all nodes in local dc

  1) If local_dc_replica_nodes = RF, synchronize with 1 node that is losing the
  range in the local DC

  2) If 0 < local_dc_replica_nodes < RF, synchronize with
  up to RF/2 + 1 nodes in local DC

  3) If local_dc_replica_nodes = 0, reject the bootstrap operation

- Decommission operation
  1) Synchronize with one node that is gaining the range in local DC

  For example, with RF = 3 and 4 nodes n1, n2, n3, n4 in the cluster, n3 is
  removed, old_replicas = {n1, n2, n3}, new_replicas = {n1, n2, n4}.

  The decommission node will repair all the ranges the leaving node is
  responsible for. Choose the decommission node n3 to run repair to synchronize
  with the new owner node n4 in the local DC.

  2) Synchronize with one node when no node is gaining the range in local DC

  For example, with RF = 3 and 3 nodes n1, n2, n3 in the cluster, n3 is
  removed, old_replicas = {n1, n2, n3}, new_replicas = {n1, n2}.

  The decommission node will repair all the ranges the leaving node is
  responsible for. The cluster is losing data on n3, it has to synchronize with
  at least one of {n1, n2}, otherwise it might lose the only new replica on n3.
  Choose the decommission node n3 to run repair to synchronize with one of the
  replica nodes, e.g., n1, in the local DC.

  Note, it is enough to synchronize with one node instead of quorum number of
  nodes. For example, with RF = 3, timestamp(B) > timestamp(A),

     n1  n2  n3  n4
  1) A   B   B       (Before n3 is decommissioned)
  2) A   B           (After n3 is decommissioned)
  3) A   B       B   (After n4 is bootstrapped)

  Suppose we decommission n3 and n3 decides to synchronize with only n2. We end
  up with n1 has A, n2 has B. Then we add a new node n4 to the cluster, since
  no node is losing the range, so n4 will sync quorum number of nodes,
  that is n1 and n2. As a result, n4 will have B instead of A.
