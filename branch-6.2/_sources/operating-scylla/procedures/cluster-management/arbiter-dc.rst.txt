=========================================================
Preventing Quorum Loss in Symmetrical Multi-DC Clusters
=========================================================

ScyllaDB requires at least a quorum (majority) of nodes in a cluster to be up
and communicate with each other. A cluster that loses a quorum can handle reads
and writes of user data, but cluster management operations, such as schema and
topology updates, are impossible.

In clusters that are symmetrical, i.e., have two (DCs) with the same number of
nodes, losing a quorum may occur if one of the DCs becomes unavailable.
For example, if one DC fails in a 2-DC cluster where each DC has three nodes,
only three out of six nodes are available, and the quorum is lost.

Adding another DC would mitigate the risk of losing a quorum, but it comes
with network and storage costs. To prevent the quorum loss with minimum costs,
you can configure an arbiter (tie-breaker) DC.

An arbiter DC is a datacenter with a :doc:`zero-token node </architecture/zero-token-nodes>`
-- a node that doesn't replicate any data but is only used for Raft quorum
voting. An arbiter DC maintains the cluster quorum if one of the other DCs
fails, while it doesn't incur extra network and storage costs as it has no
user data.

Adding an Arbiter DC
-----------------------

To set up an arbiter DC, follow the procedure to
:doc:`add a new datacenter to an existing cluster </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>`.
When editing the *scylla.yaml* file, set the ``join_ring`` parameter to
``false`` following these guidelines:

* Set ``join_ring=false`` before you start the node(s). If you set that
  parameter on a node that has already been bootstrapped and owns a token
  range, the node startup will fail. In such a case, you'll need to
  :doc:`decommission </operating-scylla/procedures/cluster-management/decommissioning-data-center>`
  the node, :doc:`wipe it clean </operating-scylla/procedures/cluster-management/clear-data>`,
  and add it back to the arbiter DC properly following
  the :doc:`procedure </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>`.
* As a rule, one node is sufficient for an arbiter to serve as a tie-breaker.
  In case you add more than one node to the arbiter DC, ensure that you set
  ``join_ring=false`` on all the nodes in that DC.

Follow-up steps:
^^^^^^^^^^^^^^^^^^^
* An arbiter DC has a replication factor of 0 (RF=0) for all keyspaces. You
  need to ``ALTER`` the keyspaces to update their RF.
* Since zero-token nodes are ignored by drivers, you can skip
  :ref:`configuring the client not to connect to the new DC <add-dc-to-existing-dc-not-connect-clients>`.

References
----------------

* :doc:`Zero-token Nodes </architecture/zero-token-nodes>`
* :doc:`Raft Consensus Algorithm in ScyllaDB </architecture/raft>`
* :doc:`Handling Node Failures </troubleshooting/handling-node-failures>`
* :doc:`Adding a New Data Center Into an Existing ScyllaDB Cluster </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>`