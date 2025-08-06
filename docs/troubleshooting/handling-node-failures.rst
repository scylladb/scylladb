Handling Node Failures
------------------------

ScyllaDB relies on the Raft consensus algorithm, which requires at least a quorum 
of nodes in a cluster to be available. If one or more nodes are down, but the quorum 
is live, reads, writes, schema updates, and topology changes proceed unaffected. When the node that 
was down is up again, it first contacts the cluster to fetch the latest schema and 
then starts serving queries.

The following examples show the recovery actions when one or more nodes or DCs 
are down, depending on the number of nodes  and DCs in your cluster.

Examples
=========

.. list-table:: Cluster A: 1 datacenter, 3 nodes
   :widths: 20 40 40
   :header-rows: 1

   * - Failure
     - Consequence
     - Action to take
   * - 1 node
     - Schema and topology updates are possible and safe.
     - Try restarting the node. If the node is dead, :doc:`replace it with a new node </operating-scylla/procedures/cluster-management/replace-dead-node/>`.
   * - 2 nodes
     - Data is available for reads and writes; schema and topology changes are impossible.
     - Restart at least 1 of the 2 nodes that are down to regain quorum. If you can’t recover at least 1 of the 2 nodes, consult the :ref:`manual recovery section <recovery-procedure>`.

.. list-table:: Cluster B: 2 datacenters, 6  nodes (3 nodes per DC)
   :widths: 20 40 40
   :header-rows: 1

   * - Failure
     - Consequence
     - Action to take
   * - 1-2 nodes
     - Schema and topology updates are possible and safe.
     - Try restarting the node(s). If the node is dead, :doc:`replace it with a new node </operating-scylla/procedures/cluster-management/replace-dead-node/>`.
   * - 3 nodes
     - Data is available for reads and writes; schema and topology changes are impossible.
     - Restart 1 of the 3 nodes that are down to regain quorum. If you can’t recover at least 1 of the 3 failed nodes, consult the :ref:`manual recovery <recovery-procedure>` section.
   * - 1DC
     - Data is available for reads and writes; schema and topology changes are impossible.
     - When the DCs come back online, restart the nodes. If the DC fails to come back online and the nodes are lost, consult the :ref:`manual recovery <recovery-procedure>` section.


.. list-table:: Cluster C: 3 datacenter, 9  nodes (3 nodes per DC)
   :widths: 20 40 40
   :header-rows: 1

   * - Failure
     - Consequence
     - Action to take
   * - 1-4 nodes
     - Schema and topology updates are possible and safe.
     - Try restarting the nodes. If the nodes are dead, :doc:`replace them with new nodes </operating-scylla/procedures/cluster-management/replace-dead-node-or-more/>`.
   * - 1 DC
     - Schema and topology updates are possible and safe.
     - When the DC comes back online, try restarting the nodes in the cluster. If the nodes are dead, :doc:`add 3 new nodes in a new region </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>`.
   * - 2 DCs
     - Data is available for reads and writes, schema and topology changes are impossible.
     - When the DCs come back online, restart the nodes. If at least one DC fails to come back online and the nodes are lost, consult the :ref:`manual recovery <recovery-procedure>` section.

.. _recovery-procedure:

Manual Recovery Procedure
===========================

.. note::

   This recovery procedure assumes that consistent topology changes are enabled for your cluster, which is mandatory in
   versions 2025.2 and later. If you failed to enable consistent topology changes during the upgrade to 2025.2, you need
   to follow the `previous recovery procedure <https://docs.scylladb.com/manual/branch-2025.1/troubleshooting/handling-node-failures.html#manual-recovery-procedure>`_.

   See :ref:`Verifying that consistent topology changes are enabled <verifying-consistent-topology-changes-enabled>`.

You can follow the manual recovery procedure when the majority of nodes (for example, 2 out of 3) failed and are irrecoverable.

During the manual recovery procedure you'll restart live nodes in a special recovery mode, which will cause the
cluster to initialize the Raft algorithm from scratch. However, this time, faulty nodes will not participate in the
algorithm. Then, you will replace all faulty nodes (using the standard
:doc:`node replacement procedure </operating-scylla/procedures/cluster-management/replace-dead-node/>`). Finally, you
will leave the recovery mode and remove the obsolete internal Raft data.

**Prerequisites**

* Before proceeding, make sure that the irrecoverable nodes are truly dead, and not, 
  for example, temporarily partitioned away due to a network failure. If it is 
  possible for the 'dead' nodes to come back to life, they might communicate and 
  interfere with the recovery procedure and cause unpredictable problems.

  If you have no means of ensuring that these irrecoverable nodes won't come back 
  to life and communicate with the rest of the cluster, setup firewall rules or otherwise 
  isolate your alive nodes to reject any communication attempts from these dead nodes.

* Ensure all live nodes are in the normal state using
  :doc:`nodetool status </operating-scylla/nodetool-commands/status>`. If there is a node
  that is joining or leaving, it cannot be recovered. You must permanently stop it. After
  performing the recovery procedure, use
  :doc:`nodetool status </operating-scylla/nodetool-commands/status>` ony any other node.
  If the stopped node appears in the output, it means that other nodes still consider it
  a member of the cluster, and you should remove it with the
  :doc:`node removal procedure </operating-scylla/procedures/cluster-management/remove-node/>`.

* Check if the cluster lost data. If the number of dead nodes is equal or larger than your
  keyspaces RF, then some of the data is lost, and you need to retrieve it from backup. After
  completing the manual recovery procedure
  :doc:`restore the data from backup </operating-scylla/procedures/backup-restore/restore/>`.

* Decide whether to shut down your service for the manual recovery procedure. ScyllaDB
  serves data queries during the procedure, however, you may not want to rely on it if:

  * you lost some data, or

  * restarting a single node could lead to unavailability of data queries (the procedure involves
    a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart>`). For
    example, if you are using the standard RF=3, CL=QUORUM setup, you have two datacenters, all
    nodes in one of the datacenters are dead and one node in the other datacenter is dead,
    restarting another node in the other datacenter will cause temporary data query
    unavailability (until the node finishes restarting).

**Procedure**

#. Perform a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart/>` of your live nodes.

#. Find the group 0 ID by performing the following query on any live node, using e.g. ``cqlsh``:

   .. code-block:: cql

        cqlsh> SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id';

   The group 0 ID is needed in the following steps.

#. Find ``commit_idx`` of all live nodes by performing the following query on **every live node**:

   .. code-block:: cql

        cqlsh> SELECT commit_idx FROM system.raft WHERE group_id = <group 0 ID>;

   Choose a node with the largest ``commit_idx``. If there are multiple such nodes, choose any of them.
   The chosen node will be the *recovery leader*.

#. Perform the following queries on **every live node**:

   .. code-block:: cql

        cqlsh> TRUNCATE TABLE system.discovery;
        cqlsh> DELETE value FROM system.scylla_local WHERE key = 'raft_group0_id';

#. Perform a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart/>` of all live nodes,
   but:

   * **restart the recovery leader first**,

   * before restarting each node, add the ``recovery_leader`` property to its ``scylla.yaml`` file and set it to the
     host ID of the recovery leader,

   * after restarting each node, make sure it participated in Raft recovery; look for one of the following messages
     in its logs:

    .. code-block:: console

        storage_service - Performing Raft-based recovery procedure with recovery leader <host ID of the recovery leader>/<IP address of the recovery leader>
        storage_service - Raft-based recovery procedure - found group 0 with ID <ID of the new group 0; different from the one used in other steps>

   After completing this step, Raft should be fully functional.

#. Replace all dead nodes in the cluster using the
   :doc:`node replacement procedure </operating-scylla/procedures/cluster-management/replace-dead-node/>`.

   .. note::

        Removing some of the dead nodes with the
        :doc:`node removal procedure </operating-scylla/procedures/cluster-management/remove-node/>` is also possible,
        but it may require decreasing RF of your keyspaces. With tablets enabled, ``nodetool removenode`` is rejected
        if there are not enough nodes to satisfy RF of any tablet keyspace in the node's datacenter.

#. Remove the ``recovery_leader`` property from the ``scylla.yaml`` file on all nodes. Send the ``SIGHUP`` signal to all
   ScyllaDB processes to ensure the change is applied.

#. Perform the following queries on **every live node**:

   .. code-block:: cql

        cqlsh> DELETE FROM system.raft WHERE group_id = <group 0 ID>;
        cqlsh> DELETE FROM system.raft_snapshots WHERE group_id = <group 0 ID>;
        cqlsh> DELETE FROM system.raft_snapshot_config WHERE group_id = <group 0 ID>;
