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

You can follow the manual recovery procedure when:

* The majority of nodes (for example, 2 out of 3) failed and are irrecoverable.
* :ref:`The Raft upgrade procedure <verify-raft-procedure>` got stuck because one 
  of the nodes failed in the middle of the procedure and is irrecoverable.

.. warning::

   Perform the manual recovery procedure **only** if you're dealing with 
   **irrecoverable** nodes. If possible, restart your nodes, and use the manual 
   recovery procedure as a last resort.

.. note::

   Before proceeding, make sure that the irrecoverable nodes are truly dead, and not, 
   for example, temporarily partitioned away due to a network failure. If it is 
   possible for the 'dead' nodes to come back to life, they might communicate and 
   interfere with the recovery procedure and cause unpredictable problems.

   If you have no means of ensuring that these irrecoverable nodes won't come back 
   to life and communicate with the rest of the cluster, setup firewall rules or otherwise 
   isolate your alive nodes to reject any communication attempts from these dead nodes.

During the manual recovery procedure you'll enter a special ``RECOVERY`` mode, remove 
all faulty nodes (using the standard :doc:`node removal procedure </operating-scylla/procedures/cluster-management/remove-node/>`), 
delete the internal Raft data, and restart the cluster. This will cause the cluster to 
perform the Raft upgrade procedure again, initializing the Raft algorithm from scratch.

The manual recovery procedure is applicable both to clusters that were not running Raft 
in the past and then had Raft enabled, and to clusters that were bootstrapped using Raft.

.. note::

   Entering ``RECOVERY`` mode requires a node restart. Restarting an additional node while 
   some nodes are already dead may lead to unavailability of data queries (assuming that 
   you haven't lost it already). For example, if you're using the standard RF=3, 
   CL=QUORUM setup, and you're recovering from a stuck of upgrade procedure because one 
   of your nodes is dead, restarting another node will cause temporary data query 
   unavailability (until the node finishes restarting). Prepare your service for 
   downtime before proceeding.

#. Perform the following query on **every alive node** in the cluster, using e.g. ``cqlsh``:

   .. code-block:: cql

        cqlsh> UPDATE system.scylla_local SET value = 'recovery' WHERE key = 'group0_upgrade_state';

#. Perform a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart/>` of your alive nodes.

#. Verify that all the nodes have entered ``RECOVERY`` mode when restarting; look for one of the following messages in their logs:

    .. code-block:: console

        group0_client - RECOVERY mode.
        raft_group0 - setup_group0: Raft RECOVERY mode, skipping group 0 setup.
        raft_group0_upgrade - RECOVERY mode. Not attempting upgrade.

#. Remove all your dead nodes using the :doc:`node removal procedure </operating-scylla/procedures/cluster-management/remove-node/>`.

#. Remove existing Raft cluster data by performing the following queries on **every alive node** in the cluster, using e.g. ``cqlsh``:

   .. code-block:: cql

        cqlsh> TRUNCATE TABLE system.discovery;
        cqlsh> TRUNCATE TABLE system.group0_history;
        cqlsh> DELETE value FROM system.scylla_local WHERE key = 'raft_group0_id';

#. Make sure that schema is synchronized in the cluster by executing :doc:`nodetool describecluster </operating-scylla/nodetool-commands/describecluster>` on each node and verifying that the schema version is the same on all nodes.

#. We can now leave ``RECOVERY`` mode. On **every alive node**, perform the following query:

   .. code-block:: cql

        cqlsh> DELETE FROM system.scylla_local WHERE key = 'group0_upgrade_state';

#. Perform a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart/>` of your alive nodes.

#. The Raft upgrade procedure will start anew. :ref:`Verify <verify-raft-procedure>` that it finishes successfully.