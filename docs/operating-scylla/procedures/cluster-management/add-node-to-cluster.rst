=================================================================
Adding a New Node Into an Existing ScyllaDB Cluster (Out Scale)
=================================================================

When you add a new node, other nodes in the cluster stream data to the new node. This operation is called bootstrapping and may
be time-consuming, depending on the data size and network bandwidth. If using a :ref:`multi-availability-zone <faq-best-scenario-node-multi-availability-zone>`, make sure they are balanced.


Prerequisites
-------------

#. Before adding the new node, check the node's status in the cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command. You cannot add new nodes to the cluster if any nodes are down.

    For example:

    .. code-block:: shell

       Datacenter: DC1
       Status=Up/Down
       State=Normal/Leaving/Joining/Moving
       --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
       UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
       DN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1

    In the example above,  the node with the IP address 192.168.1.202 has a status of Down (DN). To proceed, you need to start the node that is down or :doc:`remove it from the cluster </operating-scylla/procedures/cluster-management/remove-node/>`.

#. Login to one of the nodes in the cluster to collect the following information:

    .. include:: /operating-scylla/procedures/cluster-management/_common/prereq.rst

    .. _add-node-to-cluster-procedure:


Procedure
---------

#. Install Scylla on a new node. See :doc:`Getting Started</getting-started/index>` for further instructions - follow the Scylla installation procedure up to ``scylla.yaml`` configuration phase. Make sure that the Scylla version of the new node is identical to the other nodes in the cluster. 

    If the node starts during the process, follow :doc:`What to do if a Node Starts Automatically </operating-scylla/procedures/cluster-management/clear-data>`.

    .. include:: /operating-scylla/procedures/cluster-management/_common/match_version.rst

    .. include:: /getting-started/_common/note-io.rst

#. In the ``scylla.yaml`` file in ``/etc/scylla/``, edit the following parameters:

    * **cluster_name** - Specifies the name of the cluster.

    * **listen_address** - Specifies the IP address that Scylla used to connect to the other Scylla nodes in the cluster.

    * **endpoint_snitch** - Specifies the selected snitch.

    * **rpc_address** - Specifies the address for client connections (Thrift, CQL).

    * **seeds** - Specifies the IP address of an existing node in the cluster. The new node will use this IP to connect to the cluster and learn the cluster topology and state.

    * **consistent_cluster_management** - set to the same value as used by your existing nodes.

   .. note:: 

       In earlier versions of ScyllaDB, seed nodes assisted in gossip. Starting with Scylla Open Source 4.3 and Scylla Enterprise 2021.1, the seed concept in gossip has been removed. If you are using an earlier version of ScyllaDB, you need to configure the seeds parameter in the following way:
   
       * Specify the list of the current seed nodes in the cluster.
       * Do not list the node you're adding as a seed node.

       See :doc:`Scylla Seed Nodes</kb/seed-nodes>` for more information.

       We recommend updating your ScyllaDB to version 4.3 or later (Open Source) or 2021.1 or later (Enterprise).

#. Start the ScyllaDB node with the following command:

    .. include:: /rst_include/scylla-commands-start-index.rst

#. Verify that the node was added to the cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command. Other nodes in the cluster will be streaming data to the new node, so the new node will be in Up Joining (UJ) status. Wait until the node's status changes to Up Normal (UN) - the time depends on the data size and network bandwidth.

    **For example:**

    Nodes in the cluster are streaming data to the new node:

    .. code-block:: shell

       Datacenter: DC1
       Status=Up/Down
       State=Normal/Leaving/Joining/Moving
       --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
       UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
       UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
       UJ  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

   Nodes in the cluster finished streaming data to the new node:

   .. code-block:: shell

        Datacenter: DC1
        Status=Up/Down
        State=Normal/Leaving/Joining/Moving
        --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
        UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
        UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
        UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

#. When the new node status is Up Normal (UN), run the :doc:`nodetool cleanup </operating-scylla/nodetool-commands/cleanup>` command on all nodes in the cluster except for the new node that has just been added. Cleanup removes keys that were streamed to the newly added node and are no longer owned by the node.

    .. note::

       To prevent data resurrection, it's essential to complete cleanup after adding nodes and before any node is decommissioned or removed.
       However, cleanup may consume significant resources. Use the following guideline to reduce cleanup impact:

       Tip 1: When adding multiple nodes, run the cleanup operations after all nodes are added on all nodes but the last one to be added.

       Tip 2: Postpone cleanup to low demand hours while ensuring it completes successfully before any node is decommissioned or removed.

       Tip 3: Run cleanup one node at a time, reducing overall cluster impact.

#. Wait until the new node becomes UN (Up Normal) in the output of :doc:`nodetool status </operating-scylla/nodetool-commands/status>` on one of the old nodes. 

    .. note:: 
       If you are using ScyllaDB Open Source 4.3 or later or ScyllaDB Enterprise 2021.1 or later and configure the list of seed nodes to participate in gossip, you can now edit the ``scylla.yaml`` files to add the new node as a seed node.
       You don't need to restart the Scylla service after modifying the seeds list in ``scylla.yaml``.

#. If you are using Scylla Monitoring, update the `monitoring stack <https://monitoring.docs.scylladb.com/stable/install/monitoring_stack.html#configure-scylla-nodes-from-files>`_ to monitor it. If you are using Scylla Manager, make sure you install the `Manager Agent <https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.html>`_, and Manager can access it.

Handling Failures
=================

If the node starts bootstrapping but then fails in the middle e.g. due to a power loss, you can retry bootstrap (by restarting the node). If you don't want to retry, or the node refuses to boot on subsequent attempts, consult the :doc:`Handling Membership Change Failures document</operating-scylla/procedures/cluster-management/handling-membership-change-failures>`.
