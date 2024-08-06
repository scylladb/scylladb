=================================================================
Adding a New Node Into an Existing ScyllaDB Cluster (Out Scale)
=================================================================

When you add a new node, other nodes in the cluster stream data to the new node. This operation is called bootstrapping and may
be time-consuming, depending on the data size and network bandwidth. If using a :ref:`multi-availability-zone <faq-best-scenario-node-multi-availability-zone>`, make sure they are balanced.

Prerequisites
-------------

Check the Status of Nodes
===========================

You cannot add new nodes to the cluster if any existing node is down.
Before adding new nodes, check the status of the nodes in the cluster using 
:doc:`nodetool status </operating-scylla/nodetool-commands/status>` command. 

Collect Cluster Information
=============================

Log into one of the nodes in the cluster to collect the following information:

    .. include:: /operating-scylla/procedures/cluster-management/_common/prereq.rst


.. _add-node-to-cluster-procedure:


Procedure
---------

#. Install ScyllaDB on the nodes you want to add to the cluster. See :doc:`Getting Started</getting-started/index>` for further instructions. Follow the ScyllaDB installation procedure up to ``scylla.yaml`` configuration phase. Make sure that the ScyllaDB version of the new node is identical to the other nodes in the cluster. 

   If the node starts during the process, follow :doc:`What to do if a Node Starts Automatically </operating-scylla/procedures/cluster-management/clear-data>`.

   .. include:: /operating-scylla/procedures/cluster-management/_common/match_version.rst

   .. include:: /getting-started/_common/note-io.rst

#. On each node, edit the ``scylla.yaml`` file ``/etc/scylla/`` to configure the parameters listed below.

    * **cluster_name** - Specifies the name of the cluster.

    * **listen_address** - Specifies the IP address that ScyllaDB used to connect to the other ScyllaDB nodes in the cluster.

    * **endpoint_snitch** - Specifies the selected snitch.

    * **rpc_address** - Specifies the address for CQL client connections.

    * **seeds** - Specifies the IP address of an existing node in the cluster. The new node will use this IP to connect to the cluster and learn the cluster topology and state.

#. Start the nodes with the following command:

    .. include:: /rst_include/scylla-commands-start-index.rst

#. Verify that the nodes were added to the cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command. Other nodes in the cluster will be streaming data to the new nodes, so the new nodes will be in Up Joining (UJ) status. Wait until the nodes' status changes to Up Normal (UN) - the time depends on the data size and network bandwidth.

   **Example:**

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

#. If you are using ScyllaDB Monitoring, update the `monitoring stack <https://monitoring.docs.scylladb.com/stable/install/monitoring_stack.html#configure-scylla-nodes-from-files>`_ to monitor it. If you are using ScyllaDB Manager, make sure you install the `Manager Agent <https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.html>`_, and Manager can access it.

