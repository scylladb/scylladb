Adding a New Data Center Into an Existing ScyllaDB Cluster
***********************************************************

The following procedure specifies how to add a Data Center (DC) to a live ScyllaDB Cluster, in a single data center, :ref:`multi-availability zone <faq-best-scenario-node-multi-availability-zone>`, or multi-datacenter. Adding a DC out-scales the cluster and provides higher availability (HA).

The procedure includes:

* Install nodes on the new DC.
* Add the new nodes to the cluster.
* Update the replication strategy of the selected keyspace/keyspaces to use with the new DC.
* Rebuild new nodes
* Run full cluster repair
* Update the Monitoring stack  

.. note::

   Make sure to complete the full procedure **before** starting to read from the new datacenter. You should also be aware that the new DC by default will be used for reads.

.. warning::

   The node/nodes you add **must** be clean (no data). Otherwise, you risk data loss. See `Clean Data from Nodes`_ below.

Prerequisites
-------------

#. Log in to one of the nodes in the cluster, collect the following info from the node:

   .. include:: /operating-scylla/procedures/cluster-management/_common/prereq.rst

#. On all client applications, switch the consistency level to ``LOCAL_*`` (LOCAL_ONE, LOCAL_QUORUM,etc.) to prevent the coordinators from accessing the data center you're adding.

#. Install the new **clean** ScyllaDB nodes (See `Clean Data from Nodes`_ below) on the new datacenter, see :doc:`Getting Started </getting-started/index>` for further instructions, create as many nodes that you need.
   Follow the ScyllaDB install procedure up to ``scylla.yaml`` configuration phase.
   In the case that the node starts during the installation process follow :doc:`these instructions </operating-scylla/procedures/cluster-management/clear-data>`.

.. include:: /operating-scylla/procedures/cluster-management/_common/quorum-requirement.rst

Clean Data from Nodes
---------------------

.. include:: /rst_include/clean-data-code.rst


Add New DC
----------

**Procedure**

.. warning::

   Make sure **all** your keyspaces are using ``NetworkTopologyStrategy``. If this is not the case, follow the :doc:`these instructions <update-topology-strategy-from-simple-to-network>` to fix that.

#. For each node in the **existing data-center(s)** edit the ``scylla.yaml`` file to use either

   * ``Ec2MultiRegionSnitch`` - for AWS cloud-based, multi-data-center deployments.

   * ``GossipingPropertyFileSnitch`` - for bare metal and cloud (other than AWS) deployments.

#. Set the DC and Rack Names.
   In the ``cassandra-rackdc.properties`` file edit the parameters listed below.
   The file can be found under ``/etc/scylla/``.

   ``Ec2MultiRegionSnitch``

   Ec2MultiRegionSnitch gives each DC and rack default names, the region name defined as datacenter name, and :ref:`availability zones <faq-best-scenario-node-multi-availability-zone>` are defined as racks within a datacenter.

   For example:

   A node in the ``us-east-1`` region,
   us-east is the data center name, and 1 is the rack location.

   To change the DC names, do the following:

   Edit the ``cassandra-rackdc.properties`` file with the preferred datacenter name.
   The file can be found under ``/etc/scylla/``

   The dc_suffix defines a suffix added to the datacenter name as described below.

   For example:

   Region us-east

   ``dc_suffix=_1_scylla`` will be ``us-east_1_scylla``

   Or

   Region us-west

   ``dc_suffix=_1_scylla`` will be ``us-west_1_scylla``

   ``GossipingPropertyFileSnitch``

   - **dc** - Set the datacenter name
   - **rack** - Set the rack name

   For example:

   .. code-block:: shell
   
      # cassandra-rackdc.properties
      #
      # The lines may include white spaces at the beginning and the end.
      # The rack and data center names may also include white spaces.
      # All trailing and leading white spaces will be trimmed.
      #
      dc=thedatacentername
      rack=therackname
      # prefer_local=<false | true>
      # dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>

#. In the **existing datacenter(s)**, restart the ScyllaDB nodes one by one.

   .. include:: /rst_include/scylla-commands-restart-index.rst

#. For each node in the **new datacenter** edit the ``scylla.yaml`` file parameters listed below,
   the file can be found under ``/etc/scylla/``.

   * **cluster_name** - Set the selected cluster_name.
   * **seeds** - IP address of an existing node (or nodes).
   * **listen_address** - IP address that ScyllaDB used to connect to the other ScyllaDB nodes in the cluster.
   * **endpoint_snitch** - Set the selected snitch.
   * **rpc_address** - Address for CQL client connections.

   The parameters ``seeds``, ``cluster_name`` and ``endpoint_snitch`` need to match the existing cluster.

#. In the **new datacenter**, set the DC and Rack Names (see step number **three** for more details).

#. In the **new datacenter**, start the ScyllaDB nodes one by one.

   .. include:: /rst_include/scylla-commands-start-index.rst

#. Verify that the nodes were added to the cluster using ``nodetool status``.

   For example:

   .. code-block:: shell

      $ nodetool status

      Datacenter: US-DC
      =========================
      Status=Up/Down
      |/ State=Normal/Leaving/Joining/Moving
      --   Address         Load            Tokens  Owns            Host ID                                 Rack
      UN   54.191.2.121    120.97 KB       256     ?               c84b80ea-cb60-422b-bc72-fa86ede4ac2e    RACK1
      UN   54.191.72.56    109.54 KB       256     ?               129087eb-9aea-4af6-92c6-99fdadb39c33    RACK1
      UN   54.187.25.99    104.94 KB       256     ?               0540c7d7-2622-4f1f-a3f0-acb39282e0fc    RACK1
   
      Datacenter: ASIA-DC
      =======================
      Status=Up/Down
      |/ State=Normal/Leaving/Joining/Moving
      --   Address         Load            Tokens  Owns            Host ID                                 Rack
      UN   54.160.174.243  109.54 KB       256     ?               c7686ffd-7a5b-4124-858e-df2e61130aaa    RACK1
      UN   54.235.9.159    109.75 KB       256     ?               39798227-9f6f-4868-8193-08570856c09a    RACK1
      UN   54.146.228.25   128.33 KB       256     ?               7a4957a1-9590-4434-9746-9c8a6f796a0c    RACK1

.. TODO possibly provide additional information WRT how ALTER works with tablets

#. When all nodes are up and running ``ALTER`` the following Keyspaces in the new nodes:

   * Keyspace created by the user (which needed to replicate to the new DC).
   * System: ``system_distributed``, ``system_traces``, for example, replicate the data to three nodes in the new DC.

   For example:

   Before

   .. code-block:: cql

      DESCRIBE KEYSPACE mykeyspace;

      CREATE KEYSPACE mykeyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3};

   ALTER Command

   .. code-block:: cql

      ALTER KEYSPACE mykeyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
      ALTER KEYSPACE system_distributed WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
      ALTER KEYSPACE system_traces WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};

   After

   .. code-block:: cql

      DESCRIBE KEYSPACE mykeyspace;
      CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class’: 'NetworkTopologyStrategy', <exiting_dc>:3, <new_dc>: 3};
      CREATE KEYSPACE system_distributed WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};
      CREATE KEYSPACE system_traces WITH replication = { 'class' : 'NetworkTopologyStrategy', '<exiting_dc>' : 3, <new_dc> : 3};

#. Run ``nodetool rebuild`` on each node in the new datacenter, specify the existing datacenter name in the rebuild command.

   For example:

   ``nodetool rebuild <existing_data_center_name>``

   The rebuild ensures that the new nodes that were just added to the cluster will recognize the existing datacenters in the cluster.

#. Run a full cluster repair, using :doc:`nodetool repair -pr </operating-scylla/nodetool-commands/repair>` on each node, or using `ScyllaDB Manager ad-hoc repair <https://manager.docs.scylladb.com/stable/repair>`_

#. If you are using ScyllaDB Monitoring, update the `monitoring stack <https://monitoring.docs.scylladb.com/stable/install/monitoring_stack.html#configure-scylla-nodes-from-files>`_ to monitor it. If you are using ScyllaDB Manager, make sure you install the `Manager Agent <https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.html>`_ and Manager can access the new DC.


Configure the Client not to Connect to the New DC
-------------------------------------------------

This procedure will help your clients not to connect to the DC you just added.

The example below is for clients using the Java driver. Modify this example to suit your needs.

#. One way to prevent clients from connecting to the new DC is to temporarily restrict them to only use the nodes which are in the same DC as the clients.
   This can be done by ensuring the operations are performed with CL=LOCAL_* (for example LOCAL_QUORUM) and using
   ``DCAwareRoundRobinPolicy`` created by calling ``withLocalDc("dcLocalToTheClient)`` and ``withUsedHostsPerRemoteDc(0)`` on ``DCAwareRoundRobinPolicy.Builder``.

   Example of DCAwareRoundRobinPolicy creation:

   .. code-block:: java

      variable = DCAwareRoundRobinPolicy.builder()
          .withLocalDc("<name of DC local to the client being configured>")
          .withUsedHostsPerRemoteDc(0)
          .build();

#. Once the new DC is operational you can remove "withUsedHostsPerRemoteDc(0)" from the configuration and change CL to the previous value.

Additional Resources for Java Clients
=====================================

* `DCAwareRoundRobinPolicy.Builder <https://java-driver.docs.scylladb.com/scylla-3.10.2.x/api/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.Builder.html>`_
* `DCAwareRoundRobinPolicy <https://java-driver.docs.scylladb.com/scylla-3.10.2.x/api/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html>`_

