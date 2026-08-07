Create a ScyllaDB Cluster - Single Datacenter (DC)
****************************************************

-------------
Prerequisites
-------------

.. include:: /operating-scylla/procedures/_common/create_cluster_prerequisites_include.rst

---------
Procedure
---------

1. Install ScyllaDB on the nodes you want to add to the cluster. See :doc:`Getting Started</getting-started/index>` for further instructions.
Follow the ScyllaDB install procedure up to ``scylla.yaml`` configuration phase.

In case the node starts during the process, follow :doc:`these instructions </operating-scylla/procedures/cluster-management/clear-data>` 

2. On each node, edit the ``scylla.yaml`` file to configure the parameters listed below.
The file can be found under ``/etc/scylla/``.

- **cluster_name** - Set the selected cluster_name
- **seeds** - Specify the IP of the node you chose to be a seed node. New nodes will use the IP of this seed node to connect to the cluster and learn the cluster topology and state.
- **listen_address** - IP address that ScyllaDB used to connect to other ScyllaDB nodes in the cluster
- **endpoint_snitch** - Set the selected snitch
- **rpc_address** - Address for client connection (Thrift, CQL)

3. This step needs to be done **only** if you are using the **GossipingPropertyFileSnitch**. If not, skip this step.
In the ``cassandra-rackdc.properties`` file, edit the parameters listed below.
The file can be found under ``/etc/scylla/``

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
   
4.  Start the nodes.

.. include:: /rst_include/scylla-commands-start-index.rst

5. Verify that the nodes have been added to the cluster using
``nodetool status``.

-------
Example
-------

This example shows how to install and configure a three-node cluster using GossipingPropertyFileSnitch as the endpoint_snitch, each node on a different rack.

1. Install three ScyllaDB nodes; the IPs are:

.. code-block:: shell

   192.168.1.201 (seed)
   192.168.1.202 
   192.168.1.203

2. In each ScyllaDB node, edit the ``scylla.yaml`` file

**192.168.1.201**

.. code-block:: shell

   cluster_name: 'Scylla_cluster_demo'
   seeds: "192.168.1.201"
   endpoint_snitch: GossipingPropertyFileSnitch
   rpc_address: "192.168.1.201"
   listen_address: "192.168.1.201"

**192.168.1.202**

.. code-block:: shell

   cluster_name: 'Scylla_cluster_demo'
   seeds: "192.168.1.201"
   endpoint_snitch: GossipingPropertyFileSnitch
   rpc_address: "192.168.1.202"
   listen_address: "192.168.1.202"

**192.168.1.203**

.. code-block:: shell

   cluster_name: 'Scylla_cluster_demo'
   seeds: "192.168.1.201"
   endpoint_snitch: GossipingPropertyFileSnitch
   rpc_address: "192.168.1.203"
   listen_address: "192.168.1.203"

3. This step only needs to be done if you're using **GossipingPropertyFileSnitch**.
   In each ScyllaDB node, edit the ``cassandra-rackdc.properties`` file.

**192.168.1.201**

.. code-block:: shell

   # cassandra-rackdc.properties
   #
   # The lines may include white spaces at the beginning and the end.
   # The rack and data center names may also include white spaces.
   # All trailing and leading white spaces will be trimmed.
   #
   dc=datacenter1
   rack=rack43
   # prefer_local=<false | true>
   # dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>

**192.168.1.202**

.. code-block:: shell

   # cassandra-rackdc.properties
   #
   # The lines may include white spaces at the beginning and the end.
   # The rack and data center names may also include white spaces.
   # All trailing and leading white spaces will be trimmed.
   #
   dc=datacenter1
   rack=rack44
   # prefer_local=<false | true>
   # dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>


**192.168.1.203**

.. code-block:: shell

   # cassandra-rackdc.properties
   #
   # The lines may include white spaces at the beginning and the end.
   # The rack and data center names may also include white spaces.
   # All trailing and leading white spaces will be trimmed.
   #
   dc=datacenter1
   rack=rack45
   # prefer_local=<false | true>
   # dc_suffix=<Data Center name suffix, used by EC2SnitchXXX snitches>

4. Start the nodes. 

.. include:: /rst_include/scylla-commands-start-index.rst

5. Verify that the nodes have been added to the cluster by using the ``nodetool status`` command.

.. code-block:: shell

   Datacenter: datacenter1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   43
   UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   44
   UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   45

