Create a ScyllaDB Cluster on EC2 (Single or Multi Data Center)
===============================================================

The easiest way to run a Scylla cluster on EC2 is by using `Scylla AMI <https://www.scylladb.com/download/?platform=aws>`_, which is Ubuntu-based. 
To use a different OS or your own `AMI <https://en.wikipedia.org/wiki/Amazon_Machine_Image>`_ (Amazon Machine Image) or set up a multi DC Scylla cluster,
you need to configure the Scylla cluster on your own. This page guides you through this process.

A Scylla cluster on EC2 can be deployed as a single-DC cluster or a multi-DC cluster. The table below describes how to configure parameters in the ``scylla.yaml`` file for each node in your cluster for both cluster types.

For more information on Scylla AMI and the configuration of parameters in ``scylla.yaml`` from the EC2 user data, see `Scylla Machine Image <https://github.com/scylladb/scylla-machine-image>`_.

The best practice is to use each EC2 region as a Scylla DC. In such a case, nodes communicate using Internal (Private) IPs inside the region and using External (Public) IPs between regions (Data Centers).

For further information, see `AWS instance addressing <http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html>`_.

.. _table:

EC2 Configuration Table
-----------------------

=====================  ====================  ====================
Parameter              Single DC             Multi DC 
=====================  ====================  ====================
seeds	               Internal IP address   External IP address                
---------------------  --------------------  --------------------  
listen_address         Internal IP address   Internal IP address          
---------------------  --------------------  --------------------  
rpc_address            Internal IP address   Internal IP address      
---------------------  --------------------  --------------------
broadcast_address      Internal IP address   External IP address
---------------------  --------------------  --------------------
broadcast_rpc_address  Internal IP address   External IP address
---------------------  --------------------  --------------------
endpoint_snitch        Ec2Snitch             Ec2MultiRegionSnitch
=====================  ====================  ====================

Prerequisites
-------------

* EC2 instance with SSH access.

* Ensure that all the relevant :ref:`ports <cqlsh-networking>` are open in your EC2 Security Group.

* Select a unique name as ``cluster_name`` for the cluster (identical for all the nodes in the cluster).

* Choose one of the nodes to be a seed node. You'll need to provide the IP of that node using 
  the ``seeds`` parameter in the ``scylla.yaml`` configuration file on each node.

Procedure
---------

#. Install ScyllaDB on the nodes you want to add to the cluster. See :doc:`Getting Started</getting-started/index>` for installation instructions and
   follow the procedure up to  the ``scylla.yaml`` configuration phase.

   If the Scylla service is already running (for example, if you are using `Scylla AMI`_), stop it before moving to the next step by using :doc:`these instructions </operating-scylla/procedures/cluster-management/clear-data>`.

#. On each node, edit the ``scylla.yaml`` file in ``/etc/scylla/`` to configure the parameters listed below. See the :ref:`table` above on how to configure your cluster.

     * **cluster_name** - Set the selected cluster_name.
     * **seeds** - Specify the IP of the node you chose to be a seed node. See :doc:`Scylla Seed Nodes </kb/seed-nodes/>` for details.
     * **listen_address** - IP address that Scylla used to connect to other Scylla nodes in the cluster.
     * **endpoint_snitch** - Set the selected snitch.
     * **rpc_address** - Address for client connection (Thrift, CQL).
     * **broadcast_address** - The IP address a node tells other nodes in the cluster to contact it by.
     * **broadcast_rpc_address** - Default: unset. The RPC address to broadcast to drivers and other Scylla nodes. It cannot be set to 0.0.0.0. If left blank, it will be set to the value of ``rpc_address``. If ``rpc_address`` is set to 0.0.0.0, ``broadcast_rpc_address`` must be explicitly configured.

#. Start the nodes.

   .. include:: /rst_include/scylla-commands-start-index.rst

#. Verify that the node has been added to the cluster using
   ``nodetool status``.

EC2snitch's Default DC and Rack Names, and how to Override DC Names
...................................................................

EC2snitch and Ec2MultiRegionSnitch give each DC and rack default names. The region name is defined as the datacenter name, and :ref:`availability zones <faq-best-scenario-node-multi-availability-zone>` are defined as racks within a datacenter. The rack names cannot be changed.

Example
^^^^^^^

For a node in the ``us-east-1`` region, ``us-east`` is the datacenter name and ``1`` is the rack. 

To change the name of the datacenter, open the ``cassandra-rackdc.properties`` file located in ``/etc/scylla/`` and edit the DC name.

The ``dc_suffix`` defines a suffix added to the datacenter name. For example:

* for region us-east and suffix ``dc_suffix=_1_scylla``, it will be ``us-east_1_scylla``.
* for region us-west and suffix ``dc_suffix=_1_scylla``, it will be ``us-west_1_scylla``.



