ScyllaDB Snitches
==================

Snitches are used in the following ways:

* To determine to which datacenters and racks the ScyllaDB nodes belong to
* To inform ScyllaDB about the network topology so that requests are routed efficiently
* To allow ScyllaDB to distribute replicas by grouping machines into data centers and racks.

Note, that if you do not choose a Snitch when creating a ScyllaDB cluster, the SimpleSnitch is selected by default.

ScyllaDB supports the following snitches:

* SimpleSnitch_
* RackInferringSnitch_
* GossipingPropertyFileSnitch_
* Ec2Snitch_
* Ec2MultiRegionSnitch_
* GoogleCloudSnitch_
* AzureSnitch_

.. note::

   For production clusters, it is strongly recommended to use ``GossipingPropertyFileSnitch`` or ``Ec2MultiRegionSnitch``.
   Other snitches are limited and will make it harder for you to add a Data Center (DC) later.   

.. warning::

   Do not disable access to instance metadata if you're using Ec2Snitch or Ec2MultiRegionSnitch. With access to medatata disabled,
   the information about datacenter names and racks may be missing or incorrect, or the instance may fail to boot. 

.. _snitch-simple-snitch:

SimpleSnitch
............
Use the SimpleSnitch when working with single cluster deployments and all the nodes are under the same datacenter.
The SimpleSnitch binds all the nodes to the same Rack and datacenter and is recommended to be used only in single datacenter deployments.

.. _snitch-rack-inferring-snitch:

RackInferringSnitch
...................

RackInferringSnitch binds nodes to DCs and racks according to their broadcast IP addresses.


For Example:

If a node has a Broadcast IP 192.168.100.200; then it would belong to a DC '168' and Rack '100'.

.. _snitch-gossiping-property-file-snitch:

GossipingPropertyFileSnitch 
...........................


Use the GossipingPropertyFileSnitch when working with multi-cluster deployments where the nodes are in various datacenters.
It is recommended to use the GossipingPropertyFileSnitch in production installations.
This snitch allows ScyllaDB to explicitly define which DC and rack a specific node belongs to.
In addition, it reads its configuration from the ``cassandra-rackdc.properties`` file, which is located in the ``/etc/scylla/`` directory.

For Example:

.. code-block:: shell

   prefer_local=true 
   dc=my_data_center 
   rack=my_rack


Setting *prefer_local* to *true* instructs ScyllaDB to use an internal IP address for interactions with nodes in the same DC.

An example use case is when your host uses different addresses for LAN and WAN sessions. You want your cluster to be accessible by clients *outside* the ScyllaDB nodes’ LAN while still allowing ScyllaDB nodes to communicate over internal LAN keeping latency low. In AWS, this is similar to a VM’s “Public” and “Private” addresses.
To set an internal and external address, set a LAN address as a *listen_address* and use a WAN address as a *broadcast_address*.

If you set ``prefer_local: true`` nodes in the same DC would use their LAN addresses to communicate with each other and WAN addresses to access nodes in different DCs and communicate with Clients.

.. _snitch-ec2-snitch:

Ec2Snitch
.........

Use the Ec2Snitch when working on EC2 with a single cluster deployments where all nodes are located in the same region.
This basic snitch reads its configuration from Amazon's EC2 registry services.
When using EC2, the region name is treated as the datacenter name and availability zones are treated as racks within a datacenter.
If the setup includes a single datacenter, there is no need to specify any parameters.
As private IPs are used, this snitch does not work well across multiple regions.
It should also be noted that according to this snitch, a DC is a region and if a region is down, the entire cluster will be down.

If you are working with multiple datacenters, specify the DC and set the parameter ``dc_suffix=<DCNAME>`` in the ``cassandra-rackdc.properties`` file, which is located in the ``/etc/scylla/`` directory.


For Example, suppose you had created a 5 node cluster and added the following configuration settings to each node's ``/etc/scylla/cassandra-rackdc.properties`` file as shown:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Node number
     - Parameter to add to the specific node's /etc/scylla/cassandra-rackdc.properties
   * - 1
     - dc_suffix=_dc1-europe
   * - 2
     - dc_suffix=_dc1-europe
   * - 3
     - dc_suffix=_dc2-asia
   * - 4
     - dc_suffix=_dc2-asia
   * - 5
     - dc_suffix=_dc3-australia  

This action adds a suffix to the name of each of the datacenters for the region.

Running the ``nodetool status`` command shows all three datacenters:

.. code-block:: none
  
   Datacenter: us-east_dc1-europe
   ==============================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address     Load       Tokens       Owns    Host ID                               Rack
   UN  172.20.0.4  111.23 KB  256          ?       eaabc5db-61ff-419b-b1a7-f70af23edb1b  Rack1
   UN  172.20.0.5  127.09 KB  256          ?       bace1b4e-67c6-4bdb-8eba-398162b7b56e  Rack1
   Datacenter: us-east_dc2-asia
   ============================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address     Load       Tokens       Owns    Host ID                               Rack
   UN  172.20.0.6  110.59 KB  256          ?       bda5fb11-9369-48fb-91be-82c8d821f758  Rack1
   UN  172.20.0.3  111.26 KB  256          ?       b9ea3516-5e1e-4ffb-abff-c6a6701cb41b  Rack1
   Datacenter: us-east_dc3-australia
   =================================
   Status=Up/Down   
   |/ State=Normal/Leaving/Joining/Moving
   --  Address     Load       Tokens       Owns    Host ID                               Rack
   UN  172.20.0.7  111.23 KB  256          ?       eaabc5db-61ff-419b-b1a7-f70af23edb1b  Rack1

.. note::

   The datacenter naming convention in this example is based on location.
   You can use other conventions, such as DC1, DC2 or 100, 200, or analytics, search, ScyllaDB, and more.
   Providing a separator such as a dash keeps the name of the DC readable as the ``dc_suffix`` property adds the suffix to the DC name.

.. note::

   Ec2Snitch and Ec2MultiRegionSnitch will define DC/RACK differently for AWS Availability Zones (AZs) that end with ``1x`` compared to other AZs:

   * For the former class of AZs, e.g. ``us-east-1d``, the Snitch will set ``DC='us-east'``, ``RACK='1d'``
   * For the latter class of AZs, e.g. ``us-east-4c``, the Snitch will set ``DC='us-east-4'``, ``RACK='4c'``

.. _snitch-ec2-multi-region-snitch:

Ec2MultiRegionSnitch
....................

Use the Ec2MultiRegionSnitch when working on EC2 and using multi-cluster deployments where the nodes are in various regions.
This snitch works like the Ec2Snitch, but in addition, it sets the node's ``broadcast_address`` and ``broadcast_rpc_address`` to the node's public IP address.
This setting allows nodes from other zones to communicate with the node regardless of what is configured in the node's :doc:`scylla.yaml </operating-scylla/admin>` configuration file for ``broadcast_address`` and ``broadcast_rpc_address`` parameters.

``Ec2MultiRegionSnitch`` also unconditionally imposes the "prefer local" policy on a node (similar to GossipingPropertyFileSnitch when ``prefer_local`` is set to ``true``).

In EC2, the region name is treated as the datacenter name and availability zones are treated as racks within a datacenter. 

To change the DC and rack names, do the following:

Edit the ``cassandra-rackdc.properties`` file with the preferred datacenter name. The file can be found under ``/etc/scylla/``
The ``dc_suffix`` defines a suffix added to the datacenter name as described below.

For Example:

Node - region ``DC='us-west'`` and Rack ``Rack='1'`` will be ``us-west-1`` dc_suffix= ``scylla_node_west``

Node - region ``DC='us-east'`` and Rack ``Rack='2'`` will be ``us-east-2`` dc_suffix= ``scylla_node_east``

.. code-block:: shell

   us-west-1_scylla_node_west
   us-east-2_scylla_node_east

.. _googlecloudsnitch:

GoogleCloudSnitch
.................

Use the GoogleCloudSnitch for deploying ScyllaDB on the Google Cloud Engine (GCE) platform across one or more regions.
The region is treated as a datacenter, and the availability zones are treated as racks within the datacenter.
All communication occurs over private IP addresses within the same logical network.

To use the GoogleCloudSnitch, add the snitch name to the :doc:`scylla.yaml </operating-scylla/admin>` file, which is located in the ``/etc/scylla/`` directory for **all nodes** in the cluster.

You can add a suffix to the data center name as an additional identifier. This suffix is appended to the Zone name without adding any spaces. To add this suffix edit the ``cassandra-rackdc.properties`` file, which can be found under ``/etc/scylla/`` and set the ``dc_suffix`` with an appropriate text string. It may help to add an underscore or dash in front. Keep in mind that this property file is used for all ScyllaDB snitches. When using GoogleCloudSnitch, all other properties are ignored.



Example
^^^^^^^^

You have two datacenters running on GCE. One is for the office in Miami and is in region **us-east1**, zone **us-east-1-b**.
The other office is in Portland and is in region **us-west1**,, zone **us-west-1-b**.

It's important to note that:

* DC1 is us-east1 with rack name b
* DC2 is us-west1 with rack b

Racks are important for distributing replicas, but not for datacenter naming as this Snitch can work across multiple regions without additional configuration.

After creating the instances on GCE, edit the :doc:`scylla.yaml </operating-scylla/admin>` file to select the GoogleCloudSnitch.

.. code-block:: none

   endpoint_snitch: GoogleCloudSnitch

As you want to set the data center suffix for the nodes in each datacenter, you open each node's properties file in the ``cassandra-rackdc.properties`` The file can be found under ``/etc/scylla/``.
You set the following parameters for Miami:

.. code-block:: none

   # node 1 - 192.0.2.2 (you use the same properties for node #2 (192.0.2.3) and #3 (192.0.2.4)) 

   dc_suffix=_scylla_node_Miami
   
and for Portland:  

.. code-block:: none

   # node 4 192.0.2.5 

   dc_suffix=_scylla_node_Portland

Start the cluster, one node at a time, and then run ``nodetool status`` to check connectivity. 

.. code-block:: shell

   nodetool status

   Datacenter: us-east1_scylla_node_Miami
   ======================================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address       Load       Tokens       Owns    Host ID                               Rack
   UN  192.0.2.2     1.27 MB    256          ?       5b1d864f-a026-4076-bb19-3e7dd693abf1  b
   UN  192.0.2.3     954.89 KB  256          ?       783a815e-6e9d-4ab5-a092-bbf15fd76a9f  b
   UN  192.0.2.4     1.02 MB    256          ?       1edf5b52-6ae3-41c1-9ec1-c431d34a1aa1  b

   Datacenter: us-west1_scylla_node_Portland
   ======================================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address       Load       Tokens       Owns    Host ID                               Rack
   UN  192.0.2.5     670.16 KB  256          ?       f0a44a49-0035-4146-8fdc-30e66c037f95  b


.. _azuresnitch:

AzureSnitch
.................

Use the AzureSnitch for deploying ScyllaDB on the Azure platform across one or more locations.
The location is treated as a datacenter, and the availability zones are treated as racks within the datacenter.
All communication occurs over private IP addresses within the same logical network.

To use the AzureSnitch, add the snitch name to the :doc:`scylla.yaml </operating-scylla/admin>` file, which is located in the ``/etc/scylla/`` directory, for **all nodes** in the cluster:

.. code-block:: none

   endpoint_snitch: AzureSnitch


Optionally, add a suffix to the datacenter name as an additional identifier. To add the suffix, set the ``dc_suffix`` property in 
the ``cassandra-rackdc.properties`` file, which can be found in ``/etc/scylla/``.

The suffix is appended to the Zone name without adding any spaces, so consider adding an underscore or dash as 
the first character in the suffix. For example:
 
.. code-block:: none

   dc_suffix=_scylladb_node_2022

Keep in mind that this property file is used for all ScyllaDB snitches. When using AzureSnitch, all other properties are ignored.


Example
^^^^^^^^

In the following example, there are two datacenters running on Azure. One is for the office in Miami and is in **us-east1** location, **us-east-1-b** zone.
The other office is in Portland and is in **us-west1** location, **us-west-1-b** zone.

It's important to note that:

* DC1 is us-east1 with rack name b
* DC2 is us-west1 with rack b

Racks are important for distributing replicas, but not for datacenter naming, as this Snitch can work across multiple locations without additional configuration.

After creating the instances on Azure, edit the :doc:`scylla.yaml </operating-scylla/admin>` file to select the AzureSnitch.

.. code-block:: none

   endpoint_snitch: AzureSnitch

To set the datacenter suffix for the nodes in each datacenter, open each node's properties file in the ``cassandra-rackdc.properties``. The file can be found under ``/etc/scylla/``.
Set the following parameters for Miami:

.. code-block:: none

   # node 1 - 192.0.2.2 (use the same properties for node #2 (192.0.2.3) and #3 (192.0.2.4)) 

   dc_suffix=_scylla_node_Miami
   
and for Portland:  

.. code-block:: none

   # node 4 192.0.2.5 

   dc_suffix=_scylla_node_Portland

Start the cluster, one node at a time, and then run ``nodetool status`` to check connectivity. 

.. code-block:: shell

   nodetool status

   Datacenter: us-east1_scylla_node_Miami
   ======================================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address       Load       Tokens       Owns    Host ID                               Rack
   UN  192.0.2.2     1.27 MB    256          ?       5b1d864f-a026-4076-bb19-3e7dd693abf1  b
   UN  192.0.2.3     954.89 KB  256          ?       783a815e-6e9d-4ab5-a092-bbf15fd76a9f  b
   UN  192.0.2.4     1.02 MB    256          ?       1edf5b52-6ae3-41c1-9ec1-c431d34a1aa1  b

   Datacenter: us-west1_scylla_node_Portland
   ======================================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address       Load       Tokens       Owns    Host ID                               Rack
   UN  192.0.2.5     670.16 KB  256          ?       f0a44a49-0035-4146-8fdc-30e66c037f95  b


Related Topics
.................

.. include:: /rst_include/advance-index.rst
