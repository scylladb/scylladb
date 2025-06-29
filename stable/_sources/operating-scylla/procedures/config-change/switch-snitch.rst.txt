
How to Switch Snitches
**********************
.. REMOVE IN FUTURE VERSIONS - when the limitation in the note is no longer valid

.. note::

    Switching from one type of snitch to another is NOT supported for clusters 
    where one or more keyspaces have tablets enabled. 
    
    NOTE: If you :ref:`create a new keyspace <create-keyspace-statement>`, 
    it has tablets enabled by default.

This procedure describes the steps that need to be done when switching from one type of snitch to another.
Such a scenario can be when increasing the cluster and adding more data-centers in different locations. 
Snitches are responsible for specifying how ScyllaDB distributes the replicas. The procedure is dependent on any changes in the cluster topology.

**Note** - Switching a snitch requires a full cluster shutdown, so It is highly recommended to choose the :doc:`right snitch </operating-scylla/system-configuration/snitch>` for your needs at the cluster setup phase.

+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
|Cluster Status	                                                                     |Needed Procedure	                                                                  |
+====================================================================================+====================================================================================+
|No change in network topology                                                       |Set the new snitch                                                                  |
+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
|Network topology was changed                                                        |Set the new snitch and run repair                                                   | 
+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+


Changes in network topology mean that there are changes in the racks or data-centers where the nodes are located. 

For example:

**No topology changes**

Original cluster: three nodes cluster on a single data-center with :ref:`Simplesnitch <snitch-simple-snitch>` or :ref:`Ec2snitch <snitch-ec2-snitch>`. 


Change to: three nodes in one data-center and one rack using a :ref:`GossipingPropertyFileSnitch <snitch-gossiping-property-file-snitch>` or :ref:`Ec2multiregionsnitch <snitch-ec2-multi-region-snitch>`.

**Topology changes**

Original cluster: three nodes using the :ref:`Simplesnitch <snitch-simple-snitch>` or :ref:`Ec2snitch <snitch-ec2-snitch>` in a single data-center.

Change to: nine nodes in two data-centers using the :ref:`GossipingPropertyFileSnitch <snitch-gossiping-property-file-snitch>` or :ref:`Ec2multiregionsnitch <snitch-ec2-multi-region-snitch>` (add a new data-center).

---------
Procedure
---------

1. Stop all the nodes in the cluster.

.. include:: /rst_include/scylla-commands-stop-index.rst

2. In the ``scylla.yaml`` file edit the endpoint_snitch. The file can be found under ``/etc/scylla/``. Change the endpoint_snitch to all the nodes in the cluster.

For example:

``endpoint_snitch: GossipingPropertyFileSnitch``

3. In the ``cassandra-rackdc.properties`` file edit the rack and data-center information.  

For example, ``Ec2MultiRegionSnitch``:

A node in the ``us-east-1`` region, us-east is the data center name, and 1 is the rack location. 

4. Start all the nodes in the cluster in parallel.

.. include:: /rst_include/scylla-commands-start-index.rst

5. Run full repair (consult with the table above if this action is needed).

