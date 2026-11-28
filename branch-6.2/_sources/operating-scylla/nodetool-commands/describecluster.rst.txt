Nodetool describecluster
========================

**describecluster** - Print the name, snitch, partitioner, and schema version of a cluster.

For example:

``nodetool describecluster``

Example output:

.. code-block:: shell

   nodetool describecluster

   Cluster Information:
   	Name: Test Cluster
	Snitch: org.apache.cassandra.locator.SimpleSnitch
	Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	Schema versions:
		86a67fc7-1d7c-3dc3-9be9-9c86b27e2506: [172.17.0.2]

===========  ===============================
Parameter    Description
===========  ===============================
Name         Cluster name
-----------  -------------------------------
Snitch       Snitch used by the cluster
-----------  -------------------------------
Partitioner  Partitioner used by the cluster 
-----------  -------------------------------
Schema       Schema version
===========  ===============================

.. include:: nodetool-index.rst
