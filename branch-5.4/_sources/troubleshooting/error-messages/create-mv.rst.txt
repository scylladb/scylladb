A Removed Node was not Removed Properly from the Seed Node List
===============================================================

Phenonoma
^^^^^^^^^

Failed to create :doc:`materialized view </cql/mv>` after node was removed from the cluster. 


Error message:

.. code-block:: shell

   InvalidRequest: Error from server: code=2200 [Invalid query] message="Can't create materialized views until the whole cluster has been upgraded"

Problem
^^^^^^^

A removed node was not removed properly from the seed node list.

Scylla Open Source 4.3 and later and Scylla Enterprise 2021.1 and later are seedless. See :doc:`Scylla Seed Nodes </kb/seed-nodes/>` for details.
This problem may occur in an earlier version of Scylla.

How to Verify
^^^^^^^^^^^^^

Scylla logs show the error message above.

To verify that the node wasn't remove properly use the :doc:`nodetool gossipinfo </operating-scylla/nodetool-commands/gossipinfo>` command

For example:

A three nodes cluster, with one node (54.62.0.101) removed.

.. code-block:: shell

   nodetool gossipinfo

   /54.62.0.99
   generation:1172279348
   heartbeat:7212
   LOAD:2.0293227179E10
   INTERNAL_IP:10.240.0.83
   DC:E1
   STATUS:NORMAL,-872190912874367364312
   HOST_ID:12fdcf43-4642-53b1-a987-c0e825e4e10a
   RPC_ADDRESS:10.240.0.83
   RACK:R1

   /54.62.0.100
   generation:1657463198
   heartbeat:8135
   LOAD:2.0114638716E12
   INTERNAL_IP:10.240.0.93
   DC:E1
   STATUS:NORMAL,-258152127640110957173
   HOST_ID:99acbh55-1013-24a1-a987-s1w718c1e01b
   RPC_ADDRESS:10.240.0.93
   RACK:R1

   /54.62.0.101
   generation:1657463198
   heartbeat:7022
   LOAD:2.5173672157E48
   INTERNAL_IP:10.240.0.103
   DC:E1
   STATUS:NORMAL,-365481201980413697284
   HOST_ID:99acbh55-1301-55a1-a628-s4w254c1e01b
   RPC_ADDRESS:10.240.0.103
   RACK:R1

We can see that node ``54.62.0.101`` is still part of the cluster and needs to be removed.
  
Solution
^^^^^^^^

Remove the relevant node from the other nodes seed list (under scylla.yaml) and restart the nodes one by one.

For example:

Seed list before remove the node

.. code-block:: shell

   - seeds: "10.240.0.83,10.240.0.93,10.240.0.103" 

Seed list after removing the node

.. code-block:: shell

   - seeds: "10.240.0.83,10.240.0.93" 

Restart Scylla nodes

.. include:: /rst_include/scylla-commands-restart-index.rst
