Remove a Data-Center from a ScyllaDB Cluster
*********************************************

Before removing (decommission) the data-center from the cluster, it is essential to verify that the data-center that is about to be
removed does not hold any unique data.


Prerequisites
-------------

* Verify that there are no client writes to nodes in the data-center that is to be decommissioned.
* Check the cluster status using the :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command to understand the cluster deployment.

.. code-block:: shell

   Datacenter: US-DC
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-lac8-23fddce9123e   B1
   UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

   Datacenter: ASIA-DC
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  50.191.1.204  112.82 KB  256     32.7%             4d5ed9f4-7764-4ded-dad8-63fdace94b7c   B1
   UN  50.191.1.205  91.11 KB   256     32.9%             145id9f4-7777-1dvn-nac8-83fdzce917r4   B1
   UN  50.191.1.206  124.42 KB  256     32.6%             777ed9f4-6564-6dsd-can8-13fdxce999gy   B1

   Datacenter: EUROPE-DC
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  172.91.202.31  112.82 KB  256     32.7%             1d5ed8f4-7764-4dbd-rad8-44fddce94b7v   B1
   UN  172.91.202.32  91.11 KB   256     32.9%             525ed7g4-7437-1dbn-mac8-53fddce9123c   B1
   UN  172.91.202.33  124.42 KB  256     32.6%             975edbm4-6564-63bd-san8-73fddce952ga   B1

Procedure
---------

#. Run the ``nodetool repair -pr`` command on each node in the data-center that is going to be decommissioned. This will verify that all the data is in sync between the decommissioned data-center and the other data-centers in the cluster.

   For example:

   If the ASIA-DC cluster is to be removed, then, run the ``nodetool repair -pr`` command on all the nodes in the ASIA-DC

#. ALTER every cluster KEYSPACE, so that the keyspaces will no longer replicate data to the decommissioned data-center.

   For example:

   .. code-block:: shell

      cqlsh> DESCRIBE <KEYSPACE_NAME>
      cqlsh> CREATE KEYSPACE <KEYSPACE_NAME> WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', '<DC_NAME1>' : 3, '<DC_NAME2>' : 3, '<DC_NAME3>' : 3};

      cqlsh> ALTER KEYSPACE <KEYSPACE_NAME> WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', '<DC_NAME1>' : 3, '<DC_NAME2>' : 3};

   For example:

   There are three data-centers: US-DC, ASIA-DC and EUROPE-DC, the ASIA-DC is to be removed.
   The current KEYSPACE is:

   .. code-block:: shell

      cqlsh> DESCRIBE nba
      cqlsh> CREATE KEYSPACE nba WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'US-DC' : 3, 'ASIA-DC' : 2, 'EUROPE-DC' : 3};

   Use the ALTER KEYSPACE to change the KEYSPACE and remove the decommissioned data-center.

   .. code-block:: shell

      cqlsh> ALTER KEYSPACE nba WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'US-DC' : 3, 'EUROPE-DC' : 3};

#. Run :doc:`nodetool decommission </operating-scylla/nodetool-commands/decommission>` on every node in the data center that is to be removed.
   Refer to :doc:`Remove a Node from a ScyllaDB Cluster - Down Scale </operating-scylla/procedures/cluster-management/remove-node>` for further information.

   For example:

   If ASIA-DC is to be removed, then, execute the ``nodetool decommission`` command on all the nodes in this data-center.


#. Verify that the data-center was successfully removed by using the :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command.

   For example:

   .. code-block:: shell

      Datacenter: US-DC
      Status=Up/Down
      State=Normal/Leaving/Joining/Moving
      --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
      UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
      UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
      UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

      Datacenter: EUROPE-DC
      Status=Up/Down
      State=Normal/Leaving/Joining/Moving
      --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
      UN  172.91.202.31  112.82 KB  256     32.7%             1d5ed8f4-7764-4dbd-rad8-44fddce94b7v   B1
      UN  172.91.202.32  91.11 KB   256     32.9%             525ed7g4-7437-1dbn-mac8-53fddce9123c   B1
      UN  172.91.202.33  124.42 KB  256     32.6%             975edbm4-6564-63bd-san8-73fddce952ga   B1
