Update Topology Strategy From Simple to Network
***********************************************

The following procedure specifies how to update the replication strategy from ``SimpleStrategy`` to ``NetworkTopologyStrategy``.
Note that ``SimpleStrategy`` is **not** recommended for production usage, and it is strongly advised to create new clusters with ``NetworkTopologyStrategy``.

In case you are using ``SimpleStrategy``, there are two possible procedures:

* **Procedure with no downtime**: This procedure can be used if any of the following conditions are met:

  * All nodes are on the same rack

  * Regardless of the number of racks, the Replication Factor (RF) of the ``SimpleStrategy`` keyspace is not changing, and the RF is also equal to the number of cluster nodes. For example, if the RF of a keyspace is 3 and the cluster has 3 nodes, and if the RF is not changing, then the number of racks does not matter.

* **Procedure with downtime**: This procedure should be used if any of the following conditions are met:

  * Cluster nodes are on multiple racks, and the Replication Factor (RF) of the ``SimpleStrategy`` keyspace is not changing, and the RF is **not** equal to the number of nodes in the cluster.

  * Cluster nodes are on multiple racks, and the Replication Factor (RF) of the ``SimpleStrategy`` keyspace is changing.

To check which node is on which rack, use ``nodetool status`` (or ``sctool status`` from the Scylla Manager node)

Check keyspaces
---------------

Check if the following keyspaces use ``SimpleStrategy``:

* Any user-created keyspace.

  To check the replication strategy of a specific user-created keyspace:

  .. code-block:: cql

     DESCRIBE KEYSPACE mykeyspace;

  Or to check all user-created keyspaces:

  .. code-block:: cql

     DESCRIBE SCHEMA;

* Certain system keyspaces, including ``system_distributed``, ``system_traces``, ``system_audit``.

  To check the replication strategy of the above system keyspaces:

  .. code-block:: cql

     DESCRIBE KEYSPACE system_distributed;
     DESCRIBE KEYSPACE system_traces;
     DESCRIBE KEYSPACE system_audit;

If any of the above keyspaces uses ``SimpleStrategy``, use the relevant procedure on this page to switch it to ``NetworkTopologyStategy``.

Procedure with no downtime
--------------------------

Alter each Keyspace replication to use ``class : NetworkTopologyStrategy``.

For example:

Before

.. code-block:: cql

   DESCRIBE KEYSPACE mykeyspace; 
   CREATE KEYSPACE mykeyspace WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '3'};

ALTER Command

.. code-block:: cql
   
   ALTER KEYSPACE mykeyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<existing_dc>' : 3};

After

.. code-block:: cql

   DESCRIBE KEYSPACE mykeyspace;
   CREATE KEYSPACE mykeyspace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<existing_dc>' : 3};

To complete the process, you need to :doc:`change the snitch </operating-scylla/procedures/config-change/switch-snitch/>`, edit the 
``cassandra-rackdc.properties`` file, and set the preferred data-center name.

Procedure with downtime
-----------------------

This is a more complex scenario, as the new strategy may select different replicas depending on whether the nodes are on different racks.
To fix that, you will need a **full shutdown of the cluster**.

#. Stop all traffic to the cluster. **A failure to stop the traffic can cause data loss.**
#. Run :doc:`full repair on the cluster </operating-scylla/procedures/maintenance/repair>`
#. Alter the strategy as detailed above
#. Run a second full repair
#. Run ``nodetool cleanup`` on each node
#. Resume traffic

To complete the process, you need to change the Snitch, edit the ``cassandra-rackdc.properties`` file, and set the preferred data-center name.
