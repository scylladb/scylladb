Update Topology Strategy From Simple to Network
***********************************************

The following procedure specifies how to update the replication strategy from ``SimpleStrategy`` to ``NetworkTopologyStrategy``.
Note that ``SimpleStrategy`` is **not** recommended for production usage, and it is strongly advised to create new clusters with ``NetworkTopologyStrategy``.

In case you are using ``SimpleStrategy``, there are two alternatives:

* Nodes are all on the same rack (can be updated without downtime)
* Nodes are on different racks (requires full shutdown)

To check which node is on which rack, use ``nodetool status``

Note that if the Replication Factor (RF) of the relevant Keyspace is equal to the number of nodes, regardless of the number of racks, for example, RF=3, nodes=3, you can use the first procedure without downtime.

All nodes are on the same rack
------------------------------

Alter each Keyspace replication to use ``class : NetworkTopologyStrategy``.

Alter the following:

* Keyspace created by the user.
* System: ``system_distributed``, ``system_traces``.

For example:

Before

.. code-block:: cql

   DESCRIBE KEYSPACE mykeyspace; 
   CREATE KEYSPACE mykespace WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '3'};

ALTER Command

.. code-block:: cql
   
   ALTER KEYSPACE mykespace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<existing_dc>' : 3};

After

.. code-block:: cql

   DESCRIBE KEYSPACE mykeyspace;
   CREATE KEYSPACE mykespace WITH replication = { 'class' : 'NetworkTopologyStrategy', '<existing_dc>' : 3};

To complete the process, you need to :doc:`change the snitch </operating-scylla/procedures/config-change/switch-snitch/>`, edit the 
``cassandra-rackdc.properties`` file, and set the preferred data-center name.


Nodes are on different racks
----------------------------

This is a more complex scenario, as the new strategy may select different replicas depending on whether the nodes are on different racks.
To fix that, you will need a **full shutdown of the cluster**.

#. Stop all traffic to the cluster. **A failure to stop the traffic can cause data loss.**
#. Run :doc:`full repair on the cluster </operating-scylla/procedures/maintenance/repair>`
#. Alter the strategy as detailed above
#. Run a second full repair
#. Run ``nodetool cleanup`` on each node
#. Resume traffic

To complete the process, you need to change the Snitch, edit the ``cassandra-rackdc.properties`` file, and set the preferred data-center name.
