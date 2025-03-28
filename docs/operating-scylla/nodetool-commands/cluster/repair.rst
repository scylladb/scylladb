Nodetool cluster repair
=======================

**cluster repair** - A process that runs in the background and synchronizes the data between nodes. It only repairs keyspaces with tablets enabled (default).
To repair keyspaces with tablets disabled (vnodes-based), see :doc:`nodetool repair </operating-scylla/nodetool-commands/repair/>`.

Running ``cluster repair`` on a **single node** synchronizes all data on all nodes in the cluster.

To synchronize all data in clusters that have both tablets-based and vnodes-based keyspaces, run :doc:`nodetool repair -pr </operating-scylla/nodetool-commands/repair/>` on **all**
of the nodes in the cluster, and :doc:`nodetool cluster repair </operating-scylla/nodetool-commands/cluster/repair/>` on  **any** of the nodes in the cluster.

To check if a keyspace enables tablets, use:

.. code-block:: cql

  DESCRIBE KEYSPACE `keyspace_name`

ScyllaDB node will ensure the exclusivity of tablet repair and maintenance operations (add/remove/decommission/replace/rebuild).

ScyllaDB nodetool cluster repair command supports the following options:


- ``-dc`` ``--in-dc`` syncs data between all nodes in a list of Data Centers (DCs).


  .. warning:: This command leaves part of the data subset (all remaining DCs) out of sync.

     For example:

     ::

        nodetool cluster repair -dc US_DC
        nodetool cluster repair --in-dc US_DC, EU_DC

- ``-hosts`` ``--in-hosts`` syncs the data only between a list of nodes, using host ID.

  .. warning:: this command leaves part of the data subset (on nodes that are *not* listed) out of sync.

  For example:

  ::

     nodetool cluster repair -hosts cdc295d7-c076-4b07-af69-1385fefdb40b,2dbdf288-9e73-11ea-bb37-0242ac130002
     nodetool cluster repair --in-hosts cdc295d7-c076-4b07-af69-1385fefdb40b,2dbdf288-9e73-11ea-bb37-0242ac130002,3a5993f8-9e73-11ea-bb37-0242ac130002

- ``--tablet-tokens`` selects which tablets to repair. When the listed token belongs to a tablet, the whole tablet that owns the token will be repaired. By default, all tablets are repaired.

  .. warning:: this command leaves part of the data subset (on nodes that are *not* listed) out of sync.

  For example:

  ::

     nodetool cluster repair --tablet-tokens 1,10474535988

- ``keyspace`` executes a repair on a specific keyspace. The default is all keyspaces.

  For example:

  ::

     nodetool cluster repair <my_keyspace>


- ``table`` executes a repair on a specific table or a list of space-delimited table names. The default is all tables.

  For example:

  ::

     nodetool cluster repair <my_keyspace> <my_table>

See also `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_.
