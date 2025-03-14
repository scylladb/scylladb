Nodetool tablet-repair
======================

**Tablet-repair** - a process that runs in the background and synchronizes the data between nodes. The command repairs only the tablet keyspaces.

When running ``nodetool tablet-repair`` on a **single node**, all the specified data will be repaired even if it is not contained in the requested node.

To repair **all** of the data in the cluster, it is enough to run tablet-repair on one node in a cluster.

.. note:: Run the :doc:`nodetool tablet-repair </operating-scylla/nodetool-commands/tablet-repair/>` command regularly. If you delete data frequently, it should be more often than the value of ``gc_grace_seconds`` (by default: 10 days), for example, every week.

Scylla node will ensure the exclusivity of tablet repair and maintenance operations (add/remove/decommission/replace/rebuild).

ScyllaDB nodetool repair command supports the following options:


- ``-dc`` ``--in-dc`` syncs data between all nodes in a list of Data Centers (DCs).


  .. warning:: This command leaves part of the data subset (all remaining DCs) out of sync.

     For example:

     ::

        nodetool tablet-repair -dc US_DC
        nodetool tablet-repair --in-dc US_DC, EU_DC

- ``-hosts`` ``--in-hosts`` syncs the data only between a list of nodes, using host ID.

  .. warning:: this command leaves part of the data subset (on nodes that are *not* listed) out of sync.

  For example:

  ::

     nodetool tablet-repair -hosts cdc295d7-c076-4b07-af69-1385fefdb40b,2dbdf288-9e73-11ea-bb37-0242ac130002
     nodetool tablet-repair --in-hosts cdc295d7-c076-4b07-af69-1385fefdb40b,2dbdf288-9e73-11ea-bb37-0242ac130002,3a5993f8-9e73-11ea-bb37-0242ac130002

- ``-local`` ``--in-local-dc`` executes a repair on the nodes in the local datacenter only.

  For example:

  ::

     nodetool tablet-repair -local
     nodetool tablet-repair --in-local-dc

- ``--tablet-tokens`` executes a repair on tablets that own any token in the list. The default is all tokens.

  For example:

  ::

     nodetool tablet-repair --tablet-tokens 1,10474535988

- ``keyspace`` executes a repair on a specific keyspace. The default is all keyspaces.

  For example:

  ::

     nodetool tablet-repair <my_keyspace>


- ``table`` executes a repair on a specific table or a list of space-delimited table names. The default is all tables.

  For example:

  ::

     nodetool repair <my_keyspace> <my_table>

To repair vnode keyspaces see :doc:`nodetool repair </operating-scylla/nodetool-commands/repair/>`.

.. note:: To repair all, both vnode and tablet, keyspaces you need to run :doc:`nodetool repair -pr </operating-scylla/nodetool-commands/repair/>` on **all** of the nodes in the cluster and :doc:`nodetool tablet-repair </operating-scylla/nodetool-commands/tablet-repair/>` on **any** of the nodes in the cluster.

See also `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_.

.. include:: nodetool-index.rst
