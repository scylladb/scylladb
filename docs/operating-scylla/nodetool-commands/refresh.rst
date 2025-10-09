Nodetool refresh
================

**refresh** - Load newly placed SSTables to the system without a restart.

Add the files to the upload directory, by default it is located under ``/var/lib/scylla/data/keyspace_name/table_name-UUID/upload``

:doc:`Materialized Views (MV)</cql/mv/>` and :doc:`Secondary Indexes (SI)</cql/secondary-indexes/>` of the upload table, and if they exist, they are automatically updated. Uploading MV or SI SSTables is not required and will fail.


.. note:: ScyllaDB node will ignore the partitions in the sstables which are not assigned to this node. For example, if sstable are copied from a different node.


Execute the ``nodetool refresh`` command 

``nodetool refresh <my_keyspace> <my_table>``

For example:

``/var/lib/scylla/data/nba/player_stats-91cd2060f99d11e6a47/upload``

``nodetool refresh nba player_stats``

.. _nodetool-refresh-load-and-stream:


Load and Stream
---------------

.. code::

<<<<<<< HEAD
   nodetool refresh <my_keyspace> <my_table> [--load-and-stream | -las]
||||||| parent of aaf53e9c42 (nodetool refresh primary-replica-only)
   nodetool refresh <my_keyspace> <my_table> [--load-and-stream | -las] [--scope <scope>]
=======
   nodetool refresh <my_keyspace> <my_table> [(--load-and-stream | -las) [[(--primary-replica-only | -pro)] | [--scope <scope>]]]

The Load and Stream feature extends nodetool refresh. 

The ``--load-and-stream`` option loads arbitrary sstables into the cluster by reading the sstable data and streaming each partition to the replica(s) that owns it. In addition, the ``--scope`` and ``--primary-replica-only`` options are applied to filter the set of target replicas for each partition.  For example, say the old cluster has 6 nodes and the new cluster has 3 nodes. One can copy the sstables from the old cluster to any of the new nodes and trigger refresh with load and stream.



>>>>>>> aaf53e9c42 (nodetool refresh primary-replica-only)


Load and Stream make restores and migrations much easier:

* You can place sstable from every node to every node
* No need to run nodetool cleanup to remove unused data

<<<<<<< HEAD
||||||| parent of aaf53e9c42 (nodetool refresh primary-replica-only)
Scope
-----

The `scope` parameter describes the subset of cluster nodes where you want to load data:

* `node` - On the local node.
* `rack` - On the local rack.
* `dc` - In the datacenter (DC) where the local node lives.
* `all` (default) - Everywhere across the cluster.

Scope supports a variety of options for filtering out the destination nodes.
On one extreme, one node is given all SStables with the scope ``all``; on the other extreme, all
nodes are loading only their own SStables with the scope ``node``. In between, you can choose
a subset of nodes to load only SStables that belong to the rack or DC.

This option is only valid when using the ``--load-and-stream`` option.


Skip cleanup
---------------

.. code::

   nodetool refresh <my_keyspace> <my_table> [--skip-cleanup]

When loading an SSTable, Scylla will cleanup it from keys that the node is not responsible for. To skip this step, use the `--skip-cleanup` option.
See :ref:`nodetool cleanup <nodetool-cleanup-cmd>`.


Skip reshape
---------------

.. code::

   nodetool refresh <my_keyspace> <my_table> [--skip-reshape]

When refreshing, the SSTables to load might be out of shape, Scylla will attempt to reshape them if that's the case. To skip this step, use the `--skip-reshape` option.
=======
With --primary-replica-only (or -pro) option, only the primary replica of each partition in an sstable will be used as the target. 
--primary-replica-only must be applied together with --load-and-stream.
--primary-replica-only cannot be used with --scope, they are mutually exclusive.
--primary-replica-only requires repair to be run after the load and stream operation is completed. 


Scope
-----

The `scope` parameter describes the subset of cluster nodes where you want to load data:

* `node` - On the local node.
* `rack` - On the local rack.
* `dc` - In the datacenter (DC) where the local node lives.
* `all` (default) - Everywhere across the cluster.

Scope supports a variety of options for filtering out the destination nodes.
On one extreme, one node is given all SStables with the scope ``all``; on the other extreme, all
nodes are loading only their own SStables with the scope ``node``. In between, you can choose
a subset of nodes to load only SStables that belong to the rack or DC.

This option is only valid when using the ``--load-and-stream`` option.


Skip cleanup
---------------

.. code::

   nodetool refresh <my_keyspace> <my_table> [--skip-cleanup]

When loading an SSTable, Scylla will cleanup it from keys that the node is not responsible for. To skip this step, use the `--skip-cleanup` option.
See :ref:`nodetool cleanup <nodetool-cleanup-cmd>`.


Skip reshape
---------------

.. code::

   nodetool refresh <my_keyspace> <my_table> [--skip-reshape]

When refreshing, the SSTables to load might be out of shape, Scylla will attempt to reshape them if that's the case. To skip this step, use the `--skip-reshape` option.
>>>>>>> aaf53e9c42 (nodetool refresh primary-replica-only)

.. include:: nodetool-index.rst
