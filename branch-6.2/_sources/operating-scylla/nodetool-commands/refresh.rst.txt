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

   nodetool refresh <my_keyspace> <my_table> [--load-and-stream | -las]

The Load and Stream feature extends nodetool refresh. The new ``-las`` option loads arbitrary sstables that do not belong to a node into the cluster. It loads the sstables from the disk and calculates the data's owning nodes, and streams automatically.
For example, say the old cluster has 6 nodes and the new cluster has 3 nodes. We can copy the sstables from the old cluster to any of the new nodes and trigger the load and stream process.

Load and Stream make restores and migrations much easier:

* You can place sstable from every node to every node
* No need to run nodetool cleanup to remove unused data


.. include:: nodetool-index.rst
