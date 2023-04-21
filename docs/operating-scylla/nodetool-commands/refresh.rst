Nodetool refresh
================

**refresh** - Load newly placed SSTables to the system without a restart.

Add the files to the upload directory, by default it is located under ``/var/lib/scylla/data/keyspace_name/table_name-UUID/upload``

:doc:`Materialized Views (MV)</cql/mv/>` and :doc:`Secondary Indexes (SI)</cql/secondary-indexes/>` of the upload table, and if they exist, they are automatically updated. Uploading MV or SI SSTables is not required and will fail.


.. note:: Scylla node will ignore the partitions in the sstables which are not assigned to this node. For example, if sstable are copied from a different node.


Execute the ``nodetool refresh`` command 

``nodetool refresh <my_keyspace> <my_table>``

For example:

``/var/lib/scylla/data/nba/player_stats-91cd2060f99d11e6a47/upload``

``nodetool refresh nba player_stats``


.. include:: nodetool-index.rst
