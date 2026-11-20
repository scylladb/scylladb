Nodetool listsnapshots
======================

**listsnapshots** - Lists all the snapshots along with the size on disk and true size.
Snapshots of dropped tables (for example, the automatic snapshot taken on ``DROP TABLE``) are also listed.


For example:

.. code-block:: shell

   nodetool listsnapshots

   Snapshot Details: 
   Snapshot name Keyspace name Column family name True size Size on disk
   5487138454987 nba           player_name        0 bytes   308.66 MiB
   2157384283120 nba           player_team        0 bytes   107.21 MiB
   4824891793663 nba           player_stats       0 bytes   41.69 MiB

   Total TrueDiskSpaceUsed: 0 bytes
               

================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
Snapshot name                                     Name of the snapshot
------------------------------------------------  ---------------------------------------------------------------------------------
Keyspace name                                     Keyspace name
------------------------------------------------  ---------------------------------------------------------------------------------
Column family name                                Column Family (table) name
------------------------------------------------  ---------------------------------------------------------------------------------
True size                                         Total size of the snapshot files that are not shared with the table's current SSTables, i.e. disk space used only by the snapshot
------------------------------------------------  ---------------------------------------------------------------------------------
Size on disk                                      Total size of the snapshot on disk
================================================  =================================================================================

For example: 

There is a single 1TB file in the snapshot directory. If that file also exists in the main column family directory, the size on the disk is 1TB and the true size is 0 because it is already backed up to disk.
 
.. include:: nodetool-index.rst
