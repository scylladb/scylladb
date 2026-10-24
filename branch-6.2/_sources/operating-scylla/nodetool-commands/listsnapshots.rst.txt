Nodetool listsnapshots
======================

**listsnapshots** - Lists all the snapshots along with the size on disk and true size.
Dropped tables (column family) will not be part of the ``listsnapshots``. 


For example:

.. code-block:: shell

   nodetool listsnapshots

   Snapshot Details: 
   Snapshot Name  Keyspace   Column Family  True Size   Size on Disk 

   5487138454987  nba        player_name    0 bytes     308.66 MB
   2157384283120  nba        player_team    0 bytes     107.21 MB
   4824891793663  nba        player_stats   0 bytes      41.69 MB                   
               

================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
Snapshot Name                                     Name of the snapshot
------------------------------------------------  ---------------------------------------------------------------------------------
Keyspace                                          Keyspace name
------------------------------------------------  ---------------------------------------------------------------------------------
Column Family                                     Column Family (table) name
------------------------------------------------  ---------------------------------------------------------------------------------
True Size                                         Total size of all SSTables which are not backed up to disk
------------------------------------------------  ---------------------------------------------------------------------------------
Size on Disk                                      Total size of the snapshot on disk
================================================  =================================================================================

For example: 

There is a single 1TB file in the snapshot directory. If that file also exists in the main column family directory, the size on the disk is 1TB and the true size is 0 because it is already backed up to disk.
 
.. include:: nodetool-index.rst
