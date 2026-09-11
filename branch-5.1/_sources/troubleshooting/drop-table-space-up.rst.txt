Dropped (or truncated) Table (or keyspace) and Disk Space is not Reclaimed 
==========================================================================

This troubleshooting guide describes what to do when Scylla keeps using disk space after a table or keyspaces are dropped or truncated.

Problem
^^^^^^^

When performing a ``DROP`` or ``TRUNCATE`` operation on a table or keyspace, disk usage is not seen to be reduced.
Usually this is verified by using an external utility like the ``du`` Linux command.
This is caused by the fact that by default, Scylla creates a snapshot of every dropped table. Space won't be reclaimed until the snapshot is dropped.

Solution
^^^^^^^^
1. Locate ``/var/lib/scylla/data/<your_keyspace>/<your_table>``. Inside that directory you will see a ``snapshots`` subdirectory with your dropped data. Follow the procedure to use nodetool to :doc:`remove the snapshot </operating-scylla/procedures/backup-restore/delete-snapshot>`.
2. If you are deleting an entire keyspace, repeat the procedure above for every table inside the keyspace.
3. This behavior is controlled by the ``auto_snapshot`` flag in ``/etc/scylla/scylla.yaml``, which set to true by default. To stop taking snapshots on deletion, set that flag to false and restart all your scylla nodes.

.. note:: Alternatively you can use the``rm`` Linux utility to remove the files. If you do, keep in mind that the ``rm`` Linux utility is not aware if some snapshots are still associated with existing keyspaces, but nodetool is. 
 

