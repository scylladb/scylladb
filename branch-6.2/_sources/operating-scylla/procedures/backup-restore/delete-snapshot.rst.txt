
Delete Backups
**************

Please note that when taking a new snapshot, the previous snapshots are not deleted. 
Old snapshots must be deleted in order to clear disk space.


Procedure
---------

Use the :doc:`nodetool clearsnapshot </operating-scylla/nodetool-commands/clearsnapshot/>` command to delete all the existing snapshots files from their directory, ``nodetool clearsnapshot`` can be applied on a single KEYSPACE or multiple KEYSPACES.

``$ nodetool clearsnapshot  <KEYSPACE_NAME>``

For example:

``$ nodetool clearsnapshot mykeyspace`` 

The output is:

``Requested clearing snapshot(s) for [mykeyspace]``

Note: if an automatic procedure is used to take snapshots, it is recommended that an automatic procedure is used to delete the previous snapshots.
Make sure that the data is backed up before deletion.


