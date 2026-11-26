Nodetool cluster listsnapshots
==============================

**cluster listsnapshots** - Lists the cluster snapshots and the tables each of them contains.

The command lists the snapshots taken with :doc:`cluster snapshot </operating-scylla/nodetool-commands/snapshot>`. They are read from the snapshot catalog, so every node returns the same list. A snapshot that failed before completion is not listed. Snapshots taken on one node only are listed by :doc:`listsnapshots </operating-scylla/nodetool-commands/listsnapshots>`.

  For example:

  ::

     nodetool cluster listsnapshots

Example output:

.. code-block:: none

   Snapshot name Created at           Keyspace name Table name
   snap1         2026-11-26T10:00:00Z ks1           tbl1
   snap1         2026-11-26T10:00:00Z ks1           tbl2

A snapshot is cleared with :doc:`cluster clearsnapshot </operating-scylla/nodetool-commands/cluster/clearsnapshot>`.
