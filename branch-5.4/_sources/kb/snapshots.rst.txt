================
Scylla Snapshots
================

.. your title should be something customers will search for.

**Topic: snapshots**

.. Give a subtopic for the title (User Management, Security, Drivers, Automation, Optimization, Schema management, Data Modeling, etc.)

**Learn: What are Scylla snapshots? What are they used for? How do they get created and removed?**


**Audience: Scylla administrators**

.. Choose (Application Developer, Scylla Administrator, Internal, All)

Synopsis
--------

Snapshots in Scylla are an essential part of the :doc:`backup and restore mechanism </operating-scylla/procedures/backup-restore/index>`. Whereas in other databases a backup starts with creating a copy of a data file (cold backup, hot backup, shadow copy backup), in Scylla the process starts with creating a table or keyspace snapshot. The snapshots are created either automatically (this is described further in this article) or by invoking the :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>` command. 
To prevent any issues with restoring your data, the backup strategy must include saving copies of the snapshots on a secondary storage. This makes sure the snapshot is available to restore if the primary storage fails.

.. note:: If you come from RDBMS background you should not confuse snapshots with the notion of materialized views (as they are sometimes called snapshots in that area of technology). With Scylla, snapshots are `hard links <https://en.wikipedia.org/wiki/Hard_link>`_ to data files. :doc:`Materialized views </using-scylla/materialized-views>` do exist in Scylla but they are not called snapshots.


How Snapshots Work
------------------

Scylla, like Cassandra, requires `Unix-like storage <https://en.wikipedia.org/wiki/Unix_filesystem?>`_ (such is also a file system supported by Linux). As mentioned above, snapshots are hard links to SSTables on disk. It is important to understand that SSTables are immutable and as such are not re-written in the same file. When data in database changes and data is written to disk, it is written as a new file. The new files are consolidated following compaction, which merges table’s data into one or more SSTable files (depending on the compaction strategy).

If snapshots (hard links) were created to existing SSTables on disk they are preserved even if table data is eventually stored in one or more of the new SSTables. The :doc:`compaction process </cql/compaction>` removes files in the data directory, but the snapshot hard links **will still** be pointing to the **old files**. Only after all of the pointers are removed, the actual file is removed. If even one pointer exists, the file will remain. Therefore, even as the database is moving on, once the snapshot hard links are created, the content of the data files can be copied off to another storage and serve as the foundation for a table, keyspace, or entire database restore (on that node, as this backup and restore process is node specific). 

Apart from *planned backup* procedure described above, and as a safeguard from *accidental* loss of data, the Scylla database includes an optional creation of an automatic snapshot every time a table is dropped or truncated.  As dropping a keyspace involves dropping tables within that keyspace, these actions will invoke auto snapshots as well. This option is enabled out of the box and is controlled by the auto_snapshot flag in the ``/etc/scylla/scylla.yaml`` configuration file. Note that a keyspace cannot be truncated. It can only be dropped. A table, on the other hand, can  be either truncated or dropped. The data in a table can also be deleted, which is different from being truncated.

The default setting for the ``auto_snapshot`` flag in ``/etc/scylla/scylla.yaml`` file is ``true``. It is **not** recommended to set it to ``false``, unless there is a good backup and recovery strategy in place.

Snapshot Creation
-----------------

Snapshots **are created** when:

* ``nodetool snapshot <keyspace_name>`` runs as this creates snapshots for all tables in the keyspace. 
* ``nodetool snapshot <keyspace_name>.<table_name>`` runs as this creates snapshots for a specific table
* The ``auto_snapshot`` flag (in scylla.yaml) is set to ``true`` and the table is dropped (as in ``DROP TABLE <keyspace_name>.<table_name>`` )
* The ``auto_snapshot`` flag (in scylla.yaml) is set to ``true`` and the table is truncated (as in ``TRUNCATE TABLE <keyspace_name>.<table_name>`` )
* The ``auto_snapshot`` flag (in scylla.yaml) is set to ``true`` and the keyspace is dropped (as in ``DROP KEYSPACE <keyspace_name>`` ) - which is the equivalent to dropping all tables in the keyspace and also dropping the keyspace as well.

Snapshots **are not created** when:

* Data in the table is deleted, as opposed to truncated.
* The ``auto_snapshot`` flag (in scylla.yaml) is set to ``false`` and the table is dropped or truncated.


List Current Snapshots
-----------------------

Snapshot information for tables or keyspaces which are present in the database at the time of invoking the command can be shown with ``nodetool listsnapshots``. Any snapshot which refers to a dropped keyspace or dropped table **cannot** be listed, even if the snapshot still exists on disk (because the database information about these snapshots was deleted but snapshots remain on disk).  You can locate these snapshots by looking for the ``snapshot`` directory using a `find <http://man7.org/linux/man-pages/man1/find.1.html>`_ command in Linux.

Remove Snapshots
----------------

Snapshots are not removed automatically. On  active database nodes, old snapshots take up disk space and need to be removed manually to free up the storage space. Snapshots are only taking additional space on disk once the original data files have been changed.

.. caution:: Do not delete snapshots manually at the file system level, use the nodetool command.

Use this procedure to remove snapshots or local backups.

**Procedure**

Use one of the following steps:

To remove a specific snapshot from a specific keyspace:

* Run ``nodetool clearsnapshot -t <snapshot_name> <keyspace_name>``. Note that the snapshot name in this command is a tag, a label, assigned to that snapshot. 

To remove the named snapshot from all keyspaces, that is, if any of the keyspaces happen to contain the named snapshot:

* Run ``nodetool clearsnapshot -t <snapshot_name>`` command. Here the keyspace name was omitted. 

To remove all existing snapshots without any warning:

* Run ``nodetool clearsnapshot`` 

.. caution:: use caution when running ``nodetool clearsnapshot`` without specifying a keyspace or snapshot as this command will remove not only snapshots listed by “nodetool listsnapshots” command but all other snapshots on the node’s storage as well, including those for previously dropped tables or keyspaces. 

When all else fails, and you need to remove the snapshot manually:

* If database can’t be brought up, it will be impossible for the nodetool command to list or delete snapshots. If, in this situation, the storage must be cleared of old snapshots, the only other remaining way would be removing snapshots manually, at the storage level, with the Linux `rm <http://man7.org/linux/man-pages/man1/rm.1.html>`_ command. 

.. note:: After you remove the snapshot with the ``rm`` command and the database which couldn't be brought up returns, ``nodetool listsnapshots`` may still list snapshots that were manually removed.



Additional References
---------------------

* :doc:`Taking backups </operating-scylla/procedures/backup-restore/backup>` with snapshots.
* :doc:`How snapshots are created on demand </operating-scylla/nodetool-commands/snapshot>` (rather than automatically when tables are dropped or truncated).
* :doc:`Restoring from snapshots </operating-scylla/procedures/backup-restore/restore>`

