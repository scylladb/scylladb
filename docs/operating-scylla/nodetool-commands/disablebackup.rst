======================
Nodetool disablebackup
======================
**disablebackup** - Disables incremental backup.

.. warning::

   Incremental backup is deprecated, and support for it may be removed in a future release.
   Use :doc:`snapshots </operating-scylla/procedures/backup-restore/backup>` and
   `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/backup/>`_ to back up your data instead.

For example:

``nodetool disablebackup``

See also

* :doc:`statusbackup <statusbackup>`
* :doc:`enablebackup <enablebackup>`

.. include:: nodetool-index.rst

