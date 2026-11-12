======================
Nodetool statusbackup
======================
**statusbackup** - Displays the incremental backup status.

.. warning::

   Incremental backup is deprecated, and support for it may be removed in a future release.
   Use :doc:`snapshots </operating-scylla/procedures/backup-restore/backup>` and
   `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/backup/>`_ to back up your data instead.

For example:

``nodetool statusbackup``

Results can be one of the following:``running`` Or ``not running``

By default, the incremental backup status is ``not running``

See also

* :doc:`disablebackup <disablebackup>`
* :doc:`enablebackup <enablebackup>`

.. include:: nodetool-index.rst
