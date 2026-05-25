:term:`Sorted Strings Table (SSTable)<SSTable>` is the persistent file format used by ScyllaDB and Apache Cassandra. SSTable is saved as a persistent, ordered, immutable set of files on disk.
Immutable means SSTables are never modified; they are created by a MemTable flush and are deleted by a compaction.
The location of ScyllaDB SSTables is specified in scylla.yaml ``data_file_directories`` parameter (default location: ``/var/lib/scylla/data``).

SSTable 3.x is more efficient and requires less disk space than the SSTable 2.x.

For more information on each of the SSTable formats, see below:

* :doc:`SSTable 2.x </architecture/sstable/sstable2/index>`
* :doc:`SSTable 3.x </architecture/sstable/sstable3/index>`

SSTable Version Support
------------------------

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - SSTable Version
     - ScyllaDB Version
   * - 3.x ('mt')
     - 2026.2 and above
   * - 3.x ('ms')
     - 2025.4 and above
   * - 3.x (``me``)
     - 2022.2 and above
   * - 3.x (``md``)
     - 2021.1

* The supported formats are ``me`` and ``mt``.
* The ``md`` format is used only when upgrading from an existing cluster using
  ``md``. The ``sstable_format`` parameter is ignored if it is set to ``md``.
* The ``ms`` format has been superseded by ``mt``, and is used only when upgrading
  from an existing cluster using ``ms``. If the ``sstable_format`` parameter is
  set to ``ms``, ``mt`` files will be written.
* Note: The ``sstable_format`` parameter specifies the SSTable format used for
  **writes**. The legacy SSTable formats (``ka``, ``la``, ``mc``) remain
  supported for reads, which is essential for restoring clusters from existing
  backups.

The ms Format: Trie-Based SSTable Index
-----------------------------------------

The ``ms`` format introduces a *trie-based* SSTable index.

For a detailed description of the trie index format, see :doc:`SSTable ms Index </architecture/sstable/sstable3/sstable-ms-index>`.
