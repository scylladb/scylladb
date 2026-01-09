:term:`Sorted Strings Table (SSTable)<SSTable>` is the persistent file format used by ScyllaDB and Apache Cassandra. SSTable is saved as a persistent, ordered, immutable set of files on disk.
Immutable means SSTables are never modified; they are created by a MemTable flush and are deleted by a compaction.
The location of ScyllaDB SSTables is specified in scylla.yaml ``data_file_directories`` parameter (default location: ``/var/lib/scylla/data``).

SSTable 3.x is more efficient and requires less disk space than the SSTable 2.x.

SSTable Version Support
------------------------

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - SSTable Version
     - ScyllaDB Version
   * - 3.x ('ms')
     - 2025.4 and above
   * - 3.x ('me')
     - 2022.2 and above
   * - 3.x ('md')
     - 2021.1

     
