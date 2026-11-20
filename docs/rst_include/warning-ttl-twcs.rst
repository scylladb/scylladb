
.. caution::
   * We strongly recommend using a single TTL value for any given table.
   * This means sticking to the default time to live as specified in the table's schema.
   * Using multiple TTL values for a given table may lead to inefficiency when purging expired data, because an SSTable will remain until **all** of its data is expired.
   * Tombstone compaction can be enabled to remove data from partially expired SSTables, but this creates additional WA (write amplification).

.. caution::
   * Avoid overwriting data and deleting data explicitly at all costs, as this can potentially block an expired SSTable from being purged, due to the checks that are performed to avoid data resurrection.
   * If a table is guaranteed to have no overwrites or deletions, user can use the :ref:`immediate tombstone GC mode <ddl-tombstones-gc>` for optimal GC efficiency.
   * If a table has either overwrites or deletions, user must use the :ref:`repair tombstone GC mode <ddl-tombstones-gc>`. It reduces GC efficiency since expired data will be removed only after repair operation completes.