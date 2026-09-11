Scylla Metric Update - Scylla 4.5 to 4.6
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 4.6 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 4.6
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New cache metrics
^^^^^^^^^^^^^^^^^

  * scylla_cache_range_tombstone_reads
  * scylla_cache_row_tombstone_reads

New sstable metrics
^^^^^^^^^^^^^^^^^^^

  * scylla_sstables_bloom_filter_memory_size
  * scylla_sstables_currently_open_for_reading
  * scylla_sstables_currently_open_for_writing
  * scylla_sstables_index_page_cache_bytes_in_std
  * scylla_sstables_index_page_evictions
  * scylla_sstables_index_page_populations
  * scylla_sstables_index_page_used_bytes
  * scylla_sstables_range_tombstone_reads
  * scylla_sstables_row_tombstone_reads
  * scylla_sstables_total_deleted
  * scylla_sstables_total_open_for_reading
  * scylla_sstables_total_open_for_writing

More new metrics
^^^^^^^^^^^^^^^^

  * scylla_commitlog_disk_slack_end_bytes
  * scylla_io_queue_disk_queue_length
  * scylla_io_queue_total_exec_sec
  * scylla_lsa_memory_evicted
  * scylla_lsa_memory_freed
  * scylla_node_ops_finished_percentage
