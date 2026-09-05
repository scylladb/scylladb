====================================================================
Scylla Enterprise Metric Update - Scylla Enterprise 2019.1 to 2020.1
====================================================================


The following metrics are new in 2020.1 compared to 2019.1

CQL metrics
~~~~~~~~~~~

* *scylla_cql_deletes_per_ks* : Counts the number of CQL DELETE requests executed on particular keyspaces. The label 'who' indicates where the reqs come from (clients or DB internals)
* *scylla_cql_inserts_per_ks* : Counts the number of CQL INSERT requests executed on particular keyspaces. The label 'who' indicates where the reqs come from (clients or DB internals).
* *scylla_cql_reads_per_ks* : Counts the number of CQL SELECT requests executed on particular keyspaces. The label 'who' indicates where the reqs come from (clients or DB internals)
* *scylla_cql_select_allow_filtering* : Counts the number of SELECT query executions with ALLOW FILTERING option.
* *scylla_cql_select_bypass_caches* : Counts the number of SELECT query executions with BYPASS CACHE option.
* *scylla_cql_select_partition_range_scan* : Counts the number of SELECT query executions requiring partition range scan.
* *scylla_cql_select_partition_range_scan_no_bypass_cache* : Counts the number of SELECT query executions requiring partition range scan without BYPASS CACHE option.
* *scylla_cql_unpaged_select_queries_per_ks* : Counts the number of unpaged CQL SELECT requests against particular keyspaces.
* *scylla_cql_updates_per_ks* : Counts the number of CQL UPDATE requests executed on particular keyspaces. The label 'who' indicates where the reqs come from (clients or DB internals)

SSTable metrics
~~~~~~~~~~~~~~~
  
* *scylla_sstables_capped_local_deletion_time* : Was local deletion time capped at maximum allowed value in Statistics
* *scylla_sstables_capped_tombstone_deletion_time* : Was partition tombstone deletion time capped at maximum allowed value
* *scylla_sstables_cell_tombstone_writes* : Number of cell tombstones written
* *scylla_sstables_cell_writes* : Number of cells written
* *scylla_sstables_partition_reads* : Number of partitions read
* *scylla_sstables_partition_seeks* : Number of partitions seeked
* *scylla_sstables_partition_writes* : Number of partitions written
* *scylla_sstables_range_partition_reads* : Number of partition range flat mutation reads
* *scylla_sstables_range_tombstone_writes* : Number of range tombstones written
* *scylla_sstables_row_reads* : Number of rows read
* *scylla_sstables_row_writes* : Number of clustering rows written
* *scylla_sstables_single_partition_reads* : Number of single partition flat mutation reads
* *scylla_sstables_sstable_partition_reads* : Number of whole sstable flat mutation reads
* *scylla_sstables_static_row_writes* : Number of static rows written
* *scylla_sstables_tombstone_writes* : Number of tombstones written

Storage Proxy Metrics
~~~~~~~~~~~~~~~~~~~~~

* *scylla_storage_proxy_coordinator_cas_dropped_prune* : How many times a coordinator did not perfom prune after cas
* *scylla_storage_proxy_coordinator_cas_failed_read_round_optimization* : Cas read rounds issued only if previous value is missing on some replica
* *scylla_storage_proxy_coordinator_cas_prune* : How many times paxos prune was done after successful cas operation
* *scylla_storage_proxy_coordinator_cas_read_contention* : How many contended reads were encountered
* *scylla_storage_proxy_coordinator_cas_read_latency* : Transactional read latency histogram
* *scylla_storage_proxy_coordinator_cas_read_timeouts* : Number of transactional read request failed due to a timeout
* *scylla_storage_proxy_coordinator_cas_read_unavailable* : Number of transactional read requests failed due to an "unavailable" error
* *scylla_storage_proxy_coordinator_cas_read_unfinished_commit* : Number of transaction commit attempts that occurred on read
* *scylla_storage_proxy_coordinator_cas_write_condition_not_met* : Number of transaction preconditions that did not match current values
* *scylla_storage_proxy_coordinator_cas_write_contention* : How many contended writes were encountered
* *scylla_storage_proxy_coordinator_cas_write_latency* : Transactional write latency histogram
* *scylla_storage_proxy_coordinator_cas_write_timeout_due_to_uncertainty* : How many times write timeout was reported because of uncertainty in the result
* *scylla_storage_proxy_coordinator_cas_write_timeouts* : Number of transactional write request failed due to a timeout
* *scylla_storage_proxy_coordinator_cas_write_unavailable* : Number of transactional write requests failed due to an "unavailable" error
* *scylla_storage_proxy_coordinator_cas_write_unfinished_commit* : Number of transaction commit attempts that occurred on write
* *scylla_storage_proxy_coordinator_foreground_read_repairs* : Number of foreground read repairs
* *scylla_storage_proxy_coordinator_reads_coordinator_outside_replica_set* : Number of CQL read requests which arrived to a non-replica and had to be forwarded to a replica
* *scylla_storage_proxy_coordinator_writes_coordinator_outside_replica_set* : Number of CQL write requests which arrived to a non-replica and had to be forwarded to a replica
* *scylla_storage_proxy_replica_cas_dropped_prune* : How many times a coordinator did not perfom prune after cas
* *scylla_tracing_keyspace_helper_bad_column_family_errors*
* *Scylla_tracing_keyspace_helper_tracing_errors*  

Other metrics
~~~~~~~~~~~~~

* *scylla_stall_detector_reported* : Total number of reported stalls. Look in the traces for the exact reason
* *scylla_database_paused_reads* : The number of currently active reads that are temporarily paused.
* *scylla_database_paused_reads_permit_based_evictions* : The number of paused reads evicted to free up permits. Permits are required for new reads to start, and the database will evict paused reads (if any) to be able to admit new ones if there is a shortage of permits.
* *scylla_database_schema_changed* : The number of times the schema changed
* *scylla_memtables_failed_flushes* : Holds the number of failed memtable flushes. A high value in this metric may indicate a permanent failure to flush a memtable.
* *scylla_query_processor_queries* : Counts queries by consistency level.
* *scylla_reactor_abandoned_failed_futures* : Total number of abandoned failed futures, futures destroyed while still containing an exception
* *scylla_reactor_aio_errors* : Total aio errors
  

CDC Metrics (disabled in 2020.1.0)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* *scylla_cdc_operations_failed* : Number of failed CDC operations
* *scylla_cdc_operations_on_clustering_row_performed_failed* : Number of failed CDC operations that processed a clustering_row
* *scylla_cdc_operations_on_clustering_row_performed_total* : Number of total CDC operations that processed a clustering_row
* *scylla_cdc_operations_on_list_performed_failed* : Number of failed CDC operations that processed a list
* *scylla_cdc_operations_on_list_performed_total* : Number of total CDC operations that processed a list
* *scylla_cdc_operations_on_map_performed_failed* : Number of failed CDC operations that processed a map
* *scylla_cdc_operations_on_map_performed_total* : Number of total CDC operations that processed a map
* *scylla_cdc_operations_on_partition_delete_performed_failed* : Number of failed CDC operations that processed a partition_delete
* *scylla_cdc_operations_on_partition_delete_performed_total* : Number of total CDC operations that processed a partition_delete
* *scylla_cdc_operations_on_range_tombstone_performed_failed* : Number of failed CDC operations that processed a range_tombstone
* *scylla_cdc_operations_on_range_tombstone_performed_total* : Number of total CDC operations that processed a range_tombstone
* *scylla_cdc_operations_on_row_delete_performed_failed* : Number of failed CDC operations that processed a row_delete
* *scylla_cdc_operations_on_row_delete_performed_total* : Number of total CDC operations that processed a row_delete
* *scylla_cdc_operations_on_set_performed_failed* : Number of failed CDC operations that processed a set
* *scylla_cdc_operations_on_set_performed_total* : Number of total CDC operations that processed a set
* *scylla_cdc_operations_on_static_row_performed_failed* : Number of failed CDC operations that processed a static_row
* *scylla_cdc_operations_on_static_row_performed_total* : Number of total CDC operations that processed a static_row
* *scylla_cdc_operations_on_udt_performed_failed* : Number of failed CDC operations that processed a udt
* *scylla_cdc_operations_on_udt_performed_total* : Number of total CDC operations that processed a udt
* *scylla_cdc_operations_total* : Number of total CDC operations
* *scylla_cdc_operations_with_postimage_failed* : Number of failed operations that included postimage
* *scylla_cdc_operations_with_postimage_total* : Number of total operations that included postimage
* *scylla_cdc_operations_with_preimage_failed* : Number of failed operations that included preimage
* *scylla_cdc_operations_with_preimage_total* : Number of total operations that included preimage
* *scylla_cdc_preimage_selects_failed* : Number of failed preimage queries performed
* *scylla_cdc_preimage_selects_total* : Number of total preimage queries performed
* *scylla_compaction_manager_pending_compactions* : Holds the number of compaction tasks waiting for an opportunity to run.

  
