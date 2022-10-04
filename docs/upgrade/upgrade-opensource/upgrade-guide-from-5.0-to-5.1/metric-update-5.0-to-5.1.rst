Scylla Metric Update - Scylla 5.0 to 5.1
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 5.1 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 5.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_cache_rows_compacted_with_tombstones
     - Number of rows dropped in the cache by a tombstone write
   * - scylla_cache_rows_dropped_by_tombstones
     - Total number of rows in memtables that were dropped during a cache update on memtable flush
   * - scylla_commitlog_active_allocations
     - Current number of active allocations
   * - scylla_commitlog_blocked_on_new_segment
     - Number of allocations blocked on acquiring a new segment
   * - scylla_commitlog_bytes_flush_requested
     - Number of bytes requested to be flushed (persisted)
   * - scylla_commitlog_bytes_released
     - Number of bytes released from disk (deleted/recycled
   * - scylla_compaction_manager_completed_compactions
     - Number of completed compaction tasks
   * - scylla_compaction_manager_failed_compactions
     - Number of failed compaction tasks
   * - scylla_compaction_manager_normalized_backlog
     - Sum of normalized compaction backlog for all tables in the system. Backlog is normalized by dividing backlog by the shard's available memory.
   * - scylla_compaction_manager_postponed_compactions
     - Number of tables with postponed compaction
   * - scylla_compaction_manager_validation_errors
     - Number of encountered validation errors
   * - scylla_cql_select_parallelized
     - Number of parallelized aggregation SELECT query executions
   * - scylla_database_total_reads_rate_limited
     - Number of read operations that were rejected on the replica side because the per-partition limit was reached
   * - scylla_database_total_writes_rate_limited
     - Number of write operations that were rejected on the replica side because the per-partition limit was reached
   * - scylla_forward_service_requests_dispatched_to_other_nodes
     - Number of forward requests that were dispatched to other nodes
   * - scylla_forward_service_requests_dispatched_to_own_shards
     - Number of forward requests that were dispatched to local shards
   * - scylla_forward_service_requests_executed
     - Number of forward requests that were executed
   * - scylla_gossip_live
     - Number of live nodes the current node sees
   * - scylla_gossip_unreachable
     - Number of unreachable nodes the current node sees
   * - scylla_io_queue_adjusted_consumption
     - Consumed disk capacity units adjusted for class shares and idling preemption
   * - scylla_io_queue_consumption
     - Accumulated disk capacity units consumed by this class; an increment per-second rate indicates full utilization
   * - scylla_io_queue_total_split_bytes
     - Total number of bytes split
   * - scylla_io_queue_total_split_ops
     - Total number of requests split
   * - scylla_per_partition_rate_limiter_allocations
     - Number of times an entry was allocated over an empty/expired entry
   * - scylla_per_partition_rate_limiter_failed_allocations
     - Number of times the rate limiter gave up trying to allocate
   * - scylla_per_partition_rate_limiter_load_factor
     - Current load factor of the hash table (upper bound, may be overestimated)
   * - scylla_per_partition_rate_limiter_probe_count
     - Number of probes made during lookups
   * - scylla_per_partition_rate_limiter_successful_lookups
     - Number of times a lookup returned an already allocated entry
   * - scylla_reactor_aio_outsizes
     - Total number of AIO operations that exceed IO limit
   * - scylla_schema_commitlog_active_allocations
     - Current number of active allocations
   * - scylla_schema_commitlog_alloc
     - Number of not closed segments that still have some free space. This value should not get too high.
   * - scylla_schema_commitlog_allocating_segments
     - Number of times a new mutation has been added to a segment. Divide bytes_written by this value to get the average number of bytes per mutation written to the disk.
   * - scylla_schema_commitlog_blocked_on_new_segment
     - Number of allocations blocked on acquiring a new segment
   * - scylla_schema_commitlog_bytes_flush_requested
     - Number of bytes requested to be flushed (persisted)
   * - scylla_schema_commitlog_bytes_released
     - Number of bytes released from disk (deleted/recycled)
   * - scylla_schema_commitlog_bytes_written
     - Number of bytes written to disk. Divide this value by "alloc" to get the average number of bytes per mutation written to the disk.
   * - scylla_schema_commitlog_cycle
     - Number of commitlog write cycles - when the data is written from the internal memory buffer to the disk
   * - scylla_schema_commitlog_disk_active_bytes
     - Size of disk space in bytes used for data so far. A too high value indicates that there is a bottleneck in writing to SStable paths.
   * - scylla_schema_commitlog_disk_slack_end_bytes
     - Size of disk space (in bytes) unused because of segment switching (end slack). A too high value indicates that not enough data is written to each segment.
   * - scylla_schema_commitlog_disk_total_bytes
     - Size of disk space (in bytes) reserved for data so far. A too high value indicates that there is a bottleneck in writing to SStable paths.
   * - scylla_schema_commitlog_flush
     - Number of times the flush() method was called for a file
   * - scylla_schema_commitlog_flush_limit_exceeded
     - Number of times a flush limit was exceeded. A non-zero value indicates that there are too many pending flush operations (see pending_flushes), and some of them will be blocked till the total amount of pending flush operations drops below 5.
   * - scylla_schema_commitlog_memory_buffer_bytes
     - Total number of bytes in internal memory buffers
   * - scylla_schema_commitlog_pending_allocations
     - Number of currently pending allocations. A non-zero value indicates that there is a bottleneck in the disk write flow.
   * - scylla_schema_commitlog_pending_flushes
     - Number of currently pending flushes. See the related ``flush_limit_exceeded`` metric.
   * - scylla_schema_commitlog_requests_blocked_memory
     - Number of requests blocked due to memory pressure. A non-zero value indicates that the commitlog memory quota is not enough to serve the required amount of requests.
   * - scylla_schema_commitlog_segments
     - Current number of segments
   * - scylla_schema_commitlog_slack
     - Number of unused bytes written to the disk due to disk segment alignment
   * - scylla_schema_commitlog_unused_segments
     - Current number of unused segments. A non-zero value indicates that the disk write path became temporarily slow.
   * - scylla_sstables_pi_auto_scale_events
     - Number of promoted index auto-scaling events
   * - scylla_storage_proxy_coordinator_read_rate_limited
     - Number of read requests that were rejected by replicas because the rate limit for the partition was reached
   * - scylla_storage_proxy_coordinator_write_rate_limited
     - Number of write requests that were rejected by replicas because the rate limit for the partition was reached