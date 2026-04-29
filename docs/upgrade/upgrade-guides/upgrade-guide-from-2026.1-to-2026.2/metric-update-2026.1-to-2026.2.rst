.. |SRC_VERSION| replace:: 2026.1
.. |NEW_VERSION| replace:: 2026.2
.. |PRECEDING_VERSION| replace:: 2026.1

================================================================
Metrics Update Between |SRC_VERSION| and |NEW_VERSION|
================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.


New Metrics in |NEW_VERSION|
--------------------------------------

The following metrics are new in ScyllaDB |NEW_VERSION| compared to |PRECEDING_VERSION|.

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_auth_cache_permissions
     - Total number of permission sets currently cached across all roles.
   * - scylla_auth_cache_roles
     - Number of roles currently cached.
   * - scylla_cql_forwarded_requests
     - Counts the total number of attempts to forward CQL requests to other nodes.
       One request may be forwarded multiple times, particularly when a write is
       handled by a non-replica node.
   * - scylla_cql_write_consistency_levels_disallowed_violations
     - Counts the number of write_consistency_levels_disallowed guardrail violations,
       i.e. attempts to write with a forbidden consistency level.
   * - scylla_cql_write_consistency_levels_warned_violations
     - Counts the number of write_consistency_levels_warned guardrail violations,
       i.e. attempts to write with a discouraged consistency level.
   * - scylla_cql_writes_per_consistency_level
     - Counts the number of writes for each consistency level.
   * - scylla_io_queue_integrated_disk_queue_length
     - Length of the integrated disk queue.
   * - scylla_io_queue_integrated_queue_length
     - Length of the integrated queue.
   * - scylla_logstor_sm_bytes_freed
     - Counts the number of data bytes freed.
   * - scylla_logstor_sm_bytes_read
     - Counts the number of bytes read from the disk.
   * - scylla_logstor_sm_bytes_written
     - Counts the number of bytes written to the disk.
   * - scylla_logstor_sm_compaction_bytes_written
     - Counts the number of bytes written to the disk by compaction.
   * - scylla_logstor_sm_compaction_data_bytes_written
     - Counts the number of data bytes written to the disk by compaction.
   * - scylla_logstor_sm_compaction_records_rewritten
     - Counts the number of records rewritten during compaction.
   * - scylla_logstor_sm_compaction_records_skipped
     - Counts the number of records skipped during compaction.
   * - scylla_logstor_sm_compaction_segments_freed
     - Counts the number of data bytes written to the disk.
   * - scylla_logstor_sm_disk_usage
     - Total disk usage.
   * - scylla_logstor_sm_free_segments
     - Counts the number of free segments currently available.
   * - scylla_logstor_sm_segment_pool_compaction_segments_get
     - Counts the number of segments taken from the segment pool for compaction.
   * - scylla_logstor_sm_segment_pool_normal_segments_get
     - Counts the number of segments taken from the segment pool for normal writes.
   * - scylla_logstor_sm_segment_pool_normal_segments_wait
     - Counts the number of times normal writes had to wait for a segment to become
       available in the segment pool.
   * - scylla_logstor_sm_segment_pool_segments_put
     - Counts the number of segments returned to the segment pool.
   * - scylla_logstor_sm_segment_pool_separator_segments_get
     - Counts the number of segments taken from the segment pool for separator writes.
   * - scylla_logstor_sm_segment_pool_size
     - Counts the number of segments in the segment pool.
   * - scylla_logstor_sm_segments_allocated
     - Counts the number of segments allocated.
   * - scylla_logstor_sm_segments_compacted
     - Counts the number of segments compacted.
   * - scylla_logstor_sm_segments_freed
     - Counts the number of segments freed.
   * - scylla_logstor_sm_segments_in_use
     - Counts the number of segments currently in use.
   * - scylla_logstor_sm_separator_buffer_flushed
     - Counts the number of times the separator buffer has been flushed.
   * - scylla_logstor_sm_separator_bytes_written
     - Counts the number of bytes written to the separator.
   * - scylla_logstor_sm_separator_data_bytes_written
     - Counts the number of data bytes written to the separator.
   * - scylla_logstor_sm_separator_flow_control_delay
     - Current delay applied to writes to control separator debt in microseconds.
   * - scylla_logstor_sm_separator_segments_freed
     - Counts the number of segments freed by the separator.
   * - scylla_transport_cql_pending_response_memory
     - Holds the total memory in bytes consumed by responses waiting to be sent.
   * - scylla_transport_cql_request_histogram_bytes
     - A histogram of received bytes in CQL messages of a specific kind and
       specific scheduling group.
   * - scylla_transport_cql_requests_serving
     - Holds the number of requests that are being processed right now.
   * - scylla_transport_cql_response_histogram_bytes
     - A histogram of received bytes in CQL messages of a specific kind and
       specific scheduling group.
   * - scylla_transport_requests_forwarded_failed
     - Counts the number of requests that were forwarded to another replica
       but failed to execute there.
   * - scylla_transport_requests_forwarded_prepared_not_found
     - Counts the number of requests that were forwarded to another replica
       but failed there because the statement was not prepared on the target.
       When this happens, the coordinator performs an additional remote call
       to prepare the statement on the replica and retries the EXECUTE request
       afterwards.
   * - scylla_transport_requests_forwarded_redirected
     - Counts the number of requests that were forwarded to another replica
       but that replica responded with a redirect to another node. This can
       happen when replica has stale information about the cluster topology or
       when the request is handled by a node that is not a replica for the data
       being accessed by the request.
   * - scylla_transport_requests_forwarded_successfully
     - Counts the number of requests that were forwarded to another replica
       and executed successfully there.

