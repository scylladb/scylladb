# Scylla Metric Update - Scylla 4.4 to 4.5

Scylla 4.5 Dashboards are available as part of the latest [Scylla Monitoring Stack](https://monitoring.docs.scylladb.com).

## The following metrics are new in Scylla 4.5

* scylla_io_queue_total_delay_sec
* scylla_cache_dummy_row_hits - see [Dummy Rows](https://opensource.docs.scylladb.com/branch-5.2/glossary.md#term-Dummy-Rows)

New Reaft related metrics

* scylla_raft_add_entries
* scylla_raft_applied_entries
* scylla_raft_in_memory_log_size
* scylla_raft_messages_received
* scylla_raft_messages_sent
* scylla_raft_persisted_log_entries
* scylla_raft_polls
* scylla_raft_queue_entries_for_apply
* scylla_raft_sm_load_snapshot
* scylla_raft_snapshots_taken
* scylla_raft_store_snapshot
* scylla_raft_store_term_and_vote
* scylla_raft_truncate_persisted_log
* scylla_raft_waiter_awaiken
* scylla_raft_waiter_dropped

## The following metrics are removed in Scylla 4.5

* scylla_database_querier_cache_memory_based_evictions
* scylla_sstables_sstable_partition_reads
