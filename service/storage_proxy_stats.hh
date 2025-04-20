/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/estimated_histogram.hh"
#include "utils/histogram.hh"
#include <seastar/core/metrics.hh>
#include "locator/host_id.hh"

namespace locator { class topology; }

namespace service {

namespace storage_proxy_stats {

// split statistics counters
struct split_stats {
    static seastar::metrics::label datacenter_label;

private:
    struct stats_counter {
        uint64_t val = 0;
    };

    // counter of operations performed on a local Node
    stats_counter _local;
    // counters of operations performed on external Nodes aggregated per Nodes' DCs
    std::unordered_map<sstring, stats_counter> _dc_stats;
    // collectd registrations container
    seastar::metrics::metric_groups _metrics;
    // a prefix string that will be used for a collectd counters' description
    sstring _short_description_prefix;
    sstring _long_description_prefix;
    // a statistics category, e.g. "client" or "replica"
    sstring _category;
    // type of operation (data/digest/mutation_data)
    sstring _op_type;
    // whether to register per-endpoint metrics automatically
    bool _auto_register_metrics;

    scheduling_group _sg;
public:
    /**
     * @param category a statistics category, e.g. "client" or "replica"
     * @param short_description_prefix a short description prefix
     * @param long_description_prefix a long description prefix
     */
    split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type, bool auto_register_metrics = true);

    void register_metrics_local();
    void register_metrics_for(sstring dc, locator::host_id ep);

    /**
     * Get a reference to the statistics counter corresponding to the given
     * destination.
     *
     * @param ep address of a destination
     *
     * @return a reference to the requested counter
     */
    uint64_t& get_ep_stat(const locator::topology& topo, locator::host_id ep) noexcept;
};

struct write_stats {
    // total write attempts
    split_stats writes_attempts;
    split_stats writes_errors;
    split_stats background_replica_writes_failed;

    // write attempts due to Read Repair logic
    split_stats read_repair_write_attempts;

    utils::timed_rate_moving_average write_unavailables;
    utils::timed_rate_moving_average write_timeouts;
    utils::timed_rate_moving_average write_rate_limited_by_replicas;
    utils::timed_rate_moving_average write_rate_limited_by_coordinator;

    utils::timed_rate_moving_average_summary_and_histogram write;

    utils::timed_rate_moving_average cas_write_unavailables;
    utils::timed_rate_moving_average cas_write_timeouts;

    utils::timed_rate_moving_average_summary_and_histogram cas_write;

    utils::estimated_histogram cas_write_contention;

    uint64_t writes = 0;
    // A CQL write query arrived to a non-replica node and was
    // forwarded by a coordinator to a replica
    uint64_t writes_coordinator_outside_replica_set = 0;
    // A CQL read query arrived to a non-replica node and was
    // forwarded by a coordinator to a replica
    uint64_t reads_coordinator_outside_replica_set = 0;
    uint64_t background_writes = 0; // client no longer waits for the write
    uint64_t throttled_writes = 0; // total number of writes ever delayed due to throttling
    uint64_t throttled_base_writes = 0; // current number of base writes delayed due to view update backlog
    uint64_t total_throttled_base_writes = 0; // total number of base writes delayed due to view update backlog
    uint64_t background_writes_failed = 0;
    uint64_t writes_failed_due_to_too_many_in_flight_hints = 0;

    uint64_t cas_write_unfinished_commit = 0;
    uint64_t cas_write_condition_not_met = 0;
    uint64_t cas_write_timeout_due_to_uncertainty = 0;
    uint64_t cas_failed_read_round_optimization = 0;
    uint16_t cas_now_pruning = 0;
    uint64_t cas_prune = 0;
    uint64_t cas_coordinator_dropped_prune = 0;
    uint64_t cas_replica_dropped_prune = 0;

    seastar::metrics::metric_groups _metrics;

    std::chrono::microseconds last_mv_flow_control_delay; // delay added for MV flow control in the last request
    uint64_t mv_flow_control_delay = 0; // total delay added for MV flow control (in microseconds)
public:
    write_stats();
    write_stats(const sstring& category, bool auto_register_stats);

    void register_stats();
    void register_split_metrics_local();
};

struct stats : public write_stats {
    seastar::metrics::metric_groups _metrics;
    utils::timed_rate_moving_average read_timeouts;
    utils::timed_rate_moving_average read_unavailables;
    utils::timed_rate_moving_average read_rate_limited_by_replicas;
    utils::timed_rate_moving_average read_rate_limited_by_coordinator;
    utils::timed_rate_moving_average range_slice_timeouts;
    utils::timed_rate_moving_average range_slice_unavailables;

    utils::timed_rate_moving_average cas_read_timeouts;
    utils::timed_rate_moving_average cas_read_unavailables;

    utils::estimated_histogram cas_read_contention;

    uint64_t read_repair_attempts = 0;
    uint64_t read_repair_repaired_blocking = 0;
    uint64_t read_repair_repaired_background = 0;
    uint64_t global_read_repairs_canceled_due_to_concurrent_write = 0;

    // number of mutations received as a coordinator
    uint64_t received_mutations = 0;

    // number of counter updates received as a leader
    uint64_t received_counter_updates = 0;

    // number of forwarded mutations
    uint64_t forwarded_mutations = 0;
    uint64_t forwarding_errors = 0;

    // number of read requests received as a replica
    uint64_t replica_data_reads = 0;
    uint64_t replica_digest_reads = 0;
    uint64_t replica_mutation_data_reads = 0;

    uint64_t replica_cross_shard_ops = 0;

    utils::timed_rate_moving_average_summary_and_histogram read;
    utils::timed_rate_moving_average_summary_and_histogram range;

    utils::timed_rate_moving_average_summary_and_histogram cas_read;
    uint64_t reads = 0;
    uint64_t foreground_reads = 0; // client still waits for the read
    uint64_t read_retries = 0; // read is retried with new limit
    uint64_t speculative_digest_reads = 0;
    uint64_t speculative_data_reads = 0;

    uint64_t cas_read_unfinished_commit = 0;
    uint64_t cas_foreground = 0;
    uint64_t cas_total_running = 0;
    uint64_t cas_total_operations = 0;

    // Data read attempts
    split_stats data_read_attempts;
    split_stats data_read_completed;
    split_stats data_read_errors;

    // Digest read attempts
    split_stats digest_read_attempts;
    split_stats digest_read_completed;
    split_stats digest_read_errors;

    // Mutation data read attempts
    split_stats mutation_data_read_attempts;
    split_stats mutation_data_read_completed;
    split_stats mutation_data_read_errors;

    // Received hints
    uint64_t received_hints_total = 0;
    uint64_t received_hints_bytes_total = 0;

public:
    stats();
    void register_stats();
    void register_split_metrics_local();
};

 /*** This struct represents stats that has meaning (only or also)
 * globally. For example background_write_bytes are used to decide
 * if to throttle requests and it make little sense to check it
 * per scheduling group, on the other hand this statistic has value
 * in figuring out how much load each scheduling group generates
 * on the system, this statistic should be handled elsewhere, i.e:
 * in the write_stats struct.
 */
struct global_write_stats {
    seastar::metrics::metric_groups _metrics;
    uint64_t background_write_bytes = 0;
    uint64_t queued_write_bytes = 0;
    void register_stats();
};

/***
 *  Following the convention of stats and write_stats
 */
struct global_stats : public global_write_stats {
    void register_stats();
};

}

}
