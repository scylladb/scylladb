/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "gms/inet_address.hh"
#include "utils/estimated_histogram.hh"
#include "utils/histogram.hh"
#include <seastar/core/metrics.hh>

namespace service {

namespace storage_proxy_stats {

// split statistics counters
struct split_stats {
    static seastar::metrics::label datacenter_label;
    static seastar::metrics::label op_type_label;
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

public:
    /**
     * @param category a statistics category, e.g. "client" or "replica"
     * @param short_description_prefix a short description prefix
     * @param long_description_prefix a long description prefix
     */
    split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type, bool auto_register_metrics = true);

    void register_metrics_local();
    void register_metrics_for(gms::inet_address ep);

    /**
     * Get a reference to the statistics counter corresponding to the given
     * destination.
     *
     * @param ep address of a destination
     *
     * @return a reference to the requested counter
     */
    uint64_t& get_ep_stat(gms::inet_address ep);
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

    utils::timed_rate_moving_average_and_histogram write;
    utils::estimated_histogram estimated_write;

    uint64_t writes = 0;
    uint64_t background_writes = 0; // client no longer waits for the write
    uint64_t background_write_bytes = 0;
    uint64_t queued_write_bytes = 0;
    uint64_t throttled_writes = 0; // total number of writes ever delayed due to throttling
    uint64_t throttled_base_writes = 0; // current number of base writes delayed due to view update backlog
    uint64_t background_writes_failed = 0;
public:
    write_stats();
    write_stats(const sstring& category, bool auto_register_stats);

    void register_metrics_local();
    void register_metrics_for(gms::inet_address ep);
};

struct stats : public write_stats {
    utils::timed_rate_moving_average read_timeouts;
    utils::timed_rate_moving_average read_unavailables;
    utils::timed_rate_moving_average range_slice_timeouts;
    utils::timed_rate_moving_average range_slice_unavailables;

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

    utils::timed_rate_moving_average_and_histogram read;
    utils::timed_rate_moving_average_and_histogram range;
    utils::estimated_histogram estimated_read;
    utils::estimated_histogram estimated_range;
    uint64_t reads = 0;
    uint64_t foreground_reads = 0; // client still waits for the read
    uint64_t read_retries = 0; // read is retried with new limit
    uint64_t speculative_digest_reads = 0;
    uint64_t speculative_data_reads = 0;

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

public:
    stats();

    void register_metrics_local();
    void register_metrics_for(gms::inet_address ep);
};

}

}
