/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/mutation.hh"
#include "query/query-result.hh"
#include "locator/tablets.hh"
#include "utils/histogram.hh"
#include <seastar/core/metrics.hh>
#include <seastar/util/noncopyable_function.hh>

namespace gms {

class gossiper;

}

namespace service::strong_consistency {

class groups_manager;

// Classifies a strongly consistent read based on the CQL consistency level.
//
// linearizable: the read is forwarded to the raft leader, where read_barrier()
//   is performed locally. This way we read the most up-to-date applied data.
//   Currently mapped from CL=QUORUM (the default).
//
// non_linearizable: the read is performed on the local replica without
//   a read_barrier. Because of this, the read may return slightly stale data,
//   so reads from different nodes may not be linearizable. Currently mapped from CL=ONE.
enum class read_type {
    linearizable,
    non_linearizable,
};

struct need_redirect {
    locator::tablet_replica target;
    locator::host_id_or_exception_callback on_forwarding_finished;
};
template <typename T = std::monostate>
using value_or_redirect = std::variant<T, need_redirect>;

struct stats {
    utils::timed_rate_moving_average_summary_and_histogram write;
    uint64_t write_errors_timeout = 0;
    uint64_t write_errors_status_unknown = 0;
    uint64_t write_errors_other = 0;
    uint64_t write_node_bounces = 0;
    uint64_t write_shard_bounces = 0;

    utils::timed_rate_moving_average_summary_and_histogram linearizable_read;
    utils::timed_rate_moving_average_summary_and_histogram non_linearizable_read;
    uint64_t read_errors_timeout = 0;
    uint64_t read_errors_other = 0;
    uint64_t read_node_bounces = 0;
    uint64_t read_shard_bounces = 0;

    seastar::metrics::metric_groups _metrics;

    void register_stats();
};

class coordinator : public peering_sharded_service<coordinator> {
public:
    using timeout_clock = typename db::timeout_clock;

private:
    groups_manager& _groups_manager;
    replica::database& _db;
    gms::gossiper& _gossiper;
    stats _stats;

    struct operation_ctx;
    future<value_or_redirect<operation_ctx>> create_operation_ctx(const schema& schema,
        const dht::token& token,
        abort_source& as,
        bool use_leader_cache);
public:
    coordinator(groups_manager& groups_manager, replica::database& db, gms::gossiper& gossiper);

    stats& get_stats() { return _stats; }

    using mutation_gen = noncopyable_function<mutation(api::timestamp_type)>;
    future<value_or_redirect<>> mutate(schema_ptr schema, 
        const dht::token& token,
        mutation_gen&& mutation_gen,
        timeout_clock::time_point timeout,
        abort_source& as);

    using query_result_type = value_or_redirect<lw_shared_ptr<query::result>>;
    future<query_result_type> query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        read_type rtype,
        tracing::trace_state_ptr trace_state,
        timeout_clock::time_point timeout,
        abort_source& as);

    // Sends an RPC to every host that holds a tablet replica of the given table, asking it to wait
    // until the raft groups for those tablets are started and ready to serve queries.
    // For the local node, waits directly without an RPC.
    future<> wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout);
};

}
