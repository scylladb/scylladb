/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_set>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_ptr.hh>
#include "raft/server.hh"
#include "schema/schema_fwd.hh"
#include "service/raft/raft_rpc.hh"
#include "seastarx.hh"

namespace raft { struct metrics_options; }

namespace service::strong_consistency {

/// Raft metrics of the strongly consistent tablets of one table on this shard.
///
/// Their raft servers and RPC modules share the counters held here, so the table is
/// exported as one series per metric; gauges are summed over the added servers.
class table_metrics {
    table_id _table;
    lw_shared_ptr<raft::server::stats> _server_stats = make_lw_shared<raft::server::stats>();
    lw_shared_ptr<raft_rpc::stats> _rpc_stats = make_lw_shared<raft_rpc::stats>();
    std::unordered_set<raft::server*> _servers;

    // What one sweep of _servers yields. All gauges of a scrape read the same
    // sweep, so the values they report are consistent with each other.
    struct sample {
        uint64_t leaders = 0;
        uint64_t in_memory_log_size = 0;
        uint64_t log_memory_usage = 0;
        uint64_t uncommitted_entries = 0;
        uint64_t unapplied_entries = 0;
        uint64_t log_limiter_waiters = 0;
        raft::blocked_followers blocked;
    };
    sample _sample;
    seastar::lowres_clock::time_point _sampled_at = {};
    const sample& take_sample();

    // Destroyed first, so that no gauge is evaluated once _servers is gone.
    seastar::metrics::metric_groups _metrics;

    void register_table_gauges(const raft::metrics_options& options);

public:
    enum class reporting { none, per_node, per_shard };

    table_metrics(table_id table, const sstring& ks_name, const sstring& cf_name, reporting mode);
    // The gauges capture this.
    table_metrics(const table_metrics&) = delete;
    table_metrics& operator=(const table_metrics&) = delete;

    table_id table() const { return _table; }

    // Shared with the raft servers and RPC modules of the tablets.
    lw_shared_ptr<raft::server::stats> server_stats() const { return _server_stats; }
    lw_shared_ptr<raft_rpc::stats> rpc_stats() const { return _rpc_stats; }

    // A server must be removed before it is destroyed.
    void add_server(raft::server& server) { _servers.insert(&server); }
    void remove_server(raft::server& server) { _servers.erase(&server); }

    // Stops exporting; the servers still attached keep updating the counters.
    void unregister() { _metrics.clear(); }
};

}
