/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/abort_source.hh>

#include "db_clock.hh"

#include <chrono>
#include <limits>

namespace cql3 {

class query_processor;

} // namespace cql3

namespace db {

class system_keyspace;

using all_batches_replayed = bool_class<struct all_batches_replayed_tag>;

struct batchlog_manager_config {
    db_clock::duration replay_timeout;
    uint64_t replay_rate = std::numeric_limits<uint64_t>::max();
    std::chrono::milliseconds delay = std::chrono::milliseconds(0);
    unsigned replay_cleanup_after_replays;
};

enum class batchlog_stage : int8_t {
    initial,
    failed_replay
};

class batchlog_manager : public peering_sharded_service<batchlog_manager> {
public:
    using post_replay_cleanup = bool_class<class post_replay_cleanup_tag>;

private:
    static constexpr std::chrono::seconds replay_interval = std::chrono::seconds(60);
    static constexpr uint32_t page_size = 128; // same as HHOM, for now, w/out using any heuristics. TODO: set based on avg batch size.
    static constexpr std::chrono::seconds write_timeout = std::chrono::seconds(300);

    using clock_type = lowres_clock;

    struct stats {
        uint64_t write_attempts = 0;
    } _stats;

    seastar::metrics::metric_groups _metrics;

    cql3::query_processor& _qp;
    db::system_keyspace& _sys_ks;
    db_clock::duration _replay_timeout;
    uint64_t _replay_rate;
    std::chrono::milliseconds _delay;
    unsigned _replay_cleanup_after_replays = 100;
    semaphore _sem{1};
    seastar::named_gate _gate;
    unsigned _cpu = 0;
    seastar::abort_source _stop;
    future<> _loop_done;

    gc_clock::time_point _last_replay;

    // Was the v1 -> v2 migration already done since last restart?
    // The migration is attempted once after each restart. This is redundant but
    // keeps thing simple. Once no upgrade path exists from a ScyllaDB version
    // which can still produce v1 entries, this migration code can be removed.
    bool _migration_done = false;

    future<> maybe_migrate_v1_to_v2();

    future<all_batches_replayed> replay_all_failed_batches(post_replay_cleanup cleanup);
public:
    // Takes a QP, not a distributes. Because this object is supposed
    // to be per shard and does no dispatching beyond delegating the the
    // shard qp (which is what you feed here).
    batchlog_manager(cql3::query_processor&, db::system_keyspace& sys_ks, batchlog_manager_config config);

    // abort the replay loop and return its future.
    future<> drain();
    future<> stop();

    future<all_batches_replayed> do_batch_log_replay(post_replay_cleanup cleanup);

    future<size_t> count_all_batches() const;
    gc_clock::time_point get_last_replay() const {
        return _last_replay;
    }
private:
    future<> batchlog_replay_loop();
};

}
