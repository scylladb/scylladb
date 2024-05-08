/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/timer.hh>
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

struct batchlog_manager_config {
    std::chrono::duration<double> write_request_timeout;
    uint64_t replay_rate = std::numeric_limits<uint64_t>::max();
    std::chrono::milliseconds delay = std::chrono::milliseconds(0);
};

class batchlog_manager : public peering_sharded_service<batchlog_manager> {
private:
    static constexpr uint32_t replay_interval = 60 * 1000; // milliseconds
    static constexpr uint32_t page_size = 128; // same as HHOM, for now, w/out using any heuristics. TODO: set based on avg batch size.

    using clock_type = lowres_clock;

    struct stats {
        uint64_t write_attempts = 0;
    } _stats;

    seastar::metrics::metric_groups _metrics;

    size_t _total_batches_replayed = 0;
    cql3::query_processor& _qp;
    db::system_keyspace& _sys_ks;
    db_clock::duration _write_request_timeout;
    uint64_t _replay_rate;
    std::chrono::milliseconds _delay;
    semaphore _sem{1};
    seastar::gate _gate;
    unsigned _cpu = 0;
    seastar::abort_source _stop;
    future<> _loop_done;

    future<> replay_all_failed_batches();
public:
    // Takes a QP, not a distributes. Because this object is supposed
    // to be per shard and does no dispatching beyond delegating the the
    // shard qp (which is what you feed here).
    batchlog_manager(cql3::query_processor&, db::system_keyspace& sys_ks, batchlog_manager_config config);

    // abort the replay loop and return its future.
    future<> drain();
    future<> stop();

    future<> do_batch_log_replay();

    future<size_t> count_all_batches() const;
    size_t get_total_batches_replayed() const {
        return _total_batches_replayed;
    }
    db_clock::duration get_batch_log_timeout() const;
private:
    future<> batchlog_replay_loop();
};

}
