/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <unordered_map>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>

#include "gms/inet_address.hh"
#include "db_clock.hh"
#include "mutation.hh"
#include "utils/UUID.hh"

#include <chrono>
#include <limits>
#include <random>

namespace cql3 {

class query_processor;

} // namespace cql3

namespace db {

struct batchlog_manager_config {
    std::chrono::duration<double> write_request_timeout;
    uint64_t replay_rate = std::numeric_limits<uint64_t>::max();
    std::chrono::milliseconds delay;
};

class batchlog_manager {
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
    db_clock::duration _write_request_timeout;
    uint64_t _replay_rate;
    timer<clock_type> _timer;
    std::chrono::milliseconds _delay;
    semaphore _sem{1};
    seastar::gate _gate;
    unsigned _cpu = 0;
    bool _stop = false;

    std::default_random_engine _e1{std::random_device{}()};

    future<> replay_all_failed_batches();
public:
    // Takes a QP, not a distributes. Because this object is supposed
    // to be per shard and does no dispatching beyond delegating the the
    // shard qp (which is what you feed here).
    batchlog_manager(cql3::query_processor&, batchlog_manager_config config);

    future<> start();
    future<> stop();

    future<> do_batch_log_replay();

    future<size_t> count_all_batches() const;
    size_t get_total_batches_replayed() const {
        return _total_batches_replayed;
    }
    mutation get_batch_log_mutation_for(const std::vector<mutation>&, const utils::UUID&, int32_t);
    mutation get_batch_log_mutation_for(const std::vector<mutation>&, const utils::UUID&, int32_t, db_clock::time_point);
    db_clock::duration get_batch_log_timeout() const;

    std::unordered_set<gms::inet_address> endpoint_filter(const sstring&, const std::unordered_map<sstring, std::unordered_set<gms::inet_address>>&);
};

extern distributed<batchlog_manager> _the_batchlog_manager;

inline distributed<batchlog_manager>& get_batchlog_manager() {
    return _the_batchlog_manager;
}

inline batchlog_manager& get_local_batchlog_manager() {
    return _the_batchlog_manager.local();
}

}
