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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include <unordered_map>
#include "core/future.hh"
#include "core/distributed.hh"
#include "core/timer.hh"
#include "cql3/query_processor.hh"
#include "gms/inet_address.hh"
#include "db_clock.hh"

namespace db {

class batchlog_manager {
private:
    static constexpr uint32_t replay_interval = 60 * 1000; // milliseconds
    static constexpr uint32_t page_size = 128; // same as HHOM, for now, w/out using any heuristics. TODO: set based on avg batch size.

    using clock_type = lowres_clock;

    size_t _total_batches_replayed = 0;
    cql3::query_processor& _qp;
    timer<clock_type> _timer;
    semaphore _sem;
    bool _stop = false;

    future<> replay_all_failed_batches();
public:
    // Takes a QP, not a distributes. Because this object is supposed
    // to be per shard and does no dispatching beyond delegating the the
    // shard qp (which is what you feed here).
    batchlog_manager(cql3::query_processor&);
    batchlog_manager(distributed<cql3::query_processor>& qp)
        : batchlog_manager(qp.local())
    {}

    future<> start();
    future<> stop();

    // for testing.
    future<> do_batch_log_replay() {
        return replay_all_failed_batches();
    }
    future<size_t> count_all_batches() const;
    size_t get_total_batches_replayed() const {
        return _total_batches_replayed;
    }
    mutation get_batch_log_mutation_for(std::vector<mutation>, const utils::UUID&, int32_t);
    mutation get_batch_log_mutation_for(std::vector<mutation>, const utils::UUID&, int32_t, db_clock::time_point);
    db_clock::duration get_batch_log_timeout() const;

    class endpoint_filter {
    private:
        const sstring _local_rack;
        const std::unordered_map<sstring, std::vector<gms::inet_address>> _endpoints;

    public:
        endpoint_filter(sstring, std::unordered_map<sstring, std::vector<gms::inet_address>>);
        /**
         * @return list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
         */
        std::vector<gms::inet_address> filter() const;
    };
};

}
