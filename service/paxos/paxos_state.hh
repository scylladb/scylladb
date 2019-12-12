/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
/*
 * Copyright (C) 2019 ScyllaDB
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
#include "service/paxos/proposal.hh"
#include "log.hh"
#include "digest_algorithm.hh"
#include "db/timeout_clock.hh"
#include <unordered_map>
#include "utils/UUID_gen.hh"
#include "service/paxos/prepare_response.hh"

namespace service::paxos {

using clock_type = db::timeout_clock;

// The state of a CAS update of a given primary key as persisted in the paxos table.
class paxos_state {
    using key_semaphore = basic_semaphore<semaphore_default_exception_factory, clock_type>;
    using map = std::unordered_map<dht::token, key_semaphore>;
    // A lock map protecting concurrent reads and writes of the same row in system.paxos table.
    // Locks are local to the shard which owns the corresponding token range.
    static thread_local map _locks;

    static key_semaphore& get_semaphore_for_key(const dht::token& key);
    static void release_semaphore_for_key(const dht::token& key);

    utils::UUID _promised_ballot = utils::UUID_gen::min_time_UUID(0);
    std::optional<proposal> _accepted_proposal;
    std::optional<proposal> _most_recent_commit;

public:
    //
    // A thin RAII aware wrapper around the lock map to garbage
    // collect the decorated key from the map on unlock if there
    // are no waiters.
    ///
    template<typename Func>
    static
    futurize_t<std::result_of_t<Func()>> with_locked_key(const dht::token& key, clock_type::time_point timeout, Func func) {
        return with_semaphore(get_semaphore_for_key(key), 1, timeout - clock_type::now(), std::move(func)).finally([key] {
            release_semaphore_for_key(key);
        });
    }

    static logging::logger logger;

    paxos_state() {}

    paxos_state(utils::UUID promised, std::optional<proposal> accepted, std::optional<proposal> commit)
        : _promised_ballot(std::move(promised))
        , _accepted_proposal(std::move(accepted))
        , _most_recent_commit(std::move(commit)) {}
    // Replica RPC endpoint for Paxos "prepare" phase.
    static future<prepare_response> prepare(tracing::trace_state_ptr tr_state, schema_ptr schema,
            lw_shared_ptr<query::read_command> cmd, partition_key key, utils::UUID ballot,
            bool only_digest, query::digest_algorithm da, clock_type::time_point timeout);
    // Replica RPC endpoint for Paxos "accept" phase.
    static future<bool> accept(tracing::trace_state_ptr tr_state, schema_ptr schema, proposal proposal,
            clock_type::time_point timeout);
    // Replica RPC endpoint for Paxos "learn".
    static future<> learn(schema_ptr schema, proposal decision, clock_type::time_point timeout, tracing::trace_state_ptr tr_state);
};

} // end of namespace "service::paxos"

