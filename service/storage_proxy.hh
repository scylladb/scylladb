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
 * Copyright (C) 2015 ScyllaDB
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

#include "database.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "core/distributed.hh"
#include "db/consistency_level_type.hh"
#include "db/read_repair_decision.hh"
#include "db/write_type.hh"
#include "db/hints/manager.hh"
#include "db/view/view_update_backlog.hh"
#include "db/view/node_view_update_backlog.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics.hh>
#include "frozen_mutation.hh"
#include "db/config.hh"
#include "storage_proxy_stats.hh"

namespace compat {

class one_or_two_partition_ranges;

}

namespace service {

class abstract_write_response_handler;
class abstract_read_executor;
class mutation_holder;

using replicas_per_token_range = std::unordered_map<dht::token_range, std::vector<utils::UUID>>;

struct view_update_backlog_timestamped {
    db::view::update_backlog backlog;
    api::timestamp_type ts;
};

class storage_proxy : public seastar::async_sharded_service<storage_proxy> /*implements StorageProxyMBean*/ {
public:
    using clock_type = lowres_clock;
    struct config {
        stdx::optional<std::vector<sstring>> hinted_handoff_enabled = {};
        size_t available_memory;
    };
private:

    using response_id_type = uint64_t;
    struct unique_response_handler {
        response_id_type id;
        storage_proxy& p;
        unique_response_handler(storage_proxy& p_, response_id_type id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x);
        ~unique_response_handler();
        response_id_type release();
    };

public:
    static const sstring COORDINATOR_STATS_CATEGORY;
    static const sstring REPLICA_STATS_CATEGORY;

    using write_stats = storage_proxy_stats::write_stats;
    using stats = storage_proxy_stats::stats;

    class coordinator_query_options {
        clock_type::time_point _timeout;

    public:
        tracing::trace_state_ptr trace_state = nullptr;
        replicas_per_token_range preferred_replicas;
        stdx::optional<db::read_repair_decision> read_repair_decision;

        coordinator_query_options(clock_type::time_point timeout,
                tracing::trace_state_ptr trace_state = nullptr,
                replicas_per_token_range preferred_replicas = { },
                stdx::optional<db::read_repair_decision> read_repair_decision = { })
            : _timeout(timeout)
            , trace_state(std::move(trace_state))
            , preferred_replicas(std::move(preferred_replicas))
            , read_repair_decision(read_repair_decision) {
        }

        clock_type::time_point timeout(storage_proxy& sp) const {
            return _timeout;
        }
    };

    struct coordinator_query_result {
        foreign_ptr<lw_shared_ptr<query::result>> query_result;
        replicas_per_token_range last_replicas;
        db::read_repair_decision read_repair_decision;

        coordinator_query_result(foreign_ptr<lw_shared_ptr<query::result>> query_result,
                replicas_per_token_range last_replicas = {},
                db::read_repair_decision read_repair_decision = db::read_repair_decision::NONE)
            : query_result(std::move(query_result))
            , last_replicas(std::move(last_replicas))
            , read_repair_decision(std::move(read_repair_decision)) {
        }
    };
private:
    distributed<database>& _db;
    response_id_type _next_response_id;
    std::unordered_map<response_id_type, ::shared_ptr<abstract_write_response_handler>> _response_handlers;
    // This buffer hold ids of throttled writes in case resource consumption goes
    // below the threshold and we want to unthrottle some of them. Without this throttled
    // request with dead or slow replica may wait for up to timeout ms before replying
    // even if resource consumption will go to zero. Note that some requests here may
    // be already completed by the point they tried to be unthrottled (request completion does
    // not remove request from the buffer), but this is fine since request ids are unique, so we
    // just skip an entry if request no longer exists.
    circular_buffer<response_id_type> _throttled_writes;
    db::hints::resource_manager _hints_resource_manager;
    stdx::optional<db::hints::manager> _hints_manager;
    db::hints::manager _hints_for_views_manager;
    stats _stats;
    static constexpr float CONCURRENT_SUBREQUESTS_MARGIN = 0.10;
    // for read repair chance calculation
    std::default_random_engine _urandom;
    std::uniform_real_distribution<> _read_repair_chance = std::uniform_real_distribution<>(0,1);
    seastar::metrics::metric_groups _metrics;
    uint64_t _background_write_throttle_threahsold;
    inheriting_concrete_execution_stage<
            future<>,
            storage_proxy*,
            std::vector<mutation>,
            db::consistency_level,
            clock_type::time_point,
            tracing::trace_state_ptr,
            bool> _mutate_stage;
    db::view::node_update_backlog& _max_view_update_backlog;
    std::unordered_map<gms::inet_address, view_update_backlog_timestamped> _view_update_backlogs;

private:
    void uninit_messaging_service();
    future<coordinator_query_result> query_singular(lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl,
            coordinator_query_options optional_params);
    response_id_type register_response_handler(shared_ptr<abstract_write_response_handler>&& h);
    void remove_response_handler(response_id_type id);
    void got_response(response_id_type id, gms::inet_address from, stdx::optional<db::view::update_backlog> backlog);
    void got_failure_response(response_id_type id, gms::inet_address from, size_t count, stdx::optional<db::view::update_backlog> backlog);
    future<> response_wait(response_id_type id, clock_type::time_point timeout);
    ::shared_ptr<abstract_write_response_handler>& get_write_response_handler(storage_proxy::response_id_type id);
    response_id_type create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address>, tracing::trace_state_ptr tr_state, storage_proxy::write_stats& stats);
    response_id_type create_write_response_handler(const mutation&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state);
    response_id_type create_write_response_handler(const std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state);
    void send_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    template<typename Range>
    size_t hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets, db::write_type type, tracing::trace_state_ptr tr_state) noexcept;
    void hint_to_dead_endpoints(response_id_type, db::consistency_level);
    template<typename Range>
    bool cannot_hint(const Range& targets, db::write_type type);
    bool hints_enabled(db::write_type type) noexcept;
    db::hints::manager& hints_manager_for(db::write_type type);
    std::vector<gms::inet_address> get_live_endpoints(keyspace& ks, const dht::token& token);
    std::vector<gms::inet_address> get_live_sorted_endpoints(keyspace& ks, const dht::token& token);
    db::read_repair_decision new_read_repair_decision(const schema& s);
    ::shared_ptr<abstract_read_executor> get_read_executor(lw_shared_ptr<query::read_command> cmd,
            schema_ptr schema,
            dht::partition_range pr,
            db::consistency_level cl,
            db::read_repair_decision repair_decision,
            tracing::trace_state_ptr trace_state,
            const std::vector<gms::inet_address>& preferred_endpoints);
    future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> query_result_local(schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                                                           query::result_options opts,
                                                                           tracing::trace_state_ptr trace_state,
                                                                           clock_type::time_point timeout,
                                                                           uint64_t max_size = query::result_memory_limiter::maximum_result_size);
    future<query::result_digest, api::timestamp_type, cache_temperature> query_result_local_digest(schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                                                                                   tracing::trace_state_ptr trace_state,
                                                                                                   clock_type::time_point timeout,
                                                                                                   query::digest_algorithm da,
                                                                                                   uint64_t max_size  = query::result_memory_limiter::maximum_result_size);
    future<coordinator_query_result> query_partition_key_range(lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector partition_ranges,
            db::consistency_level cl,
            coordinator_query_options optional_params);
    float estimate_result_rows_per_range(lw_shared_ptr<query::read_command> cmd, keyspace& ks);
    static std::vector<gms::inet_address> intersection(const std::vector<gms::inet_address>& l1, const std::vector<gms::inet_address>& l2);
    future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>, replicas_per_token_range> query_partition_key_range_concurrent(clock_type::time_point timeout,
            std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results,
            lw_shared_ptr<query::read_command> cmd,
            db::consistency_level cl,
            dht::partition_range_vector::iterator&& i,
            dht::partition_range_vector&& ranges,
            int concurrency_factor,
            tracing::trace_state_ptr trace_state,
            uint32_t remaining_row_count,
            uint32_t remaining_partition_count,
            replicas_per_token_range preferred_replicas);

    future<coordinator_query_result> do_query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        coordinator_query_options optional_params);
    template<typename Range, typename CreateWriteHandler>
    future<std::vector<unique_response_handler>> mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler handler);
    template<typename Range>
    future<std::vector<unique_response_handler>> mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state);
    future<> mutate_begin(std::vector<unique_response_handler> ids, db::consistency_level cl, stdx::optional<clock_type::time_point> timeout_opt = { });
    future<> mutate_end(future<> mutate_result, utils::latency_counter, write_stats& stats, tracing::trace_state_ptr trace_state);
    future<> schedule_repair(std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state);
    bool need_throttle_writes() const;
    void unthrottle();
    void handle_read_error(std::exception_ptr eptr, bool range);
    template<typename Range>
    future<> mutate_internal(Range mutations, db::consistency_level cl, bool counter_write, tracing::trace_state_ptr tr_state, stdx::optional<clock_type::time_point> timeout_opt = { });
    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_nonsingular_mutations_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector&& pr, tracing::trace_state_ptr trace_state,
            uint64_t max_size, clock_type::time_point timeout);

    future<> mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state);
    future<> mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation m, db::consistency_level cl, clock_type::time_point timeout,
                                                    tracing::trace_state_ptr trace_state);

    gms::inet_address find_leader_for_counter_update(const mutation& m, db::consistency_level cl);

    future<> do_mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, bool);

    future<> send_to_endpoint(
            std::unique_ptr<mutation_holder> m,
            gms::inet_address target,
            std::vector<gms::inet_address> pending_endpoints,
            db::write_type type,
            write_stats& stats);

    db::view::update_backlog get_view_update_backlog() const;

    void maybe_update_view_backlog_of(gms::inet_address, stdx::optional<db::view::update_backlog>);

    db::view::update_backlog get_backlog_of(gms::inet_address) const;
public:
    storage_proxy(distributed<database>& db, config cfg, db::view::node_update_backlog& max_view_update_backlog);
    ~storage_proxy();
    const distributed<database>& get_db() const {
        return _db;
    }
    distributed<database>& get_db() {
        return _db;
    }

    response_id_type get_next_response_id() {
        auto next = _next_response_id++;
        if (next == 0) { // 0 is reserved for unique_response_handler
            next = _next_response_id++;
        }
        return next;
    }
    void init_messaging_service();

    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const mutation& m, clock_type::time_point timeout = clock_type::time_point::max());
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const schema_ptr&, const frozen_mutation& m, clock_type::time_point timeout = clock_type::time_point::max());
    // Applies mutations on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<mutation> mutation, clock_type::time_point timeout = clock_type::time_point::max());

    future<> mutate_streaming_mutation(const schema_ptr&, utils::UUID plan_id, const frozen_mutation& m, bool fragmented);

    dht::partition_range_vector get_restricted_ranges(const schema& s, dht::partition_range range);

    /**
    * Use this method to have these Mutations applied
    * across all replicas. This method will take care
    * of the possibility of a replica being down and hint
    * the data across to some other replica.
    *
    * @param mutations the mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    * @param tr_state trace state handle
    */
    future<> mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, bool raw_counters = false);

    future<> replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                           clock_type::time_point timeout);

    template<typename Range>
    future<> mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, clock_type::time_point timeout);

    future<> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                  bool should_mutate_atomically, tracing::trace_state_ptr tr_state, bool raw_counters = false);

    /**
    * See mutate. Adds additional steps before and after writing a batch.
    * Before writing the batch (but after doing availability check against the FD for the row replicas):
    *      write the entire batch to a batchlog elsewhere in the cluster.
    * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
    *
    * @param mutations the Mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    * @param tr_state trace state handle
    */
    future<> mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state);

    // Send a mutation to one specific remote target.
    // Inspired by Cassandra's StorageProxy.sendToHintedEndpoints but without
    // hinted handoff support, and just one target. See also
    // send_to_live_endpoints() - another take on the same original function.
    future<> send_to_endpoint(frozen_mutation_and_schema fm_a_s, gms::inet_address target, std::vector<gms::inet_address> pending_endpoints, db::write_type type, write_stats& stats);
    future<> send_to_endpoint(frozen_mutation_and_schema fm_a_s, gms::inet_address target, std::vector<gms::inet_address> pending_endpoints, db::write_type type);

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     */
    future<> truncate_blocking(sstring keyspace, sstring cfname);

    /*
     * Executes data query on the whole cluster.
     *
     * Partitions for each range will be ordered according to decorated_key ordering. Results for
     * each range from "partition_ranges" may appear in any order.
     *
     * Will consider the preferred_replicas provided by the caller when selecting the replicas to
     * send read requests to. However this is merely a hint and it is not guaranteed that the read
     * requests will be sent to all or any of the listed replicas. After the query is done the list
     * of replicas that served it is also returned.
     *
     * IMPORTANT: Not all fibers started by this method have to be done by the time it returns so no
     * parameter can be changed after being passed to this method.
     */
    future<coordinator_query_result> query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        coordinator_query_options optional_params);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_locally(
        schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range&,
        clock_type::time_point timeout,
        tracing::trace_state_ptr trace_state = nullptr,
        uint64_t max_size = query::result_memory_limiter::maximum_result_size);


    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_locally(
        schema_ptr, lw_shared_ptr<query::read_command> cmd, const ::compat::one_or_two_partition_ranges&,
        clock_type::time_point timeout,
        tracing::trace_state_ptr trace_state = nullptr,
        uint64_t max_size = query::result_memory_limiter::maximum_result_size);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector& pr,
            clock_type::time_point timeout,
            tracing::trace_state_ptr trace_state = nullptr,
            uint64_t max_size = query::result_memory_limiter::maximum_result_size);


    future<> stop();
    future<> stop_hints_manager();
    future<> start_hints_manager(shared_ptr<gms::gossiper> gossiper_ptr, shared_ptr<service::storage_service> ss_ptr);
    void allow_replaying_hints() noexcept;

    const stats& get_stats() const {
        return _stats;
    }

    friend class abstract_read_executor;
    friend class abstract_write_response_handler;
    friend class speculating_read_executor;
    friend class view_update_backlog_broker;
};

extern distributed<storage_proxy> _the_storage_proxy;

inline distributed<storage_proxy>& get_storage_proxy() {
    return _the_storage_proxy;
}

inline storage_proxy& get_local_storage_proxy() {
    return _the_storage_proxy.local();
}

inline shared_ptr<storage_proxy> get_local_shared_storage_proxy() {
    return _the_storage_proxy.local_shared();
}

dht::partition_range_vector get_restricted_ranges(locator::token_metadata&,
    const schema&, dht::partition_range);

}
