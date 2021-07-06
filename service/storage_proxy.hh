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

#include "database_fwd.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "inet_address_vectors.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/execution_stage.hh>
#include <seastar/core/scheduling_specific.hh>
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
#include "storage_proxy_stats.hh"
#include "cache_temperature.hh"
#include "service_permit.hh"
#include "service/client_state.hh"
#include "cdc/stats.hh"
#include "locator/token_metadata.hh"
#include "db/hints/host_filter.hh"
#include "utils/small_vector.hh"
#include "service/endpoint_lifecycle_subscriber.hh"

class reconcilable_result;
class frozen_mutation_and_schema;
class frozen_mutation;

namespace seastar::rpc {

template <typename... T>
class tuple;

}

namespace compat {

class one_or_two_partition_ranges;

}

namespace cdc {
    class cdc_service;    
}

namespace service {

namespace paxos {
    class prepare_summary;
    class proposal;
}

class abstract_write_response_handler;
class paxos_response_handler;
class abstract_read_executor;
class mutation_holder;
class view_update_write_response_handler;
struct hint_wrapper;

using replicas_per_token_range = std::unordered_map<dht::token_range, std::vector<utils::UUID>>;

struct query_partition_key_range_concurrent_result {
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> result;
    replicas_per_token_range replicas;
};

struct view_update_backlog_timestamped {
    db::view::update_backlog backlog;
    api::timestamp_type ts;
};

struct allow_hints_tag {};
using allow_hints = bool_class<allow_hints_tag>;


class query_ranges_to_vnodes_generator {
    schema_ptr _s;
    dht::partition_range_vector _ranges;
    dht::partition_range_vector::iterator _i; // iterator to current range in _ranges
    bool _local;
    const locator::token_metadata_ptr _tmptr;
    void process_one_range(size_t n, dht::partition_range_vector& ranges);
public:
    query_ranges_to_vnodes_generator(const locator::token_metadata_ptr tmptr, schema_ptr s, dht::partition_range_vector ranges, bool local = false);
    query_ranges_to_vnodes_generator(const query_ranges_to_vnodes_generator&) = delete;
    query_ranges_to_vnodes_generator(query_ranges_to_vnodes_generator&&) = default;
    // generate next 'n' vnodes, may return less than requested number of ranges
    // which means either that there are no more ranges
    // (in which case empty() == true), or too many ranges
    // are requested
    dht::partition_range_vector operator()(size_t n);
    bool empty() const;
};

struct storage_proxy_coordinator_query_result {
    foreign_ptr<lw_shared_ptr<query::result>> query_result;
    replicas_per_token_range last_replicas;
    db::read_repair_decision read_repair_decision;

    storage_proxy_coordinator_query_result(foreign_ptr<lw_shared_ptr<query::result>> query_result,
            replicas_per_token_range last_replicas = {},
            db::read_repair_decision read_repair_decision = db::read_repair_decision::NONE)
        : query_result(std::move(query_result))
        , last_replicas(std::move(last_replicas))
        , read_repair_decision(std::move(read_repair_decision)) {
    }
};

class cas_request;

class storage_proxy : public seastar::async_sharded_service<storage_proxy>, public peering_sharded_service<storage_proxy>, public service::endpoint_lifecycle_subscriber  {
public:
    enum class error : uint8_t {
        NONE,
        TIMEOUT,
        FAILURE,
    };
    using clock_type = lowres_clock;
    struct config {
        db::hints::host_filter hinted_handoff_enabled = {};
        db::hints::directory_initializer hints_directory_initializer;
        size_t available_memory;
        smp_service_group read_smp_service_group = default_smp_service_group();
        smp_service_group write_smp_service_group = default_smp_service_group();
        smp_service_group hints_write_smp_service_group = default_smp_service_group();
        // Write acknowledgments might not be received on the correct shard, and
        // they need a separate smp_service_group to prevent an ABBA deadlock
        // with writes.
        smp_service_group write_ack_smp_service_group = default_smp_service_group();
    };
private:

    using response_id_type = uint64_t;
    struct unique_response_handler {
        response_id_type id;
        storage_proxy& p;
        unique_response_handler(storage_proxy& p_, response_id_type id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x) noexcept;
        unique_response_handler& operator=(unique_response_handler&&) noexcept;
        ~unique_response_handler();
        response_id_type release();
    };
    using unique_response_handler_vector = utils::small_vector<unique_response_handler, 1>;
    using response_handlers_map = std::unordered_map<response_id_type, ::shared_ptr<abstract_write_response_handler>>;

public:
    static const sstring COORDINATOR_STATS_CATEGORY;
    static const sstring REPLICA_STATS_CATEGORY;

    using write_stats = storage_proxy_stats::write_stats;
    using stats = storage_proxy_stats::stats;
    using global_stats = storage_proxy_stats::global_stats;
    using cdc_stats = cdc::stats;

    class coordinator_query_options {
        clock_type::time_point _timeout;

    public:
        service_permit permit;
        client_state& cstate;
        tracing::trace_state_ptr trace_state = nullptr;
        replicas_per_token_range preferred_replicas;
        std::optional<db::read_repair_decision> read_repair_decision;

        coordinator_query_options(clock_type::time_point timeout,
                service_permit permit_,
                client_state& client_state_,
                tracing::trace_state_ptr trace_state = nullptr,
                replicas_per_token_range preferred_replicas = { },
                std::optional<db::read_repair_decision> read_repair_decision = { })
            : _timeout(timeout)
            , permit(std::move(permit_))
            , cstate(client_state_)
            , trace_state(std::move(trace_state))
            , preferred_replicas(std::move(preferred_replicas))
            , read_repair_decision(read_repair_decision) {
        }

        clock_type::time_point timeout(storage_proxy& sp) const {
            return _timeout;
        }
    };

    using coordinator_query_result = storage_proxy_coordinator_query_result;

    // Holds  a list of endpoints participating in CAS request, for a given
    // consistency level, token, and state of joining/leaving nodes.
    struct paxos_participants {
        inet_address_vector_replica_set endpoints;
        // How many participants are required for a quorum (i.e. is it SERIAL or LOCAL_SERIAL).
        size_t required_participants;
        bool has_dead_endpoints;
    };

    gms::feature_service& features() noexcept { return _features; }
    const gms::feature_service& features() const { return _features; }

    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept;

    query::max_result_size get_max_result_size(const query::partition_slice& slice) const;

private:
    distributed<database>& _db;
    const locator::shared_token_metadata& _shared_token_metadata;
    smp_service_group _read_smp_service_group;
    smp_service_group _write_smp_service_group;
    smp_service_group _hints_write_smp_service_group;
    smp_service_group _write_ack_smp_service_group;
    response_id_type _next_response_id;
    response_handlers_map _response_handlers;
    // This buffer hold ids of throttled writes in case resource consumption goes
    // below the threshold and we want to unthrottle some of them. Without this throttled
    // request with dead or slow replica may wait for up to timeout ms before replying
    // even if resource consumption will go to zero. Note that some requests here may
    // be already completed by the point they tried to be unthrottled (request completion does
    // not remove request from the buffer), but this is fine since request ids are unique, so we
    // just skip an entry if request no longer exists.
    circular_buffer<response_id_type> _throttled_writes;
    db::hints::resource_manager _hints_resource_manager;
    db::hints::manager _hints_manager;
    db::hints::directory_initializer _hints_directory_initializer;
    db::hints::manager _hints_for_views_manager;
    scheduling_group_key _stats_key;
    storage_proxy_stats::global_stats _global_stats;
    gms::feature_service& _features;
    netw::messaging_service& _messaging;
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
            service_permit,
            bool,
            lw_shared_ptr<cdc::operation_result_tracker>> _mutate_stage;
    db::view::node_update_backlog& _max_view_update_backlog;
    std::unordered_map<gms::inet_address, view_update_backlog_timestamped> _view_update_backlogs;

    //NOTICE(sarna): This opaque pointer is here just to avoid moving write handler class definitions from .cc to .hh. It's slow path.
    class view_update_handlers_list;
    std::unique_ptr<view_update_handlers_list> _view_update_handlers_list;

    /* This is a pointer to the shard-local part of the sharded cdc_service:
     * storage_proxy needs access to cdc_service to augument mutations.
     *
     * It is a pointer and not a reference since cdc_service must be initialized after storage_proxy,
     * because it uses storage_proxy to perform pre-image queries, for one thing.
     * Therefore, at the moment of initializing storage_proxy, we don't have access to cdc_service yet.
     *
     * Furthermore, storage_proxy must keep the service object alive while augmenting mutations
     * (storage_proxy is never deintialized, and even if it would be, it would be after deinitializing cdc_service).
     * Thus cdc_service inherits from enable_shared_from_this and storage_proxy code must remember to call
     * shared_from_this().
     *
     * Eventual deinitialization of cdc_service is enabled by cdc_service::stop setting this pointer to nullptr.
     */
    cdc::cdc_service* _cdc = nullptr;

    cdc_stats _cdc_stats;

    std::unordered_set<utils::UUID> _hint_queue_checkpoints;
private:
    future<coordinator_query_result> query_singular(lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl,
            coordinator_query_options optional_params);
    response_id_type register_response_handler(shared_ptr<abstract_write_response_handler>&& h);
    void remove_response_handler(response_id_type id);
    void remove_response_handler_entry(response_handlers_map::iterator entry);
    void got_response(response_id_type id, gms::inet_address from, std::optional<db::view::update_backlog> backlog);
    void got_failure_response(response_id_type id, gms::inet_address from, size_t count, std::optional<db::view::update_backlog> backlog, error err);
    future<> response_wait(response_id_type id, clock_type::time_point timeout);
    ::shared_ptr<abstract_write_response_handler>& get_write_response_handler(storage_proxy::response_id_type id);
    response_id_type create_write_response_handler_helper(schema_ptr s, const dht::token& token,
            std::unique_ptr<mutation_holder> mh, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state,
            service_permit permit);
    response_id_type create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change, tracing::trace_state_ptr tr_state, storage_proxy::write_stats& stats, service_permit permit);
    response_id_type create_write_response_handler(const mutation&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit);
    response_id_type create_write_response_handler(const hint_wrapper&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit);
    response_id_type create_write_response_handler(const std::unordered_map<gms::inet_address, std::optional<mutation>>&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit);
    response_id_type create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token>& proposal,
            db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit);
    response_id_type create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, dht::token, inet_address_vector_replica_set>& meta,
            db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit);
    void register_cdc_operation_result_tracker(const storage_proxy::unique_response_handler_vector& ids, lw_shared_ptr<cdc::operation_result_tracker> tracker);
    void send_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    template<typename Range>
    size_t hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets, db::write_type type, tracing::trace_state_ptr tr_state) noexcept;
    void hint_to_dead_endpoints(response_id_type, db::consistency_level);
    template<typename Range>
    bool cannot_hint(const Range& targets, db::write_type type) const;
    bool hints_enabled(db::write_type type) const noexcept;
    db::hints::manager& hints_manager_for(db::write_type type);
    inet_address_vector_replica_set get_live_endpoints(keyspace& ks, const dht::token& token) const;
    static void sort_endpoints_by_proximity(inet_address_vector_replica_set& eps);
    inet_address_vector_replica_set get_live_sorted_endpoints(keyspace& ks, const dht::token& token) const;
    db::read_repair_decision new_read_repair_decision(const schema& s);
    ::shared_ptr<abstract_read_executor> get_read_executor(lw_shared_ptr<query::read_command> cmd,
            schema_ptr schema,
            dht::partition_range pr,
            db::consistency_level cl,
            db::read_repair_decision repair_decision,
            tracing::trace_state_ptr trace_state,
            const inet_address_vector_replica_set& preferred_endpoints,
            bool& is_bounced_read,
            service_permit permit);
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_result_local(schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                                                           query::result_options opts,
                                                                           tracing::trace_state_ptr trace_state,
                                                                           clock_type::time_point timeout);
    future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>> query_result_local_digest(schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                                                                                   tracing::trace_state_ptr trace_state,
                                                                                                   clock_type::time_point timeout,
                                                                                                   query::digest_algorithm da);
    future<coordinator_query_result> query_partition_key_range(lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector partition_ranges,
            db::consistency_level cl,
            coordinator_query_options optional_params);
    static inet_address_vector_replica_set intersection(const inet_address_vector_replica_set& l1, const inet_address_vector_replica_set& l2);
    future<query_partition_key_range_concurrent_result> query_partition_key_range_concurrent(clock_type::time_point timeout,
            std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results,
            lw_shared_ptr<query::read_command> cmd,
            db::consistency_level cl,
            query_ranges_to_vnodes_generator&& ranges_to_vnodes,
            int concurrency_factor,
            tracing::trace_state_ptr trace_state,
            uint64_t remaining_row_count,
            uint32_t remaining_partition_count,
            replicas_per_token_range preferred_replicas,
            service_permit permit);

    future<coordinator_query_result> do_query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        coordinator_query_options optional_params);
    future<coordinator_query_result> do_query_with_paxos(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        coordinator_query_options optional_params);
    template<typename Range, typename CreateWriteHandler>
    future<unique_response_handler_vector> mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, service_permit permit, CreateWriteHandler handler);
    template<typename Range>
    future<unique_response_handler_vector> mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit);
    future<> mutate_begin(unique_response_handler_vector ids, db::consistency_level cl, tracing::trace_state_ptr trace_state, std::optional<clock_type::time_point> timeout_opt = { });
    future<> mutate_end(future<> mutate_result, utils::latency_counter, write_stats& stats, tracing::trace_state_ptr trace_state);
    future<> schedule_repair(std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state, service_permit permit);
    bool need_throttle_writes() const;
    void unthrottle();
    void handle_read_error(std::exception_ptr eptr, bool range);
    template<typename Range>
    future<> mutate_internal(Range mutations, db::consistency_level cl, bool counter_write, tracing::trace_state_ptr tr_state, service_permit permit, std::optional<clock_type::time_point> timeout_opt = { }, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker = { });
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_nonsingular_mutations_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector&& pr, tracing::trace_state_ptr trace_state,
            clock_type::time_point timeout);
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_nonsingular_data_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector&& pr, query::result_options opts,
            tracing::trace_state_ptr trace_state, clock_type::time_point timeout);

    future<> mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state, service_permit permit);
    future<> mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation m, db::consistency_level cl, clock_type::time_point timeout,
                                                    tracing::trace_state_ptr trace_state, service_permit permit);

    gms::inet_address find_leader_for_counter_update(const mutation& m, db::consistency_level cl);

    future<> do_mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, bool, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker);

    future<> send_to_endpoint(
            std::unique_ptr<mutation_holder> m,
            gms::inet_address target,
            inet_address_vector_topology_change pending_endpoints,
            db::write_type type,
            tracing::trace_state_ptr tr_state,
            write_stats& stats,
            allow_hints allow_hints = allow_hints::yes);

    db::view::update_backlog get_view_update_backlog() const;

    void maybe_update_view_backlog_of(gms::inet_address, std::optional<db::view::update_backlog>);

    db::view::update_backlog get_backlog_of(gms::inet_address) const;

    template<typename Range>
    future<> mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, service_permit permit, clock_type::time_point timeout);

    void retire_view_response_handlers(noncopyable_function<bool(const abstract_write_response_handler&)> filter_fun);

    future<> create_hint_queue_sync_point(utils::UUID sync_point_id, std::vector<gms::inet_address> endpoints, clock_type::time_point deadline);
    future<bool> check_hint_queue_sync_point(utils::UUID sync_point);
public:
    storage_proxy(distributed<database>& db, config cfg, db::view::node_update_backlog& max_view_update_backlog,
            scheduling_group_key stats_key, gms::feature_service& feat, const locator::shared_token_metadata& stm, netw::messaging_service& ms);
    ~storage_proxy();
    const distributed<database>& get_db() const {
        return _db;
    }
    distributed<database>& get_db() {
        return _db;
    }
    const database& local_db() const noexcept {
        return _db.local();
    }
    database& local_db() noexcept {
        return _db.local();
    }

    void set_cdc_service(cdc::cdc_service* cdc) {
        _cdc = cdc;
    }
    cdc::cdc_service* get_cdc_service() const {
        return _cdc;
    }

    view_update_handlers_list& get_view_update_handlers_list() {
        return *_view_update_handlers_list;
    }

    response_id_type get_next_response_id() {
        auto next = _next_response_id++;
        if (next == 0) { // 0 is reserved for unique_response_handler
            next = _next_response_id++;
        }
        return next;
    }
    void init_messaging_service(shared_ptr<migration_manager>);
    future<> uninit_messaging_service();

    // Waits until `source_endpoints` replay their current hints towards `target_endpoints`.
    future<> wait_for_hints_to_be_replayed(utils::UUID operation_id, std::vector<gms::inet_address> source_endpoints, std::vector<gms::inet_address> target_endpoints);

private:
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout, smp_service_group smp_grp);
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const schema_ptr&, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout,
            smp_service_group smp_grp);
    // Applies mutations on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<mutation> mutation, tracing::trace_state_ptr tr_state, clock_type::time_point timeout, smp_service_group smp_grp);

public:
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout = clock_type::time_point::max()) {
        return mutate_locally(m, tr_state, sync, timeout, _write_smp_service_group);
    }
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout = clock_type::time_point::max()) {
        return mutate_locally(s, m, tr_state, sync, timeout, _write_smp_service_group);
    }
    // Applies mutations on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<mutation> mutation, tracing::trace_state_ptr tr_state, clock_type::time_point timeout = clock_type::time_point::max());

    future<> mutate_hint(const schema_ptr&, const frozen_mutation& m, tracing::trace_state_ptr tr_state, clock_type::time_point timeout = clock_type::time_point::max());

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
    future<> mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, bool raw_counters = false);

    paxos_participants
    get_paxos_participants(const sstring& ks_name, const dht::token& token, db::consistency_level consistency_for_paxos);

    future<> replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                           clock_type::time_point timeout, service_permit permit);

    future<> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                  bool should_mutate_atomically, tracing::trace_state_ptr tr_state, service_permit permit, bool raw_counters = false);

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
    future<> mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit);

    future<> send_hint_to_all_replicas(frozen_mutation_and_schema fm_a_s);

    // Send a mutation to one specific remote target.
    // Inspired by Cassandra's StorageProxy.sendToHintedEndpoints but without
    // hinted handoff support, and just one target. See also
    // send_to_live_endpoints() - another take on the same original function.
    future<> send_to_endpoint(frozen_mutation_and_schema fm_a_s, gms::inet_address target, inet_address_vector_topology_change pending_endpoints, db::write_type type,
            tracing::trace_state_ptr tr_state, write_stats& stats, allow_hints allow_hints = allow_hints::yes);
    future<> send_to_endpoint(frozen_mutation_and_schema fm_a_s, gms::inet_address target, inet_address_vector_topology_change pending_endpoints, db::write_type type,
            tracing::trace_state_ptr tr_state, allow_hints allow_hints = allow_hints::yes);

    // Send a mutation to a specific remote target as a hint.
    // Unlike regular mutations during write operations, hints are sent on the streaming connection
    // and use different RPC verb.
    future<> send_hint_to_endpoint(frozen_mutation_and_schema fm_a_s, gms::inet_address target);

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

    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_mutations_locally(
        schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range&,
        clock_type::time_point timeout,
        tracing::trace_state_ptr trace_state = nullptr);


    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_mutations_locally(
        schema_ptr, lw_shared_ptr<query::read_command> cmd, const ::compat::one_or_two_partition_ranges&,
        clock_type::time_point timeout,
        tracing::trace_state_ptr trace_state = nullptr);

    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_mutations_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector& pr,
            clock_type::time_point timeout,
            tracing::trace_state_ptr trace_state = nullptr);

    future<bool> cas(schema_ptr schema, shared_ptr<cas_request> request, lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector partition_ranges, coordinator_query_options query_options,
            db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn,
            clock_type::time_point write_timeout, clock_type::time_point cas_timeout, bool write = true);

    future<> stop();
    future<> start_hints_manager(shared_ptr<gms::gossiper> gossiper_ptr);
    void allow_replaying_hints() noexcept;
    future<> drain_on_shutdown();

    future<> change_hints_host_filter(db::hints::host_filter new_filter);
    const db::hints::host_filter& get_hints_host_filter() const;

    const stats& get_stats() const {
        return scheduling_group_get_specific<storage_proxy_stats::stats>(_stats_key);
    }
    stats& get_stats() {
        return scheduling_group_get_specific<storage_proxy_stats::stats>(_stats_key);
    }
    const global_stats& get_global_stats() const {
        return _global_stats;
    }
    global_stats& get_global_stats() {
        return _global_stats;
    }
    const cdc_stats& get_cdc_stats() const {
        return _cdc_stats;
    }
    cdc_stats& get_cdc_stats() {
        return _cdc_stats;
    }

    scheduling_group_key get_stats_key() const {
        return _stats_key;
    }

    static unsigned cas_shard(const schema& s, dht::token token);

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;

    friend class abstract_read_executor;
    friend class abstract_write_response_handler;
    friend class speculating_read_executor;
    friend class view_update_backlog_broker;
    friend class view_update_write_response_handler;
    friend class paxos_response_handler;
    friend class mutation_holder;
    friend class per_destination_mutation;
    friend class shared_mutation;
    friend class hint_mutation;
    friend class cas_mutation;
};

// A Paxos (AKA Compare And Swap, CAS) protocol involves multiple roundtrips between the coordinator
// and endpoint participants. Some endpoints may be unavailable or slow, and this does not stop the
// protocol progress. paxos_response_handler stores the shared state of the storage proxy associated
// with all the futures associated with a Paxos protocol step (prepare, accept, learn), including
// those outstanding by the time the step ends.
//
class paxos_response_handler : public enable_shared_from_this<paxos_response_handler> {
private:
    shared_ptr<storage_proxy> _proxy;
    // The schema for the table the operation works upon.
    schema_ptr _schema;
    // Read command used by this CAS request.
    lw_shared_ptr<query::read_command> _cmd;
    // SERIAL or LOCAL SERIAL - influences what endpoints become Paxos protocol participants,
    // as well as Paxos quorum size. Is either set explicitly in the query or derived from
    // the value set by SERIAL CONSISTENCY [SERIAL|LOCAL SERIAL] control statement.
    db::consistency_level _cl_for_paxos;
    // QUORUM, LOCAL_QUORUM, etc - defines how many replicas to wait for in LEARN step.
    // Is either set explicitly or derived from the consistency level set in keyspace options.
    db::consistency_level _cl_for_learn;
    // Live endpoints, as per get_paxos_participants()
    inet_address_vector_replica_set _live_endpoints;
    // How many endpoints need to respond favourably for the protocol to progress to the next step.
    size_t _required_participants;
    // A deadline when the entire CAS operation timeout expires, derived from write_request_timeout_in_ms
    storage_proxy::clock_type::time_point _timeout;
    // A deadline when the CAS operation gives up due to contention, derived from cas_contention_timeout_in_ms
    storage_proxy::clock_type::time_point _cas_timeout;
    // The key this request is working on.
    dht::decorated_key _key;
    // service permit from admission control
    service_permit _permit;
    // how many replicas replied to learn
    uint64_t _learned = 0;

    // Unique request id generator.
    static thread_local uint64_t next_id;

    // Unique request id for logging purposes.
    const uint64_t _id = next_id++;

    // max pruning operations to run in parralel
    static constexpr uint16_t pruning_limit = 1000;

public:
    tracing::trace_state_ptr tr_state;

public:
    paxos_response_handler(shared_ptr<storage_proxy> proxy_arg, tracing::trace_state_ptr tr_state_arg,
        service_permit permit_arg,
        dht::decorated_key key_arg, schema_ptr schema_arg, lw_shared_ptr<query::read_command> cmd_arg,
        db::consistency_level cl_for_paxos_arg, db::consistency_level cl_for_learn_arg,
        storage_proxy::clock_type::time_point timeout_arg, storage_proxy::clock_type::time_point cas_timeout_arg)

        : _proxy(proxy_arg)
        , _schema(std::move(schema_arg))
        , _cmd(cmd_arg)
        , _cl_for_paxos(cl_for_paxos_arg)
        , _cl_for_learn(cl_for_learn_arg)
        , _timeout(timeout_arg)
        , _cas_timeout(cas_timeout_arg)
        , _key(std::move(key_arg))
        , _permit(std::move(permit_arg))
        , tr_state(tr_state_arg) {
        storage_proxy::paxos_participants pp = _proxy->get_paxos_participants(_schema->ks_name(), _key.token(), _cl_for_paxos);
        _live_endpoints = std::move(pp.endpoints);
        _required_participants = pp.required_participants;
        tracing::trace(tr_state, "Create paxos_response_handler for token {} with live: {} and required participants: {}",
                _key.token(), _live_endpoints, _required_participants);
        _proxy->get_stats().cas_foreground++;
        _proxy->get_stats().cas_total_running++;
        _proxy->get_stats().cas_total_operations++;
    }

    ~paxos_response_handler() {
        _proxy->get_stats().cas_total_running--;
    }

    // Result of PREPARE step, i.e. begin_and_repair_paxos().
    struct ballot_and_data {
        // Accepted ballot.
        utils::UUID ballot;
        // Current value of the requested key or none.
        foreign_ptr<lw_shared_ptr<query::result>> data;
    };

    // Steps of the Paxos protocol
    future<ballot_and_data> begin_and_repair_paxos(client_state& cs, unsigned& contentions, bool is_write);
    future<paxos::prepare_summary> prepare_ballot(utils::UUID ballot);
    future<bool> accept_proposal(lw_shared_ptr<paxos::proposal> proposal, bool timeout_if_partially_accepted = true);
    future<> learn_decision(lw_shared_ptr<paxos::proposal> proposal, bool allow_hints = false);
    void prune(utils::UUID ballot);
    uint64_t id() const {
        return _id;
    }
    size_t block_for() const {
        return _required_participants;
    }
    schema_ptr schema() const {
        return _schema;
    }
    const partition_key& key() const {
        return _key.key();
    }
    void set_cl_for_learn(db::consistency_level cl) {
        _cl_for_learn = cl;
    }
    // this is called with an id of a replica that replied to learn request
    // adn returns true when quorum of such requests are accumulated
    bool learned(gms::inet_address ep);
};

extern distributed<storage_proxy> _the_storage_proxy;

// DEPRECATED, DON'T USE!
// Pass references to services through constructor/function parameters. Don't use globals.
inline distributed<storage_proxy>& get_storage_proxy() {
    return _the_storage_proxy;
}

// DEPRECATED, DON'T USE!
// Pass references to services through constructor/function parameters. Don't use globals.
inline storage_proxy& get_local_storage_proxy() {
    return _the_storage_proxy.local();
}

// DEPRECATED, DON'T USE!
// Pass references to services through constructor/function parameters. Don't use globals.
inline shared_ptr<storage_proxy> get_local_shared_storage_proxy() {
    return _the_storage_proxy.local_shared();
}

}
