/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <variant>
#include "replica/database_fwd.hh"
#include "message/messaging_service_fwd.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/execution_stage.hh>
#include <seastar/core/scheduling_specific.hh>
#include "db/read_repair_decision.hh"
#include "db/write_type.hh"
#include "db/hints/manager.hh"
#include "db/view/node_view_update_backlog.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics.hh>
#include <seastar/rpc/rpc_types.hh>
#include "storage_proxy_stats.hh"
#include "service_permit.hh"
#include "query-result.hh"
#include "cdc/stats.hh"
#include "locator/abstract_replication_strategy.hh"
#include "db/hints/host_filter.hh"
#include "utils/phased_barrier.hh"
#include "utils/small_vector.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include <seastar/core/circular_buffer.hh>
#include "exceptions/coordinator_result.hh"
#include "replica/exceptions.hh"
#include "locator/host_id.hh"
#include "dht/token_range_endpoints.hh"

class reconcilable_result;
class frozen_mutation_and_schema;
class frozen_mutation;
class cache_temperature;
class query_ranges_to_vnodes_generator;

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

namespace gms {
class gossiper;
class feature_service;
}

namespace db {
class system_keyspace;
}

namespace service {

namespace paxos {
    class prepare_summary;
    class proposal;
    class promise;
    using prepare_response = std::variant<utils::UUID, promise>;
}

class abstract_write_response_handler;
class paxos_response_handler;
class abstract_read_executor;
class mutation_holder;
class client_state;
class migration_manager;
struct hint_wrapper;
struct read_repair_mutation;

using replicas_per_token_range = std::unordered_map<dht::token_range, std::vector<locator::host_id>>;

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

using is_cancellable = bool_class<struct cancellable_tag>;

using storage_proxy_clock_type = lowres_clock;

class storage_proxy_coordinator_query_options {
    storage_proxy_clock_type::time_point _timeout;

public:
    service_permit permit;
    client_state& cstate;
    tracing::trace_state_ptr trace_state = nullptr;
    replicas_per_token_range preferred_replicas;
    std::optional<db::read_repair_decision> read_repair_decision;

    storage_proxy_coordinator_query_options(storage_proxy_clock_type::time_point timeout,
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

    storage_proxy_clock_type::time_point timeout(storage_proxy& sp) const {
        return _timeout;
    }
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
        RATE_LIMIT,
    };
    template<typename T = void>
    using result = exceptions::coordinator_result<T>;
    using clock_type = storage_proxy_clock_type;
    struct config {
        db::hints::host_filter hinted_handoff_enabled = {};
        db::hints::directory_initializer hints_directory_initializer;
        size_t available_memory;
        smp_service_group read_smp_service_group = default_smp_service_group();
        smp_service_group write_smp_service_group = default_smp_service_group();
        smp_service_group write_mv_smp_service_group = default_smp_service_group();
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

    using coordinator_query_options = storage_proxy_coordinator_query_options;
    using coordinator_query_result = storage_proxy_coordinator_query_result;

    // Holds  a list of endpoints participating in CAS request, for a given
    // consistency level, token, and state of joining/leaving nodes.
    struct paxos_participants {
        inet_address_vector_replica_set endpoints;
        // How many participants are required for a quorum (i.e. is it SERIAL or LOCAL_SERIAL).
        size_t required_participants;
    };

    gms::feature_service& features() noexcept { return _features; }
    const gms::feature_service& features() const { return _features; }

    locator::effective_replication_map_factory& get_erm_factory() noexcept {
        return _erm_factory;
    }

    const locator::effective_replication_map_factory& get_erm_factory() const noexcept {
        return _erm_factory;
    }

    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept;

    query::max_result_size get_max_result_size(const query::partition_slice& slice) const;
    query::tombstone_limit get_tombstone_limit() const;
    inet_address_vector_replica_set get_live_endpoints(const locator::effective_replication_map& erm, const dht::token& token) const;

    void update_view_update_backlog();

    // Get information about this node's view update backlog. It combines information from all local shards.
    db::view::update_backlog get_view_update_backlog();

    // Used for gossiping view update backlog information. Must be called on shard 0.
    future<std::optional<db::view::update_backlog>> get_view_update_backlog_if_changed();

    // Get information about a remote node's view update backlog. Information about remote backlogs is constantly updated
    // using gossip and by passing the information in each MUTATION_DONE rpc call response.
    db::view::update_backlog get_backlog_of(gms::inet_address) const;

    future<std::vector<dht::token_range_endpoints>> describe_ring(const sstring& keyspace, bool include_only_local_dc = false) const;

private:
    distributed<replica::database>& _db;
    const locator::shared_token_metadata& _shared_token_metadata;
    locator::effective_replication_map_factory& _erm_factory;
    smp_service_group _read_smp_service_group;
    smp_service_group _write_smp_service_group;
    smp_service_group _write_mv_smp_service_group;
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

    class remote;
    std::unique_ptr<remote> _remote;

    static constexpr float CONCURRENT_SUBREQUESTS_MARGIN = 0.10;
    // for read repair chance calculation
    std::default_random_engine _urandom;
    seastar::metrics::metric_groups _metrics;
    uint64_t _background_write_throttle_threahsold;
    inheriting_concrete_execution_stage<
            future<result<>>,
            storage_proxy*,
            std::vector<mutation>,
            db::consistency_level,
            clock_type::time_point,
            tracing::trace_state_ptr,
            service_permit,
            bool,
            db::allow_per_partition_rate_limit,
            lw_shared_ptr<cdc::operation_result_tracker>> _mutate_stage;
    db::view::node_update_backlog& _max_view_update_backlog;
    std::unordered_map<gms::inet_address, view_update_backlog_timestamped> _view_update_backlogs;

    //NOTICE(sarna): This opaque pointer is here just to avoid moving write handler class definitions from .cc to .hh. It's slow path.
    class cancellable_write_handlers_list;
    std::unique_ptr<cancellable_write_handlers_list> _cancellable_write_handlers_list;

    /* This is a pointer to the shard-local part of the sharded cdc_service:
     * storage_proxy needs access to cdc_service to augment mutations.
     *
     * It is a pointer and not a reference since cdc_service must be initialized after storage_proxy,
     * because it uses storage_proxy to perform pre-image queries, for one thing.
     * Therefore, at the moment of initializing storage_proxy, we don't have access to cdc_service yet.
     *
     * Furthermore, storage_proxy must keep the service object alive while augmenting mutations
     * (storage_proxy is never deinitialized, and even if it would be, it would be after deinitializing cdc_service).
     * Thus cdc_service inherits from enable_shared_from_this and storage_proxy code must remember to call
     * shared_from_this().
     *
     * Eventual deinitialization of cdc_service is enabled by cdc_service::stop setting this pointer to nullptr.
     */
    cdc::cdc_service* _cdc = nullptr;

    cdc_stats _cdc_stats;

    // Needed by sstable cleanup fiber to wait for all ongoing writes to complete
    utils::phased_barrier _pending_writes_phaser;
private:
    future<result<coordinator_query_result>> query_singular(lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl,
            coordinator_query_options optional_params);
    response_id_type register_response_handler(shared_ptr<abstract_write_response_handler>&& h);
    void remove_response_handler(response_id_type id);
    void remove_response_handler_entry(response_handlers_map::iterator entry);
    void got_response(response_id_type id, gms::inet_address from, std::optional<db::view::update_backlog> backlog);
    void got_failure_response(response_id_type id, gms::inet_address from, size_t count, std::optional<db::view::update_backlog> backlog, error err, std::optional<sstring> msg);
    future<result<>> response_wait(response_id_type id, clock_type::time_point timeout);
    ::shared_ptr<abstract_write_response_handler>& get_write_response_handler(storage_proxy::response_id_type id);
    result<response_id_type> create_write_response_handler_helper(schema_ptr s, const dht::token& token,
            std::unique_ptr<mutation_holder> mh, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state,
            service_permit permit, db::allow_per_partition_rate_limit allow_limit, is_cancellable);
    result<response_id_type> create_write_response_handler(locator::effective_replication_map_ptr ermp, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change, tracing::trace_state_ptr tr_state, storage_proxy::write_stats& stats, service_permit permit, db::per_partition_rate_limit::info rate_limit_info, is_cancellable);
    result<response_id_type> create_write_response_handler(const mutation&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit);
    result<response_id_type> create_write_response_handler(const hint_wrapper&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit);
    result<response_id_type> create_write_response_handler(const read_repair_mutation&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit);
    result<response_id_type> create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token>& proposal,
            db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit);
    result<response_id_type> create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token, inet_address_vector_replica_set>& meta,
            db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit);
    void register_cdc_operation_result_tracker(const storage_proxy::unique_response_handler_vector& ids, lw_shared_ptr<cdc::operation_result_tracker> tracker);
    template<typename Range>
    bool should_reject_due_to_view_backlog(const Range& targets, const schema_ptr& s) const;
    void send_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    template<typename Range>
    size_t hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets,
            locator::effective_replication_map_ptr ermptr, db::write_type type, tracing::trace_state_ptr tr_state) noexcept;
    void hint_to_dead_endpoints(response_id_type, db::consistency_level);
    template<typename Range>
    bool cannot_hint(const Range& targets, db::write_type type) const;
    bool hints_enabled(db::write_type type) const noexcept;
    db::hints::manager& hints_manager_for(db::write_type type);
    void sort_endpoints_by_proximity(const locator::topology& topo, inet_address_vector_replica_set& eps) const;
    inet_address_vector_replica_set get_endpoints_for_reading(const sstring& ks_name, const locator::effective_replication_map& erm, const dht::token& token) const;
    inet_address_vector_replica_set filter_replicas_for_read(db::consistency_level, const locator::effective_replication_map&, inet_address_vector_replica_set live_endpoints, const inet_address_vector_replica_set& preferred_endpoints, db::read_repair_decision, std::optional<gms::inet_address>* extra, replica::column_family*) const;
    // As above with read_repair_decision=NONE, extra=nullptr.
    inet_address_vector_replica_set filter_replicas_for_read(db::consistency_level, const locator::effective_replication_map&, const inet_address_vector_replica_set& live_endpoints, const inet_address_vector_replica_set& preferred_endpoints, replica::column_family*) const;
    bool is_alive(const gms::inet_address&) const;
    result<::shared_ptr<abstract_read_executor>> get_read_executor(lw_shared_ptr<query::read_command> cmd,
            locator::effective_replication_map_ptr ermp,
            schema_ptr schema,
            dht::partition_range pr,
            db::consistency_level cl,
            db::read_repair_decision repair_decision,
            tracing::trace_state_ptr trace_state,
            const inet_address_vector_replica_set& preferred_endpoints,
            bool& is_bounced_read,
            service_permit permit);
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_result_local(
            locator::effective_replication_map_ptr,
            schema_ptr,
            lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
            query::result_options opts,
            tracing::trace_state_ptr trace_state,
            clock_type::time_point timeout,
            db::per_partition_rate_limit::info rate_limit_info);
    future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature, std::optional<full_position>>> query_result_local_digest(
            locator::effective_replication_map_ptr,
            schema_ptr,
            lw_shared_ptr<query::read_command> cmd,
            const dht::partition_range& pr,
            tracing::trace_state_ptr trace_state,
            clock_type::time_point timeout,
            query::digest_algorithm da,
            db::per_partition_rate_limit::info rate_limit_info);
    future<result<coordinator_query_result>> query_partition_key_range(lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector partition_ranges,
            db::consistency_level cl,
            coordinator_query_options optional_params);
    static inet_address_vector_replica_set intersection(const inet_address_vector_replica_set& l1, const inet_address_vector_replica_set& l2);
    future<result<query_partition_key_range_concurrent_result>> query_partition_key_range_concurrent(clock_type::time_point timeout,
            locator::effective_replication_map_ptr erm,
            lw_shared_ptr<query::read_command> cmd,
            db::consistency_level cl,
            query_ranges_to_vnodes_generator ranges_to_vnodes,
            int concurrency_factor,
            tracing::trace_state_ptr trace_state,
            uint64_t remaining_row_count,
            uint32_t remaining_partition_count,
            replicas_per_token_range preferred_replicas,
            service_permit permit);

    future<result<coordinator_query_result>> do_query(schema_ptr,
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
    future<result<unique_response_handler_vector>> mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, service_permit permit, CreateWriteHandler handler);
    template<typename Range>
    future<result<unique_response_handler_vector>> mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit);
    future<result<>> mutate_begin(unique_response_handler_vector ids, db::consistency_level cl, tracing::trace_state_ptr trace_state, std::optional<clock_type::time_point> timeout_opt = { });
    future<result<>> mutate_end(future<result<>> mutate_result, utils::latency_counter, write_stats& stats, tracing::trace_state_ptr trace_state);
    future<result<>> schedule_repair(locator::effective_replication_map_ptr ermp, std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state, service_permit permit);
    bool need_throttle_writes() const;
    void unthrottle();
    void handle_read_error(std::variant<exceptions::coordinator_exception_container, std::exception_ptr> failure, bool range);
    template<typename Range>
    future<result<>> mutate_internal(Range mutations, db::consistency_level cl, bool counter_write, tracing::trace_state_ptr tr_state, service_permit permit, std::optional<clock_type::time_point> timeout_opt = { }, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker = { }, db::allow_per_partition_rate_limit allow_limit = db::allow_per_partition_rate_limit::no);
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

    gms::inet_address find_leader_for_counter_update(const mutation& m, const locator::effective_replication_map& erm, db::consistency_level cl);

    future<result<>> do_mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, bool, db::allow_per_partition_rate_limit allow_limit, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker);

    future<> send_to_endpoint(
            std::unique_ptr<mutation_holder> m,
            locator::effective_replication_map_ptr ermp,
            gms::inet_address target,
            inet_address_vector_topology_change pending_endpoints,
            db::write_type type,
            tracing::trace_state_ptr tr_state,
            write_stats& stats,
            allow_hints,
            is_cancellable);

    void maybe_update_view_backlog_of(gms::inet_address, std::optional<db::view::update_backlog>);

    template<typename Range>
    future<> mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, service_permit permit, clock_type::time_point timeout);

    // Retires (times out) write response handlers which were constructed as `cancellable` and pass the given filter.
    void cancel_write_handlers(noncopyable_function<bool(const abstract_write_response_handler&)> filter_fun);

    /**
     * Returns whether for a range query doing a query against merged is likely
     * to be faster than 2 sequential queries, one against l1 followed by one
     * against l2.
     */
    bool is_worth_merging_for_range_query(
        const locator::topology& topo,
        inet_address_vector_replica_set& merged,
        inet_address_vector_replica_set& l1,
        inet_address_vector_replica_set& l2) const;

public:
    storage_proxy(distributed<replica::database>& db, config cfg, db::view::node_update_backlog& max_view_update_backlog,
            scheduling_group_key stats_key, gms::feature_service& feat, const locator::shared_token_metadata& stm, locator::effective_replication_map_factory& erm_factory);
    ~storage_proxy();

    const distributed<replica::database>& get_db() const {
        return _db;
    }
    distributed<replica::database>& get_db() {
        return _db;
    }
    const replica::database& local_db() const noexcept {
        return _db.local();
    }

    const data_dictionary::database data_dictionary() const;

    replica::database& local_db() noexcept {
        return _db.local();
    }

    void set_cdc_service(cdc::cdc_service* cdc) {
        _cdc = cdc;
    }
    cdc::cdc_service* get_cdc_service() const {
        return _cdc;
    }

    response_id_type get_next_response_id() {
        auto next = _next_response_id++;
        if (next == 0) { // 0 is reserved for unique_response_handler
            next = _next_response_id++;
        }
        return next;
    }

    // Start/stop the remote part of `storage_proxy` that is required for performing distributed queries.
    void start_remote(netw::messaging_service&, gms::gossiper&, migration_manager&, sharded<db::system_keyspace>& sys_ks);
    future<> stop_remote();

    gms::inet_address my_address() const noexcept;

    bool is_me(gms::inet_address addr) const noexcept;

private:
    bool only_me(const inet_address_vector_replica_set& replicas) const noexcept;

    // Throws an error if remote is not initialized.
    const struct remote& remote() const;
    struct remote& remote();

    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout, smp_service_group smp_grp, db::per_partition_rate_limit::info rate_limit_info);
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const schema_ptr&, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout,
            smp_service_group smp_grp, db::per_partition_rate_limit::info rate_limit_info);
    // Applies mutations on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<mutation> mutation, tracing::trace_state_ptr tr_state, clock_type::time_point timeout, smp_service_group smp_grp, db::per_partition_rate_limit::info rate_limit_info);
    // Confirm whether the topology version from the token is greater than or equal
    // to the current fencing_version sourced from shared_token_metadata.
    // If it is not, the function will return an engaged optional.
    std::optional<replica::stale_topology_exception> apply_fence(fencing_token token,
        gms::inet_address caller_address) const noexcept;
    // Do the same when the future is resolved without exception.
    template <typename T>
    future<T> apply_fence(future<T> future, fencing_token fence, gms::inet_address caller_address) const;
    // Returns fencing_token based on effective_replication_map.
    static fencing_token get_fence(const locator::effective_replication_map& erm);

    utils::phased_barrier::operation start_write() {
        return _pending_writes_phaser.start();
    }

    mutation do_get_batchlog_mutation_for(schema_ptr schema, const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now);
    future<> drain_on_shutdown();
public:
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout = clock_type::time_point::max(), db::per_partition_rate_limit::info rate_limit_info = std::monostate()) {
        return mutate_locally(m, tr_state, sync, timeout, _write_smp_service_group, rate_limit_info);
    }
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout = clock_type::time_point::max(), db::per_partition_rate_limit::info rate_limit_info = std::monostate()) {
        return mutate_locally(s, m, tr_state, sync, timeout, _write_smp_service_group, rate_limit_info);
    }
    // Applies materialized view mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_mv_locally(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout = clock_type::time_point::max(), db::per_partition_rate_limit::info rate_limit_info = std::monostate()) {
        return mutate_locally(s, m, tr_state, sync, timeout, _write_mv_smp_service_group, rate_limit_info);
    }
    // Applies mutations on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<mutation> mutation, tracing::trace_state_ptr tr_state, clock_type::time_point timeout = clock_type::time_point::max(), db::per_partition_rate_limit::info rate_limit_info = std::monostate());
    // Applies a vector of frozen_mutation:s and their schemas on this node, in parallel.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<frozen_mutation_and_schema> mutations, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout = clock_type::time_point::max(), db::per_partition_rate_limit::info rate_limit_info = std::monostate());

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
    future<> mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters = false);

    /**
    * See mutate. Does the same, but returns some exceptions
    * through the result<>, which allows for efficient inspection
    * of the exception on the exception handling path.
    */
    future<result<>> mutate_result(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters = false);

    paxos_participants
    get_paxos_participants(const sstring& ks_name, const locator::effective_replication_map& erm, const dht::token& token, db::consistency_level consistency_for_paxos);

    future<> replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                           clock_type::time_point timeout, service_permit permit);

    future<result<>> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                          bool should_mutate_atomically, tracing::trace_state_ptr tr_state, service_permit permit,
                                          db::allow_per_partition_rate_limit allow_limit, bool raw_counters = false);

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

    /**
    * See mutate_atomically. Does the same, but returns some exceptions
    * through the result<>, which allows for efficient inspection
    * of the exception on the exception handling path.
    */
    future<result<>> mutate_atomically_result(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit);

    future<> send_hint_to_all_replicas(frozen_mutation_and_schema fm_a_s);

    // Send a mutation to one specific remote target.
    // Inspired by Cassandra's StorageProxy.sendToHintedEndpoints but without
    // hinted handoff support, and just one target. See also
    // send_to_live_endpoints() - another take on the same original function.
    future<> send_to_endpoint(frozen_mutation_and_schema fm_a_s, locator::effective_replication_map_ptr ermp, gms::inet_address target, inet_address_vector_topology_change pending_endpoints, db::write_type type,
            tracing::trace_state_ptr tr_state, write_stats& stats, allow_hints, is_cancellable);
    future<> send_to_endpoint(frozen_mutation_and_schema fm_a_s, locator::effective_replication_map_ptr ermp, gms::inet_address target, inet_address_vector_topology_change pending_endpoints, db::write_type type,
            tracing::trace_state_ptr tr_state, allow_hints, is_cancellable);

    // Send a mutation to a specific remote target as a hint.
    // Unlike regular mutations during write operations, hints are sent on the streaming connection
    // and use different RPC verb.
    future<> send_hint_to_endpoint(frozen_mutation_and_schema fm_a_s, locator::effective_replication_map_ptr ermp, gms::inet_address target);

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @param timeout (default: use truncate_request_timeout_in_ms config)
     */
    future<> truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms = std::nullopt);

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

    /*
     * Like query(), but is allowed to return some exceptions as a result.
     * The caller must remember to handle them properly.
     */
    future<result<coordinator_query_result>> query_result(schema_ptr,
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

    future<bool> cas(schema_ptr schema, shared_ptr<cas_request> request, lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector partition_ranges, coordinator_query_options query_options,
            db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn,
            clock_type::time_point write_timeout, clock_type::time_point cas_timeout, bool write = true);

    mutation get_batchlog_mutation_for(const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now);

    future<> stop();
    future<> start_hints_manager();
    void allow_replaying_hints() noexcept;
    future<> abort_view_writes();

    future<> change_hints_host_filter(db::hints::host_filter new_filter);
    const db::hints::host_filter& get_hints_host_filter() const;

    future<db::hints::sync_point> create_hint_sync_point(std::vector<gms::inet_address> target_hosts) const;
    future<> wait_for_hint_sync_point(const db::hints::sync_point spoint, clock_type::time_point deadline);

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

    future<> await_pending_writes() noexcept {
        return _pending_writes_phaser.advance_and_await();
    }

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& hid) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;

    friend class abstract_read_executor;
    friend class abstract_write_response_handler;
    friend class speculating_read_executor;
    friend class view_update_backlog_broker;
    friend class paxos_response_handler;
    friend class mutation_holder;
    friend class per_destination_mutation;
    friend class shared_mutation;
    friend class hint_mutation;
    friend class cas_mutation;
};

}
