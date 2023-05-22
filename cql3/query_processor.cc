/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/query_processor.hh"

#include <seastar/core/metrics.hh>

#include "lang/wasm_alien_thread_runner.hh"
#include "lang/wasm_instance_cache.hh"
#include "service/storage_proxy.hh"
#include "service/forward_service.hh"
#include "service/raft/raft_group0_client.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/util.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/query_backend.hh"
#include "db/config.hh"
#include "data_dictionary/data_dictionary.hh"
#include "utils/hashers.hh"
#include "utils/error_injection.hh"

namespace cql3 {

using namespace statements;
using namespace cql_transport::messages;

logging::logger log("query_processor");
logging::logger prep_cache_log("prepared_statements_cache");
logging::logger authorized_prepared_statements_cache_log("authorized_prepared_statements_cache");

class query_backend::impl {
    std::string _name;
    data_dictionary::database _db;
    service::forward_service* _fwd_service;
    service::migration_manager* _mm;
    service::raft_group0_client* _rgc;
    wasmtime::Engine* _wasm_engine;
    wasm::instance_cache* _wasm_ic;
    wasm::alien_thread_runner* _wasm_atr;
    locator::token_metadata_ptr _token_metadata;
    service::storage_proxy& _proxy;

    void check(const void* ptr, const char* func) {
        if (!ptr) {
            throw std::runtime_error(fmt::format("bad function call: query backend {} does not support calling {}()", _name, func));
        }
    }
public:
    impl(std::string name, data_dictionary::database db, service::forward_service* fwd_service, service::migration_manager* mm, service::raft_group0_client* rgc,
            wasmtime::Engine* wasm_engine, wasm::instance_cache* wasm_ic, wasm::alien_thread_runner* wasm_atr, locator::token_metadata_ptr token_metadata, service::storage_proxy& proxy)
        : _name(std::move(name))
        , _db(db)
        , _fwd_service(fwd_service)
        , _mm(mm)
        , _rgc(rgc)
        , _wasm_engine(wasm_engine)
        , _wasm_ic(wasm_ic)
        , _wasm_atr(wasm_atr)
        , _token_metadata(std::move(token_metadata))
        , _proxy(proxy)
    { }

    data_dictionary::database db() {
        return _db;
    }
    service::forward_service& forwarder() {
        check(_fwd_service, __FUNCTION__);
        return *_fwd_service;
    }
    service::migration_manager& get_migration_manager() {
        check(_mm, __FUNCTION__);
        return *_mm;
    }
    service::raft_group0_client& get_group0_client() {
        check(_rgc, __FUNCTION__);
        return *_rgc;
    }
    wasmtime::Engine& wasm_engine() {
        check(_wasm_engine, __FUNCTION__);
        return *_wasm_engine;
    }
    wasm::instance_cache& wasm_instance_cache() {
        check(_wasm_ic, __FUNCTION__);
        return *_wasm_ic;
    }
    wasm::alien_thread_runner& alien_runner() {
        check(_wasm_atr, __FUNCTION__);
        return *_wasm_atr;
    }
    locator::token_metadata_ptr get_token_metadata_ptr() {
        check(_token_metadata.get(), __FUNCTION__);
        return _token_metadata;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    virtual shared_ptr<cql_transport::messages::result_message> bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls) = 0;
    virtual future<> truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms) = 0;
    virtual future<result<coordinator_query_result>> query_result(schema_ptr, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl, coordinator_query_options optional_params) = 0;
    virtual future<coordinator_query_result> query(schema_ptr, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl, coordinator_query_options optional_params) = 0;
    virtual future<> mutate(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit,
            db::allow_per_partition_rate_limit allow_limit, bool raw_counters = false) = 0;
    virtual future<result<>> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, bool should_mutate_atomically,
            tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters = false) = 0;
    virtual future<bool> cas(schema_ptr schema, shared_ptr<service::cas_request> request, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges, coordinator_query_options query_options,
            db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn, lowres_clock::time_point write_timeout, lowres_clock::time_point cas_timeout, bool write = true) = 0;
    virtual future<std::vector<dht::token_range_endpoints>> describe_ring(const sstring& keyspace, bool include_only_local_dc = false) const = 0;

    virtual query::tombstone_limit get_tombstone_limit() const = 0;
    virtual query::max_result_size get_max_result_size(const query::partition_slice& slice) const = 0;
};

query_backend::query_backend(shared_ptr<impl> impl) : _impl(std::move(impl)) { }
query_backend::~query_backend() = default;

data_dictionary::database query_backend::db() { return _impl->db(); }
service::forward_service& query_backend::forwarder() { return _impl->forwarder(); }
service::migration_manager& query_backend::get_migration_manager() { return _impl->get_migration_manager(); }
service::raft_group0_client& query_backend::get_group0_client() { return _impl->get_group0_client(); }
wasmtime::Engine& query_backend::wasm_engine() { return _impl->wasm_engine(); }
wasm::instance_cache& query_backend::wasm_instance_cache() { return _impl->wasm_instance_cache(); }
wasm::alien_thread_runner& query_backend::alien_runner() { return _impl->alien_runner(); }
locator::token_metadata_ptr query_backend::get_token_metadata_ptr() { return _impl->get_token_metadata_ptr(); }
service::storage_proxy& query_backend::proxy() { return _impl->proxy(); }

shared_ptr<cql_transport::messages::result_message> query_backend::bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls) {
    return _impl->bounce_to_shard(shard, std::move(cached_fn_calls));
}
future<> query_backend::truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms) {
    return _impl->truncate_blocking(std::move(keyspace), std::move(cfname), std::move(timeout_in_ms));
}
future<exceptions::coordinator_result<coordinator_query_result>> query_backend::query_result(schema_ptr s, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl, coordinator_query_options optional_params) {
    return _impl->query_result(std::move(s), std::move(cmd), std::move(partition_ranges), cl, optional_params);
}
future<coordinator_query_result>
query_backend::query(schema_ptr s, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl, coordinator_query_options optional_params) {
    return _impl->query(std::move(s), std::move(cmd), std::move(partition_ranges), cl, optional_params);
}
future<> query_backend::mutate(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit,
        db::allow_per_partition_rate_limit allow_limit, bool raw_counters) {
    return _impl->mutate(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), allow_limit, raw_counters);
}
future<exceptions::coordinator_result<>>
query_backend::mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, bool should_mutate_atomically,
        tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters) {
    return _impl->mutate_with_triggers(std::move(mutations), cl, timeout, should_mutate_atomically, std::move(tr_state), std::move(permit), allow_limit, raw_counters);
}
future<bool> query_backend::cas(schema_ptr schema, shared_ptr<service::cas_request> request, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges, coordinator_query_options query_options,
        db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn, lowres_clock::time_point write_timeout, lowres_clock::time_point cas_timeout, bool write) {
    return _impl->cas(std::move(schema), std::move(request), std::move(cmd), std::move(partition_ranges), query_options, cl_for_paxos, cl_for_learn, write_timeout, cas_timeout, write);
}
future<std::vector<dht::token_range_endpoints>> query_backend::describe_ring(const sstring& keyspace, bool include_only_local_dc) const {
    return _impl->describe_ring(keyspace, include_only_local_dc);
}

query::tombstone_limit query_backend::get_tombstone_limit() const {
    return _impl->get_tombstone_limit();
}
query::max_result_size query_backend::get_max_result_size(const query::partition_slice& slice) const {
    return _impl->get_max_result_size(slice);
}

class storage_proxy_query_backend : public query_backend::impl {
    shared_ptr<service::storage_proxy> _proxy;

public:
    storage_proxy_query_backend(
            shared_ptr<service::storage_proxy> proxy,
            data_dictionary::database db,
            service::forward_service* fwd_service,
            service::migration_manager* mm,
            service::raft_group0_client* rgc,
            wasmtime::Engine* wasm_engine,
            wasm::instance_cache* wasm_ic,
            wasm::alien_thread_runner* wasm_atr)
        : impl("storage_proxy_query_backend", db, fwd_service, mm, rgc, wasm_engine, wasm_ic, wasm_atr, proxy->get_token_metadata_ptr(), *proxy)
        , _proxy(std::move(proxy))
    { }
    storage_proxy_query_backend(
            shared_ptr<service::storage_proxy> proxy,
            data_dictionary::database db,
            service::forward_service& fwd_service,
            service::migration_manager& mm,
            service::raft_group0_client& rgc,
            wasmtime::Engine& wasm_engine,
            wasm::instance_cache& wasm_ic,
            wasm::alien_thread_runner& wasm_atr)
        : storage_proxy_query_backend(std::move(proxy), db, &fwd_service, &mm, &rgc, &wasm_engine, &wasm_ic, &wasm_atr)
    { }

    virtual shared_ptr<cql_transport::messages::result_message> bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls) override {
        _proxy->get_stats().replica_cross_shard_ops++;
        return ::make_shared<cql_transport::messages::result_message::bounce_to_shard>(shard, std::move(cached_fn_calls));
    }
    virtual future<> truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms) override {
        return _proxy->truncate_blocking(std::move(keyspace), std::move(cfname), timeout_in_ms);
    }
    virtual future<exceptions::coordinator_result<coordinator_query_result>>
    query_result(schema_ptr s, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl, coordinator_query_options optional_params) override {
        return _proxy->query_result(std::move(s), std::move(cmd), std::move(partition_ranges), cl, optional_params);
    }
    virtual future<coordinator_query_result>
    query(schema_ptr s, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl, coordinator_query_options optional_params) override {
        return _proxy->query(std::move(s), std::move(cmd), std::move(partition_ranges), cl, optional_params);
    }
    virtual future<> mutate(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit,
            db::allow_per_partition_rate_limit allow_limit, bool raw_counters) override {
        return _proxy->mutate(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), allow_limit, raw_counters);
    }
    virtual future<exceptions::coordinator_result<>>
    mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, bool should_mutate_atomically,
            tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters) override {
        return _proxy->mutate_with_triggers(std::move(mutations), cl, timeout, should_mutate_atomically, std::move(tr_state), std::move(permit), allow_limit, raw_counters);
    }
    virtual future<bool> cas(schema_ptr schema, shared_ptr<service::cas_request> request, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges, coordinator_query_options query_options,
            db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn, lowres_clock::time_point write_timeout, lowres_clock::time_point cas_timeout, bool write) override {
        return _proxy->cas(std::move(schema), std::move(request), std::move(cmd), std::move(partition_ranges), query_options, cl_for_paxos, cl_for_learn, write_timeout, cas_timeout, write);
    }
    virtual future<std::vector<dht::token_range_endpoints>> describe_ring(const sstring& keyspace, bool include_only_local_dc) const override {
        return _proxy->describe_ring(keyspace, include_only_local_dc);
    }

    virtual query::tombstone_limit get_tombstone_limit() const override {
        return _proxy->get_tombstone_limit();
    }
    virtual query::max_result_size get_max_result_size(const query::partition_slice& slice) const override {
        return _proxy->get_max_result_size(slice);
    }
};

query_backend make_storage_proxy_query_backend(
        service::storage_proxy& proxy,
        data_dictionary::database db,
        service::forward_service* fwd_service,
        service::migration_manager* mm,
        service::raft_group0_client* rgc,
        wasmtime::Engine* wasm_engine,
        wasm::instance_cache* wasm_ic,
        wasm::alien_thread_runner* wasm_atr) {
    return query_backend(make_shared<storage_proxy_query_backend>(proxy.shared_from_this(), db, fwd_service, mm, rgc, wasm_engine, wasm_ic, wasm_atr));
}

const sstring query_processor::CQL_VERSION = "3.3.1";

const std::chrono::minutes prepared_statements_cache::entry_expiry = std::chrono::minutes(60);

class query_processor::internal_state {
    service::query_state _qs;
public:
    internal_state() : _qs(service::client_state::for_internal_calls(), empty_service_permit()) {
    }
    operator service::query_state&() {
        return _qs;
    }
    operator const service::query_state&() const {
        return _qs;
    }
    operator service::client_state&() {
        return _qs.get_client_state();
    }
    operator const service::client_state&() const {
        return _qs.get_client_state();
    }
};

query_processor::query_processor(service::storage_proxy& proxy, service::forward_service& forwarder, data_dictionary::database db, service::migration_notifier& mn, service::migration_manager& mm, query_processor::memory_config mcfg, cql_config& cql_cfg, utils::loading_cache_config auth_prep_cache_cfg, service::raft_group0_client& group0_client, std::optional<wasm::startup_context> wasm_ctx)
        : _migration_subscriber{std::make_unique<migration_subscriber>(this)}
        , _proxy(proxy)
        , _forwarder(forwarder)
        , _db(db)
        , _mnotifier(mn)
        , _mm(mm)
        , _mcfg(mcfg)
        , _cql_config(cql_cfg)
        , _group0_client(group0_client)
        , _internal_state(new internal_state())
        , _prepared_cache(prep_cache_log, _mcfg.prepared_statment_cache_size)
        , _authorized_prepared_cache(std::move(auth_prep_cache_cfg), authorized_prepared_statements_cache_log)
        , _auth_prepared_cache_cfg_cb([this] (uint32_t) { (void) _authorized_prepared_cache_config_action.trigger_later(); })
        , _authorized_prepared_cache_config_action([this] { update_authorized_prepared_cache_config(); return make_ready_future<>(); })
        , _authorized_prepared_cache_update_interval_in_ms_observer(_db.get_config().permissions_update_interval_in_ms.observe(_auth_prepared_cache_cfg_cb))
        , _authorized_prepared_cache_validity_in_ms_observer(_db.get_config().permissions_validity_in_ms.observe(_auth_prepared_cache_cfg_cb))
        , _wasm_engine(wasm_ctx ? std::move(wasm_ctx->engine) : nullptr)
        , _wasm_instance_cache(wasm_ctx ? std::make_optional<wasm::instance_cache>(wasm_ctx->cache_size, wasm_ctx->instance_size, wasm_ctx->timer_period) : std::nullopt)
        , _alien_runner(wasm_ctx ? std::move(wasm_ctx->alien_runner) : nullptr)
        , _backend(make_shared<storage_proxy_query_backend>(_proxy.shared_from_this(), _db, _forwarder, _mm, _group0_client, **_wasm_engine, *_wasm_instance_cache, *_alien_runner))
        {
    namespace sm = seastar::metrics;
    namespace stm = statements;
    using clevel = db::consistency_level;
    sm::label cl_label("consistency_level");

    sm::label who_label("who");  // Who queried system tables
    const auto user_who_label_instance = who_label("user");
    const auto internal_who_label_instance = who_label("internal");

    sm::label ks_label("ks");
    const auto system_ks_label_instance = ks_label("system");

    std::vector<sm::metric_definition> qp_group;
    qp_group.push_back(sm::make_counter(
        "statements_prepared",
        _stats.prepare_invocations,
        sm::description("Counts the total number of parsed CQL requests.")));
    for (auto cl = size_t(clevel::MIN_VALUE); cl <= size_t(clevel::MAX_VALUE); ++cl) {
        qp_group.push_back(
            sm::make_counter(
                "queries",
                _stats.queries_by_cl[cl],
                sm::description("Counts queries by consistency level."),
                {cl_label(clevel(cl))}));
    }
    _metrics.add_group("query_processor", qp_group);

    sm::label cas_label("conditional");
    auto cas_label_instance = cas_label("yes");
    auto non_cas_label_instance = cas_label("no");

    _metrics.add_group(
            "cql",
            {
                    sm::make_counter(
                            "reads",
                            sm::description("Counts the total number of CQL SELECT requests."),
                            [this] {
                                // Reads fall into `cond_selector::NO_CONDITIONS' pigeonhole
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT);
                            }),

                    sm::make_counter(
                            "inserts",
                            sm::description("Counts the total number of CQL INSERT requests with/without conditions."),
                            {non_cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT);
                            }),
                    sm::make_counter(
                            "inserts",
                            sm::description("Counts the total number of CQL INSERT requests with/without conditions."),
                            {cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT);
                            }),

                    sm::make_counter(
                            "updates",
                            sm::description("Counts the total number of CQL UPDATE requests with/without conditions."),
                            {non_cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE);
                            }),
                    sm::make_counter(
                            "updates",
                            sm::description("Counts the total number of CQL UPDATE requests with/without conditions."),
                            {cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE);
                            }),

                    sm::make_counter(
                            "deletes",
                            sm::description("Counts the total number of CQL DELETE requests with/without conditions."),
                            {non_cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE);
                            }),
                    sm::make_counter(
                            "deletes",
                            sm::description("Counts the total number of CQL DELETE requests with/without conditions."),
                            {cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE);
                            }),

                    sm::make_counter(
                            "reads_per_ks",
                            // Reads fall into `cond_selector::NO_CONDITIONS' pigeonhole
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT),
                            sm::description("Counts the number of CQL SELECT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance}),
                    sm::make_counter(
                            "reads_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT),
                            sm::description("Counts the number of CQL SELECT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance}),

                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {user_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {internal_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {user_who_label_instance, system_ks_label_instance, cas_label_instance}),
                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {internal_who_label_instance, system_ks_label_instance, cas_label_instance}),

                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, cas_label_instance}),
                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, cas_label_instance}),

                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, cas_label_instance}),
                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, cas_label_instance}),

                    sm::make_counter(
                            "batches",
                            _cql_stats.batches,
                            sm::description("Counts the total number of CQL BATCH requests without conditions."),
                            {non_cas_label_instance}),

                    sm::make_counter(
                            "batches",
                            _cql_stats.cas_batches,
                            sm::description("Counts the total number of CQL BATCH requests with conditions."),
                            {cas_label_instance}),

                    sm::make_counter(
                            "statements_in_batches",
                            _cql_stats.statements_in_batches,
                            sm::description("Counts the total number of sub-statements in CQL BATCH requests without conditions."),
                            {non_cas_label_instance}),

                    sm::make_counter(
                            "statements_in_batches",
                            _cql_stats.statements_in_cas_batches,
                            sm::description("Counts the total number of sub-statements in CQL BATCH requests with conditions."),
                            {cas_label_instance}),

                    sm::make_counter(
                            "batches_pure_logged",
                            _cql_stats.batches_pure_logged,
                            sm::description(
                                    "Counts the total number of LOGGED batches that were executed as LOGGED batches.")),

                    sm::make_counter(
                            "batches_pure_unlogged",
                            _cql_stats.batches_pure_unlogged,
                            sm::description(
                                    "Counts the total number of UNLOGGED batches that were executed as UNLOGGED "
                                    "batches.")),

                    sm::make_counter(
                            "batches_unlogged_from_logged",
                            _cql_stats.batches_unlogged_from_logged,
                            sm::description("Counts the total number of LOGGED batches that were executed as UNLOGGED "
                                            "batches.")),

                    sm::make_counter(
                            "rows_read",
                            _cql_stats.rows_read,
                            sm::description("Counts the total number of rows read during CQL requests.")),

                    sm::make_counter(
                            "prepared_cache_evictions",
                            [] { return prepared_statements_cache::shard_stats().prepared_cache_evictions; },
                            sm::description("Counts the number of prepared statements cache entries evictions.")),

                    sm::make_counter(
                            "unprivileged_entries_evictions_on_size",
                            [] { return prepared_statements_cache::shard_stats().unprivileged_entries_evictions_on_size; },
                            sm::description("Counts a number of evictions of prepared statements from the prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.")),

                    sm::make_gauge(
                            "prepared_cache_size",
                            [this] { return _prepared_cache.size(); },
                            sm::description("A number of entries in the prepared statements cache.")),

                    sm::make_gauge(
                            "prepared_cache_memory_footprint",
                            [this] { return _prepared_cache.memory_footprint(); },
                            sm::description("Size (in bytes) of the prepared statements cache.")),

                    sm::make_counter(
                            "secondary_index_creates",
                            _cql_stats.secondary_index_creates,
                            sm::description("Counts the total number of CQL CREATE INDEX requests.")),

                    sm::make_counter(
                            "secondary_index_drops",
                            _cql_stats.secondary_index_drops,
                            sm::description("Counts the total number of CQL DROP INDEX requests.")),

                    // secondary_index_reads total count is also included in all cql reads
                    sm::make_counter(
                            "secondary_index_reads",
                            _cql_stats.secondary_index_reads,
                            sm::description("Counts the total number of CQL read requests performed using secondary indexes.")),

                    // secondary_index_rows_read total count is also included in all cql rows read
                    sm::make_counter(
                            "secondary_index_rows_read",
                            _cql_stats.secondary_index_rows_read,
                            sm::description("Counts the total number of rows read during CQL requests performed using secondary indexes.")),

                    // read requests that required ALLOW FILTERING
                    sm::make_counter(
                            "filtered_read_requests",
                            _cql_stats.filtered_reads,
                            sm::description("Counts the total number of CQL read requests that required ALLOW FILTERING. See filtered_rows_read_total to compare how many rows needed to be filtered.")),

                    // rows read with filtering enabled (because ALLOW FILTERING was required)
                    sm::make_counter(
                            "filtered_rows_read_total",
                            _cql_stats.filtered_rows_read_total,
                            sm::description("Counts the total number of rows read during CQL requests that required ALLOW FILTERING. See filtered_rows_matched_total and filtered_rows_dropped_total for information how accurate filtering queries are.")),

                    // rows read with filtering enabled and accepted by the filter
                    sm::make_counter(
                            "filtered_rows_matched_total",
                            _cql_stats.filtered_rows_matched_total,
                            sm::description("Counts the number of rows read during CQL requests that required ALLOW FILTERING and accepted by the filter. Number similar to filtered_rows_read_total indicates that filtering is accurate.")),

                    // rows read with filtering enabled and rejected by the filter
                    sm::make_counter(
                            "filtered_rows_dropped_total",
                            [this]() {return _cql_stats.filtered_rows_read_total - _cql_stats.filtered_rows_matched_total;},
                            sm::description("Counts the number of rows read during CQL requests that required ALLOW FILTERING and dropped by the filter. Number similar to filtered_rows_read_total indicates that filtering is not accurate and might cause performance degradation.")),

                    sm::make_counter(
                            "select_bypass_caches",
                            _cql_stats.select_bypass_caches,
                            sm::description("Counts the number of SELECT query executions with BYPASS CACHE option.")),

                    sm::make_counter(
                            "select_allow_filtering",
                            _cql_stats.select_allow_filtering,
                            sm::description("Counts the number of SELECT query executions with ALLOW FILTERING option.")),

                    sm::make_counter(
                            "select_partition_range_scan",
                            _cql_stats.select_partition_range_scan,
                            sm::description("Counts the number of SELECT query executions requiring partition range scan.")),

                    sm::make_counter(
                            "select_partition_range_scan_no_bypass_cache",
                            _cql_stats.select_partition_range_scan_no_bypass_cache,
                            sm::description("Counts the number of SELECT query executions requiring partition range scan without BYPASS CACHE option.")),

                    sm::make_counter(
                            "select_parallelized",
                            _cql_stats.select_parallelized,
                            sm::description("Counts the number of parallelized aggregation SELECT query executions.")),

                    sm::make_counter(
                            "authorized_prepared_statements_cache_evictions",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_cache_evictions; },
                            sm::description("Counts the number of authenticated prepared statements cache entries evictions.")),

                    sm::make_counter(
                            "authorized_prepared_statements_privileged_entries_evictions_on_size",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_privileged_entries_evictions_on_size; },
                            sm::description("Counts a number of evictions of prepared statements from the authorized prepared statements cache after they have been used more than once.")),

                    sm::make_counter(
                            "authorized_prepared_statements_unprivileged_entries_evictions_on_size",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_unprivileged_entries_evictions_on_size; },
                            sm::description("Counts a number of evictions of prepared statements from the authorized prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.")),

                    sm::make_gauge(
                            "authorized_prepared_statements_cache_size",
                            [this] { return _authorized_prepared_cache.size(); },
                            sm::description("Number of entries in the authenticated prepared statements cache.")),

                    sm::make_gauge(
                            "user_prepared_auth_cache_footprint",
                            [this] { return _authorized_prepared_cache.memory_footprint(); },
                            sm::description("Size (in bytes) of the authenticated prepared statements cache.")),

                    sm::make_counter(
                            "reverse_queries",
                            _cql_stats.reverse_queries,
                            sm::description("Counts the number of CQL SELECT requests with reverse ORDER BY order.")),

                    sm::make_counter(
                            "unpaged_select_queries",
                            [this] {
                                return _cql_stats.unpaged_select_queries(ks_selector::NONSYSTEM)
                                        + _cql_stats.unpaged_select_queries(ks_selector::SYSTEM);
                            },
                            sm::description("Counts the total number of unpaged CQL SELECT requests.")),

                    sm::make_counter(
                            "unpaged_select_queries_per_ks",
                            _cql_stats.unpaged_select_queries(ks_selector::SYSTEM),
                            sm::description("Counts the number of unpaged CQL SELECT requests against particular keyspaces."),
                            {system_ks_label_instance})

            });

    _mnotifier.register_listener(_migration_subscriber.get());
}

query_processor::~query_processor() {
}

future<> query_processor::stop() {
    return _mnotifier.unregister_listener(_migration_subscriber.get()).then([this] {
        return _authorized_prepared_cache.stop().finally([this] { return _prepared_cache.stop(); });
    }).then([this] {
        return _wasm_instance_cache ? _wasm_instance_cache->stop() : make_ready_future<>();
    });
}

future<::shared_ptr<result_message>>
query_processor::execute_direct_without_checking_exception_message(const sstring_view& query_string, service::query_state& query_state, query_options& options) {
    log.trace("execute_direct: \"{}\"", query_string);
    tracing::trace(query_state.get_trace_state(), "Parsing a statement");
    auto p = get_statement(query_string, query_state.get_client_state());
    auto cql_statement = p->statement;
    const auto warnings = std::move(p->warnings);
    if (cql_statement->get_bound_terms() != options.get_values_count()) {
        const auto msg = format("Invalid amount of bind variables: expected {:d} received {:d}",
                cql_statement->get_bound_terms(),
                options.get_values_count());
        throw exceptions::invalid_request_exception(msg);
    }
    options.prepare(p->bound_names);

    warn(unimplemented::cause::METRICS);
#if 0
        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();
#endif
    tracing::trace(query_state.get_trace_state(), "Processing a statement");
    return cql_statement->check_access(_backend, query_state.get_client_state()).then(
            [this, cql_statement, &query_state, &options, warnings = std::move(warnings)] () mutable {
        return process_authorized_statement(std::move(cql_statement), query_state, options).then(
                [warnings = std::move(warnings)] (::shared_ptr<result_message> m) {
                    for (const auto& w : warnings) {
                        m->add_warning(w);
                    }
                    return make_ready_future<::shared_ptr<result_message>>(m);
                });
    });
}

future<::shared_ptr<result_message>>
query_processor::execute_prepared_without_checking_exception_message(
        statements::prepared_statement::checked_weak_ptr prepared,
        cql3::prepared_cache_key_type cache_key,
        service::query_state& query_state,
        const query_options& options,
        bool needs_authorization) {

    ::shared_ptr<cql_statement> statement = prepared->statement;
    future<> fut = make_ready_future<>();
    if (needs_authorization) {
        fut = statement->check_access(_backend, query_state.get_client_state()).then([this, &query_state, prepared = std::move(prepared), cache_key = std::move(cache_key)] () mutable {
            return _authorized_prepared_cache.insert(*query_state.get_client_state().user(), std::move(cache_key), std::move(prepared)).handle_exception([] (auto eptr) {
                log.error("failed to cache the entry: {}", eptr);
            });
        });
    }
    log.trace("execute_prepared: \"{}\"", statement->raw_cql_statement);

    return fut.then([this, statement = std::move(statement), &query_state, &options] () mutable {
        return process_authorized_statement(std::move(statement), query_state, options);
    });
}

future<::shared_ptr<result_message>>
query_processor::process_authorized_statement(const ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options) {
    auto& client_state = query_state.get_client_state();

    ++_stats.queries_by_cl[size_t(options.get_consistency())];

    statement->validate(_backend, client_state);

    auto fut = statement->execute_without_checking_exception_message(_backend, query_state, options);

    return fut.then([statement] (auto msg) {
        if (msg) {
            return make_ready_future<::shared_ptr<result_message>>(std::move(msg));
        }
        return make_ready_future<::shared_ptr<result_message>>(::make_shared<result_message::void_message>());
    });
}

future<::shared_ptr<cql_transport::messages::result_message::prepared>>
query_processor::prepare(sstring query_string, service::query_state& query_state) {
    auto& client_state = query_state.get_client_state();
    return prepare(std::move(query_string), client_state, client_state.is_thrift());
}

future<::shared_ptr<cql_transport::messages::result_message::prepared>>
query_processor::prepare(sstring query_string, const service::client_state& client_state, bool for_thrift) {
    using namespace cql_transport::messages;
    if (for_thrift) {
        return prepare_one<result_message::prepared::thrift>(
                std::move(query_string),
                client_state,
                compute_thrift_id, prepared_cache_key_type::thrift_id);
    } else {
        return prepare_one<result_message::prepared::cql>(
                std::move(query_string),
                client_state,
                compute_id,
                prepared_cache_key_type::cql_id);
    }
}

static std::string hash_target(std::string_view query_string, std::string_view keyspace) {
    std::string ret(keyspace);
    ret += query_string;
    return ret;
}

prepared_cache_key_type query_processor::compute_id(
        std::string_view query_string,
        std::string_view keyspace) {
    return prepared_cache_key_type(md5_hasher::calculate(hash_target(query_string, keyspace)));
}

prepared_cache_key_type query_processor::compute_thrift_id(
        const std::string_view& query_string,
        const sstring& keyspace) {
    uint32_t h = 0;
    for (auto&& c : hash_target(query_string, keyspace)) {
        h = 31*h + c;
    }
    return prepared_cache_key_type(static_cast<int32_t>(h));
}

std::unique_ptr<prepared_statement>
query_processor::get_statement(const sstring_view& query, const service::client_state& client_state) {
    std::unique_ptr<raw::parsed_statement> statement = parse_statement(query);

    // Set keyspace for statement that require login
    auto cf_stmt = dynamic_cast<raw::cf_statement*>(statement.get());
    if (cf_stmt) {
        cf_stmt->prepare_keyspace(client_state);
    }
    ++_stats.prepare_invocations;
    auto p = statement->prepare(_db, _cql_stats);
    p->statement->raw_cql_statement = sstring(query);
    return p;
}

std::unique_ptr<raw::parsed_statement>
query_processor::parse_statement(const sstring_view& query) {
    try {
        {
            const char* error_injection_key = "query_processor-parse_statement-test_failure";
            utils::get_local_injector().inject(error_injection_key, [&]() {
                if (query.find(error_injection_key) != sstring_view::npos) {
                    throw std::runtime_error(error_injection_key);
                }
            });
        }
        auto statement = util::do_with_parser(query,  std::mem_fn(&cql3_parser::CqlParser::query));
        if (!statement) {
            throw exceptions::syntax_exception("Parsing failed");
        }
        return statement;
    } catch (const exceptions::recognition_exception& e) {
        throw exceptions::syntax_exception(format("Invalid or malformed CQL query string: {}", e.what()));
    } catch (const exceptions::cassandra_exception& e) {
        throw;
    } catch (const std::exception& e) {
        log.error("The statement: {} could not be parsed: {}", query, e.what());
        throw exceptions::syntax_exception(format("Failed parsing statement: [{}] reason: {}", query, e.what()));
    }
}

std::vector<std::unique_ptr<raw::parsed_statement>>
query_processor::parse_statements(std::string_view queries) {
    try {
        auto statements = util::do_with_parser(queries, std::mem_fn(&cql3_parser::CqlParser::queries));
        if (statements.empty()) {
            throw exceptions::syntax_exception("Parsing failed");
        }
        return statements;
    } catch (const exceptions::recognition_exception& e) {
        throw exceptions::syntax_exception(format("Invalid or malformed CQL query string: {}", e.what()));
    } catch (const exceptions::cassandra_exception& e) {
        throw;
    } catch (const std::exception& e) {
        log.error("The statements: {} could not be parsed: {}", queries, e.what());
        throw exceptions::syntax_exception(format("Failed parsing statements: [{}] reason: {}", queries, e.what()));
    }
}

query_options query_processor::make_internal_options(
        const statements::prepared_statement::checked_weak_ptr& p,
        const std::initializer_list<data_value>& values,
        db::consistency_level cl,
        int32_t page_size) const {
    if (p->bound_names.size() != values.size()) {
        throw std::invalid_argument(
                format("Invalid number of values. Expecting {:d} but got {:d}", p->bound_names.size(), values.size()));
    }
    auto ni = p->bound_names.begin();
    std::vector<cql3::raw_value> bound_values;
    for (auto& v : values) {
        auto& n = *ni++;
        if (v.type() == bytes_type) {
            bound_values.push_back(cql3::raw_value::make_value(value_cast<bytes>(v)));
        } else if (v.is_null()) {
            bound_values.push_back(cql3::raw_value::make_null());
        } else {
            bound_values.push_back(cql3::raw_value::make_value(n->type->decompose(v)));
        }
    }
    if (page_size > 0) {
        lw_shared_ptr<service::pager::paging_state> paging_state;
        db::consistency_level serial_consistency = db::consistency_level::SERIAL;
        api::timestamp_type ts = api::missing_timestamp;
        return query_options(
                cl,
                bound_values,
                cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts});
    }
    return query_options(cl, bound_values);
}

statements::prepared_statement::checked_weak_ptr query_processor::prepare_internal(const sstring& query_string) {
    auto& p = _internal_statements[query_string];
    if (p == nullptr) {
        auto np = parse_statement(query_string)->prepare(_db, _cql_stats);
        np->statement->raw_cql_statement = query_string;
        np->statement->validate(_backend, *_internal_state);
        p = std::move(np); // inserts it into map
    }
    return p->checked_weak_from_this();
}

struct internal_query_state {
    sstring query_string;
    std::unique_ptr<query_options> opts;
    statements::prepared_statement::checked_weak_ptr p;
    bool more_results = true;
};

::shared_ptr<internal_query_state> query_processor::create_paged_state(
        const sstring& query_string,
        db::consistency_level cl,
        const std::initializer_list<data_value>& values,
        int32_t page_size) {
    auto p = prepare_internal(query_string);
    auto opts = make_internal_options(p, values, cl, page_size);
    ::shared_ptr<internal_query_state> res = ::make_shared<internal_query_state>(
            internal_query_state{
                    query_string,
                    std::make_unique<cql3::query_options>(std::move(opts)), std::move(p),
                    true});
    return res;
}

bool query_processor::has_more_results(::shared_ptr<cql3::internal_query_state> state) const {
    if (state) {
        return state->more_results;
    }
    return false;
}

future<> query_processor::for_each_cql_result(
        ::shared_ptr<cql3::internal_query_state> state,
         noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set::row&)>&& f) {
    do {
        auto msg = co_await execute_paged_internal(state);
        for (auto& row : *msg) {
            if ((co_await f(row)) == stop_iteration::yes) {
                co_return;
            }
        }
    } while (has_more_results(state));
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_paged_internal(::shared_ptr<internal_query_state> state) {
    return state->p->statement->execute(_backend, *_internal_state, *state->opts).then(
            [state, this](::shared_ptr<cql_transport::messages::result_message> msg) mutable {
        class visitor : public result_message::visitor_base {
            ::shared_ptr<internal_query_state> _state;
            query_processor& _qp;
        public:
            visitor(::shared_ptr<internal_query_state> state, query_processor& qp) : _state(state), _qp(qp) {
            }
            virtual ~visitor() = default;
            void visit(const result_message::rows& rmrs) override {
                auto& rs = rmrs.rs();
                if (rs.get_metadata().paging_state()) {
                    bool done = !rs.get_metadata().flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>();

                    if (done) {
                        _state->more_results = false;
                    } else {
                        const service::pager::paging_state& st = *rs.get_metadata().paging_state();
                        lw_shared_ptr<service::pager::paging_state> shrd = make_lw_shared<service::pager::paging_state>(st);
                        _state->opts = std::make_unique<query_options>(std::move(_state->opts), shrd);
                        _state->p = _qp.prepare_internal(_state->query_string);
                    }
                } else {
                    _state->more_results = false;
                }
            }
        };
        visitor v(state, *this);
        if (msg != nullptr) {
            msg->accept(v);
        }
        return make_ready_future<::shared_ptr<untyped_result_set>>(::make_shared<untyped_result_set>(msg));
    });
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_internal(
        const sstring& query_string,
        db::consistency_level cl,
        const std::initializer_list<data_value>& values,
        cache_internal cache) {
    return execute_internal(query_string, cl, *_internal_state, values, cache);
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_internal(
        const sstring& query_string,
        db::consistency_level cl,
        service::query_state& query_state,
        const std::initializer_list<data_value>& values,
        cache_internal cache) {

    if (log.is_enabled(logging::log_level::trace)) {
        log.trace("execute_internal: {}\"{}\" ({})", cache ? "(cached) " : "", query_string, fmt::join(values, ", "));
    }
    if (cache) {
        return execute_with_params(prepare_internal(query_string), cl, query_state, values);
    } else {
        auto p = parse_statement(query_string)->prepare(_db, _cql_stats);
        p->statement->raw_cql_statement = query_string;
        p->statement->validate(_backend, *_internal_state);
        auto checked_weak_ptr = p->checked_weak_from_this();
        return execute_with_params(std::move(checked_weak_ptr), cl, query_state, values).finally([p = std::move(p)] {});
    }
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_with_params(
        statements::prepared_statement::checked_weak_ptr p,
        db::consistency_level cl,
        service::query_state& query_state,
        const std::initializer_list<data_value>& values) {
    auto opts = make_internal_options(p, values, cl);
    return do_with(std::move(opts), [this, &query_state, p = std::move(p)](auto & opts) {
        return p->statement->execute(_backend, query_state, opts).then([](auto msg) {
            return make_ready_future<::shared_ptr<untyped_result_set>>(::make_shared<untyped_result_set>(msg));
        });
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
query_processor::execute_batch_without_checking_exception_message(
        ::shared_ptr<statements::batch_statement> batch,
        service::query_state& query_state,
        query_options& options,
        std::unordered_map<prepared_cache_key_type, authorized_prepared_statements_cache::value_type> pending_authorization_entries) {
    return batch->check_access(_backend, query_state.get_client_state()).then([this, &query_state, &options, batch, pending_authorization_entries = std::move(pending_authorization_entries)] () mutable {
        return parallel_for_each(pending_authorization_entries, [this, &query_state] (auto& e) {
            return _authorized_prepared_cache.insert(*query_state.get_client_state().user(), e.first, std::move(e.second)).handle_exception([] (auto eptr) {
                log.error("failed to cache the entry: {}", eptr);
            });
        }).then([this, &query_state, &options, batch] {
            batch->validate();
            batch->validate(_backend, query_state.get_client_state());
            _stats.queries_by_cl[size_t(options.get_consistency())] += batch->get_statements().size();
            if (log.is_enabled(logging::log_level::trace)) {
                std::ostringstream oss;
                for (const auto& s: batch->get_statements()) {
                    oss << std::endl <<  s.statement->raw_cql_statement;
                }
                log.trace("execute_batch({}): {}", batch->get_statements().size(), oss.str());
            }
            return batch->execute(_backend, query_state, options);
        });
    });
}

query_processor::migration_subscriber::migration_subscriber(query_processor* qp) : _qp{qp} {
}

void query_processor::migration_subscriber::on_create_keyspace(const sstring& ks_name) {
}

void query_processor::migration_subscriber::on_create_column_family(const sstring& ks_name, const sstring& cf_name) {
}

void query_processor::migration_subscriber::on_create_user_type(const sstring& ks_name, const sstring& type_name) {
}

void query_processor::migration_subscriber::on_create_function(const sstring& ks_name, const sstring& function_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_create_view(const sstring& ks_name, const sstring& view_name) {
}

void query_processor::migration_subscriber::on_update_keyspace(const sstring& ks_name) {
}

void query_processor::migration_subscriber::on_update_column_family(
        const sstring& ks_name,
        const sstring& cf_name,
        bool columns_changed) {
    // #1255: Ignoring columns_changed deliberately.
    log.info("Column definitions for {}.{} changed, invalidating related prepared statements", ks_name, cf_name);
    remove_invalid_prepared_statements(ks_name, cf_name);
}

void query_processor::migration_subscriber::on_update_user_type(const sstring& ks_name, const sstring& type_name) {
}

void query_processor::migration_subscriber::on_update_function(const sstring& ks_name, const sstring& function_name) {
}

void query_processor::migration_subscriber::on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) {
}

void query_processor::migration_subscriber::on_update_view(
        const sstring& ks_name,
        const sstring& view_name, bool columns_changed) {
}

void query_processor::migration_subscriber::on_update_tablet_metadata() {
}

void query_processor::migration_subscriber::on_drop_keyspace(const sstring& ks_name) {
    remove_invalid_prepared_statements(ks_name, std::nullopt);
}

void query_processor::migration_subscriber::on_drop_column_family(const sstring& ks_name, const sstring& cf_name) {
    remove_invalid_prepared_statements(ks_name, cf_name);
}

void query_processor::migration_subscriber::on_drop_user_type(const sstring& ks_name, const sstring& type_name) {
}

void query_processor::migration_subscriber::on_drop_function(const sstring& ks_name, const sstring& function_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_drop_view(const sstring& ks_name, const sstring& view_name) {
    remove_invalid_prepared_statements(ks_name, view_name);
}

void query_processor::migration_subscriber::remove_invalid_prepared_statements(
        sstring ks_name,
        std::optional<sstring> cf_name) {
    _qp->_prepared_cache.remove_if([&] (::shared_ptr<cql_statement> stmt) {
        return this->should_invalidate(ks_name, cf_name, stmt);
    });
}

bool query_processor::migration_subscriber::should_invalidate(
        sstring ks_name,
        std::optional<sstring> cf_name,
        ::shared_ptr<cql_statement> statement) {
    return statement->depends_on(ks_name, cf_name);
}

future<> query_processor::query_internal(
        const sstring& query_string,
        db::consistency_level cl,
        const std::initializer_list<data_value>& values,
        int32_t page_size,
        noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)>&& f) {
    return for_each_cql_result(create_paged_state(query_string, cl, values, page_size), std::move(f));
}

future<> query_processor::query_internal(
        const sstring& query_string,
        noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)>&& f) {
    return query_internal(query_string, db::consistency_level::ONE, {}, 1000, std::move(f));
}

shared_ptr<cql_transport::messages::result_message> query_processor::bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls) {
    _proxy.get_stats().replica_cross_shard_ops++;
    return ::make_shared<cql_transport::messages::result_message::bounce_to_shard>(shard, std::move(cached_fn_calls));
}

void query_processor::update_authorized_prepared_cache_config() {
    utils::loading_cache_config cfg;
    cfg.max_size = _mcfg.authorized_prepared_cache_size;
    cfg.expiry = std::min(std::chrono::milliseconds(_db.get_config().permissions_validity_in_ms()),
                          std::chrono::duration_cast<std::chrono::milliseconds>(prepared_statements_cache::entry_expiry));
    cfg.refresh = std::chrono::milliseconds(_db.get_config().permissions_update_interval_in_ms());

    if (!_authorized_prepared_cache.update_config(std::move(cfg))) {
        log.error("Failed to apply authorized prepared statements cache changes. Please read the documentation of these parameters");
    }
}

void query_processor::reset_cache() {
    _authorized_prepared_cache.reset();
}

}
