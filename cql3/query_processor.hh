/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <string_view>
#include <unordered_map>

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include "cql3/prepared_statements_cache.hh"
#include "cql3/authorized_prepared_statements_cache.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "exceptions/exceptions.hh"
#include "service/migration_listener.hh"
#include "timestamp.hh"
#include "transport/messages/result_message.hh"
#include "service/client_state.hh"
#include "service/broadcast_tables/experimental/query_result.hh"
#include "utils/assert.hh"
#include "utils/observable.hh"
#include "service/raft/raft_group0_client.hh"
#include "types/types.hh"


namespace lang { class manager; }
namespace service {
class migration_manager;
class query_state;
class mapreduce_service;
class raft_group0_client;

namespace broadcast_tables {
struct query;
}
}

namespace cql3 {

namespace statements {
class batch_statement;
class schema_altering_statement;

namespace raw {

class parsed_statement;

}
}

class untyped_result_set;
class untyped_result_set_row;

/*!
 * \brief to allow paging, holds
 * internal state, that needs to be passed to the execute statement.
 *
 */
struct internal_query_state;

class prepared_statement_is_too_big : public std::exception {
    sstring _msg;

public:
    static constexpr int max_query_prefix = 100;

    prepared_statement_is_too_big(const sstring& query_string)
        : _msg(seastar::format("Prepared statement is too big: {}", query_string.substr(0, max_query_prefix)))
    {
        // mark that we clipped the query string
        if (query_string.size() > max_query_prefix) {
            _msg += "...";
        }
    }

    virtual const char* what() const noexcept override {
        return _msg.c_str();
    }
};

class cql_config;
class query_options;
class cql_statement;

class query_processor : public seastar::peering_sharded_service<query_processor> {
public:
    class migration_subscriber;
    struct memory_config {
        size_t prepared_statment_cache_size = 0;
        size_t authorized_prepared_cache_size = 0;
    };

private:
    std::unique_ptr<migration_subscriber> _migration_subscriber;
    service::storage_proxy& _proxy;
    data_dictionary::database _db;
    service::migration_notifier& _mnotifier;
    memory_config _mcfg;
    const cql_config& _cql_config;

    struct remote;
    std::unique_ptr<remote> _remote;

    struct stats {
        uint64_t prepare_invocations = 0;
        uint64_t queries_by_cl[size_t(db::consistency_level::MAX_VALUE) + 1] = {};
    } _stats;

    cql_stats _cql_stats;

    seastar::metrics::metric_groups _metrics;

    prepared_statements_cache _prepared_cache;
    authorized_prepared_statements_cache _authorized_prepared_cache;

    std::function<void(uint32_t)> _auth_prepared_cache_cfg_cb;
    serialized_action _authorized_prepared_cache_config_action;
    utils::observer<uint32_t> _authorized_prepared_cache_update_interval_in_ms_observer;
    utils::observer<uint32_t> _authorized_prepared_cache_validity_in_ms_observer;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we
    // don't bother with expiration on those.
    std::unordered_map<sstring, std::unique_ptr<statements::prepared_statement>> _internal_statements;

    lang::manager& _lang_manager;
public:
    static const sstring CQL_VERSION;

    static prepared_cache_key_type compute_id(
            std::string_view query_string,
            std::string_view keyspace);

    static std::unique_ptr<statements::raw::parsed_statement> parse_statement(const std::string_view& query);
    static std::vector<std::unique_ptr<statements::raw::parsed_statement>> parse_statements(std::string_view queries);

    query_processor(service::storage_proxy& proxy, data_dictionary::database db, service::migration_notifier& mn, memory_config mcfg, cql_config& cql_cfg, utils::loading_cache_config auth_prep_cache_cfg, lang::manager& langm);

    ~query_processor();

    void start_remote(service::migration_manager&, service::mapreduce_service&,
                      service::storage_service& ss, service::raft_group0_client&);
    future<> stop_remote();

    data_dictionary::database db() {
        return _db;
    }

    const cql_config& get_cql_config() const {
        return _cql_config;
    }

    const service::storage_proxy& proxy() const noexcept {
        return _proxy;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    cql_stats& get_cql_stats() {
        return _cql_stats;
    }

    lang::manager& lang() { return _lang_manager; }

    db::system_keyspace::auth_version_t auth_version;

    statements::prepared_statement::checked_weak_ptr get_prepared(const std::optional<auth::authenticated_user>& user, const prepared_cache_key_type& key) {
        if (user) {
            auto vp = _authorized_prepared_cache.find(*user, key);
            if (vp) {
                try {
                    // Touch the corresponding prepared_statements_cache entry to make sure its last_read timestamp
                    // corresponds to the last time its value has been read.
                    //
                    // If we don't do this it may turn out that the most recently used prepared statement doesn't have
                    // the newest last_read timestamp and can get evicted before the not-so-recently-read statement if
                    // we need to create space in the prepared statements cache for a new entry.
                    //
                    // And this is going to trigger an eviction of the corresponding entry from the authorized_prepared_cache
                    // breaking the LRU paradigm of these caches.
                    _prepared_cache.touch(key);
                    return vp->get()->checked_weak_from_this();
                } catch (seastar::checked_ptr_is_null_exception&) {
                    // If the prepared statement got invalidated - remove the corresponding authorized_prepared_statements_cache entry as well.
                    _authorized_prepared_cache.remove(*user, key);
                }
            }
        }
        return statements::prepared_statement::checked_weak_ptr();
    }

    statements::prepared_statement::checked_weak_ptr get_prepared(const prepared_cache_key_type& key) {
        return _prepared_cache.find(key);
    }

    inline
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_prepared(
            statements::prepared_statement::checked_weak_ptr statement,
            cql3::prepared_cache_key_type cache_key,
            service::query_state& query_state,
            const query_options& options,
            bool needs_authorization) {
        auto cql_statement = statement->statement;
        return execute_prepared_without_checking_exception_message(
                query_state,
                std::move(cql_statement),
                options,
                std::move(statement),
                std::move(cache_key),
                needs_authorization)
                .then(cql_transport::messages::propagate_exception_as_future<::shared_ptr<cql_transport::messages::result_message>>);
    }

    // Like execute_prepared, but is allowed to return exceptions as result_message::exception.
    // The result_message::exception must be explicitly handled.
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_prepared_without_checking_exception_message(
                service::query_state& query_state,
                shared_ptr<cql_statement> statement,
                const query_options& options,
                statements::prepared_statement::checked_weak_ptr prepared,
                cql3::prepared_cache_key_type cache_key,
                bool needs_authorization);

    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute_prepared(
                service::query_state& query_state,
                shared_ptr<cql_statement> statement,
                const query_options& options,
                std::optional<service::group0_guard> guard,
                statements::prepared_statement::checked_weak_ptr prepared,
                cql3::prepared_cache_key_type cache_key,
                bool needs_authorization);

    /// Execute a client statement that was not prepared.
    inline
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_direct(
            const std::string_view& query_string,
            service::query_state& query_state,
            query_options& options) {
        return execute_direct_without_checking_exception_message(
                query_string,
                query_state,
                options)
                .then(cql_transport::messages::propagate_exception_as_future<::shared_ptr<cql_transport::messages::result_message>>);
    }

    // Like execute_direct, but is allowed to return exceptions as result_message::exception.
    // The result_message::exception must be explicitly handled.
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_direct_without_checking_exception_message(
            const std::string_view& query_string,
            service::query_state& query_state,
            query_options& options);

    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute_direct(
            service::query_state& query_state,
            shared_ptr<cql_statement> statement,
            const query_options& options,
            std::optional<service::group0_guard> guard,
            cql3::cql_warnings_vec warnings);

    statements::prepared_statement::checked_weak_ptr prepare_internal(const sstring& query);

    /*!
     * \brief iterate over all cql results using paging
     *
     * You create a statement with optional parameters and pass
     * a function that goes over the result rows.
     *
     * The passed function would be called for all rows; return future<stop_iteration::yes>
     * to stop iteration.
     *
     * For example:
            return query_internal(
                    "SELECT * from system.compaction_history",
                    db::consistency_level::ONE,
                    {},
                    [&history] (const cql3::untyped_result_set::row& row) mutable {
                ....
                ....
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });

     * You can use placeholders in the query, the statement will only be prepared once.
     *
     * query_string - the cql string, can contain placeholders
     * cl - consistency level of the query
     * values - values to be substituted for the placeholders in the query
     * page_size - maximum page size
     * f - a function to be run on each row of the query result,
     *     if the function returns stop_iteration::yes the iteration will stop
     *
     * \note This function is optimized for convenience, not performance.
     */
    future<> query_internal(
            const sstring& query_string,
            db::consistency_level cl,
            const data_value_list& values,
            int32_t page_size,
            noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)> f);

    /*
     * \brief iterate over all cql results using paging
     * An overload of query_internal without query parameters
     * using CL = ONE, no timeout, and page size = 1000.
     *
     * query_string - the cql string, can contain placeholders
     * f - a function to be run on each row of the query result,
     *     if the function returns stop_iteration::yes the iteration will stop
     *
     * \note This function is optimized for convenience, not performance.
     */
    future<> query_internal(
            const sstring& query_string,
            noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)> f);

    class cache_internal_tag;
    using cache_internal = bool_class<cache_internal_tag>;
    
    // NOTICE: Internal queries should be used with care, as they are expected
    // to be used for local tables (e.g. from the `system` keyspace).
    // Data modifications will usually be performed with consistency level ONE
    // and schema changes will not be announced to other nodes.
    // Because of that, changing global schema state (e.g. modifying non-local tables,
    // creating namespaces, etc) is explicitly forbidden via this interface.
    //
    // note: optimized for convenience, not performance.
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const sstring& query_string,
            db::consistency_level,
            const data_value_list&,
            cache_internal cache);
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const sstring& query_string,
            db::consistency_level,
            service::query_state& query_state,
            const data_value_list& values,
            cache_internal cache);
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const sstring& query_string,
            db::consistency_level cl,
            cache_internal cache) {
        return execute_internal(query_string, cl, {}, cache);
    }
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const sstring& query_string,
            db::consistency_level cl,
            service::query_state& query_state,
            cache_internal cache) {
        return execute_internal(query_string, cl, query_state, {}, cache);
    }
    future<::shared_ptr<untyped_result_set>>
    execute_internal(const sstring& query_string, const data_value_list& values, cache_internal cache) {
        return execute_internal(query_string, db::consistency_level::ONE, values, cache);
    }
    future<::shared_ptr<untyped_result_set>>
    execute_internal(const sstring& query_string, cache_internal cache) {
        return execute_internal(query_string, db::consistency_level::ONE, {}, cache);
    }

    // Obtains mutations from query. For internal usage, most notable
    // use-case is generating data for group0 announce(). Note that this
    // function enables putting multiple CQL queries into a single raft command
    // and vice versa, split mutations from one query into separate commands.
    // It supports write-only queries, read-modified-writes not supported.
    future<std::vector<mutation>> get_mutations_internal(
        const sstring query_string,
        service::query_state& query_state,
        api::timestamp_type timestamp,
        std::vector<data_value_or_unset> values);

    future<::shared_ptr<untyped_result_set>> execute_with_params(
            statements::prepared_statement::checked_weak_ptr p,
            db::consistency_level,
            service::query_state& query_state,
            const data_value_list& values = { });

    future<::shared_ptr<cql_transport::messages::result_message>> do_execute_with_params(
            service::query_state& query_state,
            shared_ptr<cql_statement> statement,
            const query_options& options,
            std::optional<service::group0_guard> guard);


    future<::shared_ptr<cql_transport::messages::result_message::prepared>>
    prepare(sstring query_string, service::query_state& query_state);

    future<::shared_ptr<cql_transport::messages::result_message::prepared>>
    prepare(sstring query_string, const service::client_state& client_state);

    future<> stop();

    inline
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_batch(
            ::shared_ptr<statements::batch_statement> stmt,
            service::query_state& query_state,
            query_options& options,
            std::unordered_map<prepared_cache_key_type, authorized_prepared_statements_cache::value_type> pending_authorization_entries) {
        return execute_batch_without_checking_exception_message(
                std::move(stmt),
                query_state,
                options,
                std::move(pending_authorization_entries))
                .then(cql_transport::messages::propagate_exception_as_future<::shared_ptr<cql_transport::messages::result_message>>);
    }

    // Like execute_batch, but is allowed to return exceptions as result_message::exception.
    // The result_message::exception must be explicitly handled.
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_batch_without_checking_exception_message(
            ::shared_ptr<statements::batch_statement>,
            service::query_state& query_state,
            query_options& options,
            std::unordered_map<prepared_cache_key_type, authorized_prepared_statements_cache::value_type> pending_authorization_entries);

    future<service::broadcast_tables::query_result>
    execute_broadcast_table_query(const service::broadcast_tables::query&);

    // Splits given `mapreduce_request` and distributes execution of resulting subrequests across a cluster.
    future<query::mapreduce_result>
    mapreduce(query::mapreduce_request, tracing::trace_state_ptr);

    struct retry_statement_execution_error : public std::exception {};

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_schema_statement(const statements::schema_altering_statement&, service::query_state& state, const query_options& options, service::group0_batch& mc);
    future<> announce_schema_statement(const statements::schema_altering_statement&, service::group0_batch& mc);

    std::unique_ptr<statements::prepared_statement> get_statement(
            const std::string_view& query,
            const service::client_state& client_state);

    friend class migration_subscriber;

    shared_ptr<cql_transport::messages::result_message> bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls);

    void update_authorized_prepared_cache_config();

    void reset_cache();

    bool topology_global_queue_empty();

private:
    // Keep the holder until you stop using the `remote` services.
    std::pair<std::reference_wrapper<remote>, gate::holder> remote();

    query_options make_internal_options(
            const statements::prepared_statement::checked_weak_ptr& p,
            const std::vector<data_value_or_unset>& values,
            db::consistency_level,
            int32_t page_size = -1) const;

    future<::shared_ptr<cql_transport::messages::result_message>>
    process_authorized_statement(const ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options, std::optional<service::group0_guard> guard);

    /*!
     * \brief created a state object for paging
     *
     * When using paging internally a state object is needed.
     */
    internal_query_state create_paged_state(
            const sstring& query_string,
            db::consistency_level,
            const data_value_list& values,
            int32_t page_size);

    /*!
     * \brief run a query using paging
     *
     * \note Optimized for convenience, not performance.
     */
    future<::shared_ptr<untyped_result_set>> execute_paged_internal(internal_query_state& state);

    /*!
     * \brief iterate over all results using paging, accept a function that returns a future
     *
     * \note Optimized for convenience, not performance.
     */
    future<> for_each_cql_result(
            cql3::internal_query_state& state,
            noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)> f);

    /*!
     * \brief check, based on the state if there are additional results
     * Users of the paging, should not use the internal_query_state directly
     */
    bool has_more_results(cql3::internal_query_state& state) const;

    template<typename... Args>
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_maybe_with_guard(service::query_state& query_state, ::shared_ptr<cql_statement> statement, const query_options& options,
        future<::shared_ptr<cql_transport::messages::result_message>>(query_processor::*fn)(service::query_state&, ::shared_ptr<cql_statement>, const query_options&, std::optional<service::group0_guard>, Args...), Args... args);

    future<::shared_ptr<cql_transport::messages::result_message>> execute_with_guard(
        std::function<future<::shared_ptr<cql_transport::messages::result_message>>(service::query_state&, ::shared_ptr<cql_statement>, const query_options&, std::optional<service::group0_guard>)> fn,
        ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options);

    ///
    /// \tparam ResultMsgType type of the returned result message (CQL)
    /// \tparam PreparedKeyGenerator a function that generates the prepared statement cache key for given query and
    ///         keyspace
    /// \tparam IdGetter a function that returns the corresponding prepared statement ID (CQL) for a given
    ////        prepared statement cache key
    /// \param query_string
    /// \param client_state
    /// \param id_gen prepared ID generator, called before the first deferring
    /// \param id_getter prepared ID getter, passed to deferred context by reference. The caller must ensure its
    ////       liveness.
    /// \return
    template <typename ResultMsgType, typename PreparedKeyGenerator, typename IdGetter>
    future<::shared_ptr<cql_transport::messages::result_message::prepared>>
    prepare_one(
            sstring query_string,
            const service::client_state& client_state,
            PreparedKeyGenerator&& id_gen,
            IdGetter&& id_getter) {
        return do_with(
                id_gen(query_string, client_state.get_raw_keyspace()),
                std::move(query_string),
                [this, &client_state, &id_getter](const prepared_cache_key_type& key, const sstring& query_string) {
            return _prepared_cache.get(key, [this, &query_string, &client_state] {
                auto prepared = get_statement(query_string, client_state);
                auto bound_terms = prepared->statement->get_bound_terms();
                if (bound_terms > std::numeric_limits<uint16_t>::max()) {
                    throw exceptions::invalid_request_exception(
                            format("Too many markers(?). {:d} markers exceed the allowed maximum of {:d}",
                                   bound_terms,
                                   std::numeric_limits<uint16_t>::max()));
                }
                SCYLLA_ASSERT(bound_terms == prepared->bound_names.size());
                return make_ready_future<std::unique_ptr<statements::prepared_statement>>(std::move(prepared));
            }).then([&key, &id_getter, &client_state] (auto prep_ptr) {
                const auto& warnings = prep_ptr->warnings;
                const auto msg =
                        ::make_shared<ResultMsgType>(id_getter(key), std::move(prep_ptr),
                            client_state.is_protocol_extension_set(cql_transport::cql_protocol_extension::LWT_ADD_METADATA_MARK));
                for (const auto& w : warnings) {
                    msg->add_warning(w);
                }
                return make_ready_future<::shared_ptr<cql_transport::messages::result_message::prepared>>(std::move(msg));
            }).handle_exception_type([&query_string] (typename prepared_statements_cache::statement_is_too_big&) {
                return make_exception_future<::shared_ptr<cql_transport::messages::result_message::prepared>>(
                        prepared_statement_is_too_big(query_string));
            });
        });
    };
};

class query_processor::migration_subscriber : public service::migration_listener {
    query_processor* _qp;

public:
    migration_subscriber(query_processor* qp);

    virtual void on_create_keyspace(const sstring& ks_name) override;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override;

    virtual void on_update_keyspace(const sstring& ks_name) override;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) override;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override;
    virtual void on_update_tablet_metadata(const locator::tablet_metadata_change_hint&) override;

    virtual void on_drop_keyspace(const sstring& ks_name) override;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;

private:
    void remove_invalid_prepared_statements(sstring ks_name, std::optional<sstring> cf_name);

    bool should_invalidate(
            sstring ks_name,
            std::optional<sstring> cf_name,
            ::shared_ptr<cql_statement> statement);
};

}
