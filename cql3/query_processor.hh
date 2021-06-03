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

#include <string_view>
#include <unordered_map>

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include "cql3/prepared_statements_cache.hh"
#include "cql3/authorized_prepared_statements_cache.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/prepared_statement.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "service/migration_listener.hh"
#include "service/query_state.hh"
#include "transport/messages/result_message.hh"

namespace service {
class migration_manager;
}

namespace cql3 {

namespace statements {
class batch_statement;

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

class query_processor {
public:
    class migration_subscriber;
    struct memory_config {
        size_t prepared_statment_cache_size = 0;
        size_t authorized_prepared_cache_size = 0;
    };

private:
    std::unique_ptr<migration_subscriber> _migration_subscriber;
    service::storage_proxy& _proxy;
    database& _db;
    service::migration_notifier& _mnotifier;
    service::migration_manager& _mm;
    const cql_config& _cql_config;

    struct stats {
        uint64_t prepare_invocations = 0;
        uint64_t queries_by_cl[size_t(db::consistency_level::MAX_VALUE) + 1] = {};
    } _stats;

    cql_stats _cql_stats;

    seastar::metrics::metric_groups _metrics;

    class internal_state;
    std::unique_ptr<internal_state> _internal_state;

    prepared_statements_cache _prepared_cache;
    authorized_prepared_statements_cache _authorized_prepared_cache;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we
    // don't bother with expiration on those.
    std::unordered_map<sstring, std::unique_ptr<statements::prepared_statement>> _internal_statements;

public:
    static const sstring CQL_VERSION;

    static prepared_cache_key_type compute_id(
            std::string_view query_string,
            std::string_view keyspace);

    static prepared_cache_key_type compute_thrift_id(
            const std::string_view& query_string,
            const sstring& keyspace);

    static std::unique_ptr<statements::raw::parsed_statement> parse_statement(const std::string_view& query);

    query_processor(service::storage_proxy& proxy, database& db, service::migration_notifier& mn, service::migration_manager& mm, memory_config mcfg, cql_config& cql_cfg,
            sharded<qos::service_level_controller> &sl_controller);

    ~query_processor();

    database& db() {
        return _db;
    }

    const cql_config& get_cql_config() const {
        return _cql_config;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    const service::migration_manager& get_migration_manager() const noexcept { return _mm; }
    service::migration_manager& get_migration_manager() noexcept { return _mm; }

    cql_stats& get_cql_stats() {
        return _cql_stats;
    }

    statements::prepared_statement::checked_weak_ptr get_prepared(const std::optional<auth::authenticated_user>& user, const prepared_cache_key_type& key) {
        if (user) {
            auto it = _authorized_prepared_cache.find(*user, key);
            if (it != _authorized_prepared_cache.end()) {
                try {
                    return it->get()->checked_weak_from_this();
                } catch (seastar::checked_ptr_is_null_exception&) {
                    // If the prepared statement got invalidated - remove the corresponding authorized_prepared_statements_cache entry as well.
                    _authorized_prepared_cache.remove(*user, key);
                }
            }
        }
        return statements::prepared_statement::checked_weak_ptr();
    }

    statements::prepared_statement::checked_weak_ptr get_prepared(const prepared_cache_key_type& key) {
        auto it = _prepared_cache.find(key);
        if (it == _prepared_cache.end()) {
            return statements::prepared_statement::checked_weak_ptr();
        }
        return *it;
    }

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_prepared(
            statements::prepared_statement::checked_weak_ptr statement,
            cql3::prepared_cache_key_type cache_key,
            service::query_state& query_state,
            const query_options& options,
            bool needs_authorization);

    /// Execute a client statement that was not prepared.
    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_direct(
            const std::string_view& query_string,
            service::query_state& query_state,
            query_options& options);

    // NOTICE: Internal queries should be used with care, as they are expected
    // to be used for local tables (e.g. from the `system` keyspace).
    // Data modifications will usually be performed with consistency level ONE
    // and schema changes will not be announced to other nodes.
    // Because of that, changing global schema state (e.g. modifying non-local tables,
    // creating namespaces, etc) is explicitly forbidden via this interface.
    future<::shared_ptr<untyped_result_set>>
    execute_internal(const sstring& query_string, const std::initializer_list<data_value>& values = { }) {
        return execute_internal(query_string, db::consistency_level::ONE, values, true);
    }

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
     */
    future<> query_internal(
            const sstring& query_string,
            db::consistency_level cl,
            const std::initializer_list<data_value>& values,
            int32_t page_size,
            noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)>&& f);

    /*
     * \brief iterate over all cql results using paging
     * An overload of query_internal without query parameters
     * using CL = ONE, no timeout, and page size = 1000.
     *
     * query_string - the cql string, can contain placeholders
     * f - a function to be run on each row of the query result,
     *     if the function returns stop_iteration::yes the iteration will stop
     */
    future<> query_internal(
            const sstring& query_string,
            noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)>&& f);

    // NOTICE: Internal queries should be used with care, as they are expected
    // to be used for local tables (e.g. from the `system` keyspace).
    // Data modifications will usually be performed with consistency level ONE
    // and schema changes will not be announced to other nodes.
    // Because of that, changing global schema state (e.g. modifying non-local tables,
    // creating namespaces, etc) is explicitly forbidden via this interface.
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const sstring& query_string,
            db::consistency_level,
            const std::initializer_list<data_value>& = { },
            bool cache = false);
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const sstring& query_string,
            db::consistency_level,
            service::query_state& query_state,
            const std::initializer_list<data_value>& = { },
            bool cache = false);

    future<::shared_ptr<untyped_result_set>> execute_with_params(
            statements::prepared_statement::checked_weak_ptr p,
            db::consistency_level,
            service::query_state& query_state,
            const std::initializer_list<data_value>& = { });

    future<::shared_ptr<cql_transport::messages::result_message::prepared>>
    prepare(sstring query_string, service::query_state& query_state);

    future<::shared_ptr<cql_transport::messages::result_message::prepared>>
    prepare(sstring query_string, const service::client_state& client_state, bool for_thrift);

    future<> stop();

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_batch(
            ::shared_ptr<statements::batch_statement>,
            service::query_state& query_state,
            query_options& options,
            std::unordered_map<prepared_cache_key_type, authorized_prepared_statements_cache::value_type> pending_authorization_entries);

    std::unique_ptr<statements::prepared_statement> get_statement(
            const std::string_view& query,
            const service::client_state& client_state);

    friend class migration_subscriber;

private:
    query_options make_internal_options(
            const statements::prepared_statement::checked_weak_ptr& p,
            const std::initializer_list<data_value>&,
            db::consistency_level,
            int32_t page_size = -1) const;

    future<::shared_ptr<cql_transport::messages::result_message>>
    process_authorized_statement(const ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options);

    /*!
     * \brief created a state object for paging
     *
     * When using paging internally a state object is needed.
     */
    ::shared_ptr<internal_query_state> create_paged_state(
            const sstring& query_string,
            db::consistency_level,
            const std::initializer_list<data_value>&,
            int32_t page_size);

    /*!
     * \brief run a query using paging
     */
    future<::shared_ptr<untyped_result_set>> execute_paged_internal(::shared_ptr<internal_query_state> state);

    /*!
     * \brief iterate over all results using paging
     */
    future<> for_each_cql_result(
            ::shared_ptr<cql3::internal_query_state> state,
            std::function<stop_iteration(const cql3::untyped_result_set_row&)>&& f);

    /*!
     * \brief iterate over all results using paging, accept a function that returns a future
     */
    future<> for_each_cql_result(
            ::shared_ptr<cql3::internal_query_state> state,
             noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)>&& f);

    /*!
     * \brief check, based on the state if there are additional results
     * Users of the paging, should not use the internal_query_state directly
     */
    bool has_more_results(::shared_ptr<cql3::internal_query_state> state) const;

    ///
    /// \tparam ResultMsgType type of the returned result message (CQL or Thrift)
    /// \tparam PreparedKeyGenerator a function that generates the prepared statement cache key for given query and
    ///         keyspace
    /// \tparam IdGetter a function that returns the corresponding prepared statement ID (CQL or Thrift) for a given
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
                assert(bound_terms == prepared->bound_names.size());
                return make_ready_future<std::unique_ptr<statements::prepared_statement>>(std::move(prepared));
            }).then([&key, &id_getter, &client_state] (auto prep_ptr) {
                return make_ready_future<::shared_ptr<cql_transport::messages::result_message::prepared>>(
                        ::make_shared<ResultMsgType>(id_getter(key), std::move(prep_ptr),
                            client_state.is_protocol_extension_set(cql_transport::cql_protocol_extension::LWT_ADD_METADATA_MARK)));
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
