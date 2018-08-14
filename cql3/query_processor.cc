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

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1

#include "cql3/query_processor.hh"

#include <cryptopp/md5.h>
#include <seastar/core/metrics.hh>

#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/util.hh"

namespace cql3 {

using namespace statements;
using namespace cql_transport::messages;

logging::logger log("query_processor");
logging::logger prep_cache_log("prepared_statements_cache");
logging::logger authorized_prepared_statements_cache_log("authorized_prepared_statements_cache");

distributed<query_processor> _the_query_processor;

const sstring query_processor::CQL_VERSION = "3.3.1";

const std::chrono::minutes prepared_statements_cache::entry_expiry = std::chrono::minutes(60);

class query_processor::internal_state {
    service::query_state _qs;
public:
    internal_state() : _qs(service::client_state{service::client_state::internal_tag()}) {
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
    api::timestamp_type next_timestamp() {
        return _qs.get_client_state().get_timestamp();
    }
};

api::timestamp_type query_processor::next_timestamp() {
    return _internal_state->next_timestamp();
}

query_processor::query_processor(service::storage_proxy& proxy, distributed<database>& db, query_processor::memory_config mcfg)
        : _migration_subscriber{std::make_unique<migration_subscriber>(this)}
        , _proxy(proxy)
        , _db(db)
        , _internal_state(new internal_state())
        , _prepared_cache(prep_cache_log, mcfg.prepared_statment_cache_size)
        , _authorized_prepared_cache(std::min(std::chrono::milliseconds(_db.local().get_config().permissions_validity_in_ms()),
                                              std::chrono::duration_cast<std::chrono::milliseconds>(prepared_statements_cache::entry_expiry)),
                                     std::chrono::milliseconds(_db.local().get_config().permissions_update_interval_in_ms()),
                                     mcfg.authorized_prepared_cache_size, authorized_prepared_statements_cache_log) {
    namespace sm = seastar::metrics;

    _metrics.add_group(
            "query_processor",
            {
                    sm::make_derive(
                            "statements_prepared",
                            _stats.prepare_invocations,
                            sm::description("Counts a total number of parsed CQL requests."))});

    _metrics.add_group(
            "cql",
            {
                    sm::make_derive(
                            "reads",
                            _cql_stats.reads,
                            sm::description("Counts a total number of CQL read requests.")),

                    sm::make_derive(
                            "inserts",
                            _cql_stats.inserts,
                            sm::description("Counts a total number of CQL INSERT requests.")),

                    sm::make_derive(
                            "updates",
                            _cql_stats.updates,
                            sm::description("Counts a total number of CQL UPDATE requests.")),

                    sm::make_derive(
                            "deletes",
                            _cql_stats.deletes,
                            sm::description("Counts a total number of CQL DELETE requests.")),

                    sm::make_derive(
                            "batches",
                            _cql_stats.batches,
                            sm::description("Counts a total number of CQL BATCH requests.")),

                    sm::make_derive(
                            "statements_in_batches",
                            _cql_stats.statements_in_batches,
                            sm::description("Counts a total number of sub-statements in CQL BATCH requests.")),

                    sm::make_derive(
                            "batches_pure_logged",
                            _cql_stats.batches_pure_logged,
                            sm::description(
                                    "Counts a total number of LOGGED batches that were executed as LOGGED batches.")),

                    sm::make_derive(
                            "batches_pure_unlogged",
                            _cql_stats.batches_pure_unlogged,
                            sm::description(
                                    "Counts a total number of UNLOGGED batches that were executed as UNLOGGED "
                                    "batches.")),

                    sm::make_derive(
                            "batches_unlogged_from_logged",
                            _cql_stats.batches_unlogged_from_logged,
                            sm::description("Counts a total number of LOGGED batches that were executed as UNLOGGED "
                                            "batches.")),

                    sm::make_derive(
                            "rows_read",
                            _cql_stats.rows_read,
                            sm::description("Counts a total number of rows read during CQL requests.")),

                    sm::make_derive(
                            "prepared_cache_evictions",
                            [] { return prepared_statements_cache::shard_stats().prepared_cache_evictions; },
                            sm::description("Counts a number of prepared statements cache entries evictions.")),

                    sm::make_gauge(
                            "prepared_cache_size",
                            [this] { return _prepared_cache.size(); },
                            sm::description("A number of entries in the prepared statements cache.")),

                    sm::make_gauge(
                            "prepared_cache_memory_footprint",
                            [this] { return _prepared_cache.memory_footprint(); },
                            sm::description("Size (in bytes) of the prepared statements cache.")),

                    sm::make_derive(
                            "secondary_index_creates",
                            _cql_stats.secondary_index_creates,
                            sm::description("Counts a total number of CQL CREATE INDEX requests.")),

                    sm::make_derive(
                            "secondary_index_drops",
                            _cql_stats.secondary_index_drops,
                            sm::description("Counts a total number of CQL DROP INDEX requests.")),

                    // secondary_index_reads total count is also included in all cql reads
                    sm::make_derive(
                            "secondary_index_reads",
                            _cql_stats.secondary_index_reads,
                            sm::description("Counts a total number of CQL read requests performed using secondary indexes.")),

                    // secondary_index_rows_read total count is also included in all cql rows read
                    sm::make_derive(
                            "secondary_index_rows_read",
                            _cql_stats.secondary_index_rows_read,
                            sm::description("Counts a total number of rows read during CQL requests performed using secondary indexes.")),

                    // read requests that required ALLOW FILTERING
                    sm::make_derive(
                            "filtered_read_requests",
                            _cql_stats.filtered_reads,
                            sm::description("Counts a total number of CQL read requests that required ALLOW FILTERING. See filtered_rows_read_total to compare how many rows needed to be filtered.")),

                    // rows read with filtering enabled (because ALLOW FILTERING was required)
                    sm::make_derive(
                            "filtered_rows_read_total",
                            _cql_stats.filtered_rows_read_total,
                            sm::description("Counts a total number of rows read during CQL requests that required ALLOW FILTERING. See filtered_rows_matched_total and filtered_rows_dropped_total for information how accurate filtering queries are.")),

                    // rows read with filtering enabled and accepted by the filter
                    sm::make_derive(
                            "filtered_rows_matched_total",
                            _cql_stats.filtered_rows_matched_total,
                            sm::description("Counts a number of rows read during CQL requests that required ALLOW FILTERING and accepted by the filter. Number similar to filtered_rows_read_total indicates that filtering is accurate.")),

                    // rows read with filtering enabled and rejected by the filter
                    sm::make_derive(
                            "filtered_rows_dropped_total",
                            [this]() {return _cql_stats.filtered_rows_read_total - _cql_stats.filtered_rows_matched_total;},
                            sm::description("Counts a number of rows read during CQL requests that required ALLOW FILTERING and dropped by the filter. Number similar to filtered_rows_read_total indicates that filtering is not accurate and might cause performance degradation.")),

                    sm::make_derive(
                            "authorized_prepared_statements_cache_evictions",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_cache_evictions; },
                            sm::description("Counts a number of authenticated prepared statements cache entries evictions.")),

                    sm::make_gauge(
                            "authorized_prepared_statements_cache_size",
                            [this] { return _authorized_prepared_cache.size(); },
                            sm::description("A number of entries in the authenticated prepared statements cache.")),

                    sm::make_gauge(
                            "user_prepared_auth_cache_footprint",
                            [this] { return _authorized_prepared_cache.memory_footprint(); },
                            sm::description("Size (in bytes) of the authenticated prepared statements cache.")),

                    sm::make_counter(
                            "reverse_queries",
                            _cql_stats.reverse_queries,
                            sm::description("Counts number of CQL SELECT requests with ORDER BY DESC.")),

                    sm::make_counter(
                            "unpaged_select_queries",
                            _cql_stats.unpaged_select_queries,
                            sm::description("Counts number of unpaged CQL SELECT requests.")),

            });

    service::get_local_migration_manager().register_listener(_migration_subscriber.get());
}

query_processor::~query_processor() {
}

future<> query_processor::stop() {
    service::get_local_migration_manager().unregister_listener(_migration_subscriber.get());
    return _authorized_prepared_cache.stop().finally([this] { return _prepared_cache.stop(); });
}

future<::shared_ptr<result_message>>
query_processor::process(const sstring_view& query_string, service::query_state& query_state, query_options& options) {
    log.trace("process: \"{}\"", query_string);
    tracing::trace(query_state.get_trace_state(), "Parsing a statement");
    auto p = get_statement(query_string, query_state.get_client_state());
    auto cql_statement = p->statement;
    if (cql_statement->get_bound_terms() != options.get_values_count()) {
        throw exceptions::invalid_request_exception("Invalid amount of bind variables");
    }
    options.prepare(p->bound_names);

    warn(unimplemented::cause::METRICS);
#if 0
        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();
#endif
    tracing::trace(query_state.get_trace_state(), "Processing a statement");
    return process_statement_unprepared(std::move(cql_statement), query_state, options);
}

future<::shared_ptr<result_message>>
query_processor::process_statement_unprepared(
        ::shared_ptr<cql_statement> statement,
        service::query_state& query_state,
        const query_options& options) {
    return statement->check_access(query_state.get_client_state()).then([this, statement, &query_state, &options] () mutable {
        return process_authorized_statement(std::move(statement), query_state, options);
    });
}

future<::shared_ptr<result_message>>
query_processor::process_statement_prepared(
        statements::prepared_statement::checked_weak_ptr prepared,
        cql3::prepared_cache_key_type cache_key,
        service::query_state& query_state,
        const query_options& options,
        bool needs_authorization) {

    ::shared_ptr<cql_statement> statement = prepared->statement;
    future<> fut = make_ready_future<>();
    if (needs_authorization) {
        fut = statement->check_access(query_state.get_client_state()).then([this, &query_state, prepared = std::move(prepared), cache_key = std::move(cache_key)] () mutable {
            return _authorized_prepared_cache.insert(*query_state.get_client_state().user(), std::move(cache_key), std::move(prepared)).handle_exception([this] (auto eptr) {
                log.error("failed to cache the entry", eptr);
            });
        });
    }

    return fut.then([this, statement = std::move(statement), &query_state, &options] () mutable {
        return process_authorized_statement(std::move(statement), query_state, options);
    });
}

future<::shared_ptr<result_message>>
query_processor::process_authorized_statement(const ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options) {
    auto& client_state = query_state.get_client_state();

    statement->validate(_proxy, client_state);

    auto fut = statement->execute(_proxy, query_state, options);

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

::shared_ptr<cql_transport::messages::result_message::prepared>
query_processor::get_stored_prepared_statement(
        const std::experimental::string_view& query_string,
        const sstring& keyspace,
        bool for_thrift) {
    using namespace cql_transport::messages;
    if (for_thrift) {
        return get_stored_prepared_statement_one<result_message::prepared::thrift>(
                query_string,
                keyspace,
                compute_thrift_id,
                prepared_cache_key_type::thrift_id);
    } else {
        return get_stored_prepared_statement_one<result_message::prepared::cql>(
                query_string,
                keyspace,
                compute_id,
                prepared_cache_key_type::cql_id);
    }
}

static bytes md5_calculate(const std::experimental::string_view& s) {
    constexpr size_t size = CryptoPP::Weak1::MD5::DIGESTSIZE;
    CryptoPP::Weak::MD5 hash;
    unsigned char digest[size];
    hash.CalculateDigest(digest, reinterpret_cast<const unsigned char*>(s.data()), s.size());
    return std::move(bytes{reinterpret_cast<const int8_t*>(digest), size});
}

static sstring hash_target(const std::experimental::string_view& query_string, const sstring& keyspace) {
    return keyspace + query_string.to_string();
}

prepared_cache_key_type query_processor::compute_id(
        const std::experimental::string_view& query_string,
        const sstring& keyspace) {
    return prepared_cache_key_type(md5_calculate(hash_target(query_string, keyspace)));
}

prepared_cache_key_type query_processor::compute_thrift_id(
        const std::experimental::string_view& query_string,
        const sstring& keyspace) {
    auto target = hash_target(query_string, keyspace);
    uint32_t h = 0;
    for (auto&& c : hash_target(query_string, keyspace)) {
        h = 31*h + c;
    }
    return prepared_cache_key_type(static_cast<int32_t>(h));
}

std::unique_ptr<prepared_statement>
query_processor::get_statement(const sstring_view& query, const service::client_state& client_state) {
    ::shared_ptr<raw::parsed_statement> statement = parse_statement(query);

    // Set keyspace for statement that require login
    auto cf_stmt = dynamic_pointer_cast<raw::cf_statement>(statement);
    if (cf_stmt) {
        cf_stmt->prepare_keyspace(client_state);
    }
    ++_stats.prepare_invocations;
    return statement->prepare(_db.local(), _cql_stats);
}

::shared_ptr<raw::parsed_statement>
query_processor::parse_statement(const sstring_view& query) {
    try {
        auto statement = util::do_with_parser(query,  std::mem_fn(&cql3_parser::CqlParser::query));
        if (!statement) {
            throw exceptions::syntax_exception("Parsing failed");
        }
        return statement;
    } catch (const exceptions::recognition_exception& e) {
        throw exceptions::syntax_exception(sprint("Invalid or malformed CQL query string: %s", e.what()));
    } catch (const exceptions::cassandra_exception& e) {
        throw;
    } catch (const std::exception& e) {
        log.error("The statement: {} could not be parsed: {}", query, e.what());
        throw exceptions::syntax_exception(sprint("Failed parsing statement: [%s] reason: %s", query, e.what()));
    }
}

query_options query_processor::make_internal_options(
        const statements::prepared_statement::checked_weak_ptr& p,
        const std::initializer_list<data_value>& values,
        db::consistency_level cl,
        const timeout_config& timeout_config,
        int32_t page_size) {
    if (p->bound_names.size() != values.size()) {
        throw std::invalid_argument(
                sprint("Invalid number of values. Expecting %d but got %d", p->bound_names.size(), values.size()));
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
        ::shared_ptr<service::pager::paging_state> paging_state;
        db::consistency_level serial_consistency = db::consistency_level::SERIAL;
        api::timestamp_type ts = api::missing_timestamp;
        return query_options(
                cl,
                timeout_config,
                bound_values,
                cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts});
    }
    return query_options(cl, timeout_config, bound_values);
}

statements::prepared_statement::checked_weak_ptr query_processor::prepare_internal(const sstring& query_string) {
    auto& p = _internal_statements[query_string];
    if (p == nullptr) {
        auto np = parse_statement(query_string)->prepare(_db.local(), _cql_stats);
        np->statement->validate(_proxy, *_internal_state);
        p = std::move(np); // inserts it into map
    }
    return p->checked_weak_from_this();
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_internal(const sstring& query_string, const std::initializer_list<data_value>& values) {
    if (log.is_enabled(logging::log_level::trace)) {
        log.trace("execute_internal: \"{}\" ({})", query_string, ::join(", ", values));
    }
    return execute_internal(prepare_internal(query_string), values);
}

struct internal_query_state {
    sstring query_string;
    std::unique_ptr<query_options> opts;
    statements::prepared_statement::checked_weak_ptr p;
    bool more_results = true;
};

::shared_ptr<internal_query_state> query_processor::create_paged_state(const sstring& query_string,
        const std::initializer_list<data_value>& values, int32_t page_size) {
    auto p = prepare_internal(query_string);
    auto opts = make_internal_options(p, values, db::consistency_level::ONE, infinite_timeout_config, page_size);
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
        std::function<stop_iteration(const cql3::untyped_result_set::row&)>&& f) {
    return do_with(seastar::shared_ptr<bool>(), [f, this, state](auto& is_done) mutable {
        is_done = seastar::make_shared<bool>(false);

        auto stop_when = [is_done]() {
            return *is_done;
        };
        auto do_resuls = [is_done, state, f, this]() mutable {
            return this->execute_paged_internal(
                    state).then([is_done, state, f, this](::shared_ptr<cql3::untyped_result_set> msg) mutable {
                if (msg->empty()) {
                    *is_done = true;
                } else {
                    if (!this->has_more_results(state)) {
                        *is_done = true;
                    }
                    for (auto& row : *msg) {
                        if (f(row) == stop_iteration::yes) {
                            *is_done = true;
                            break;
                        }
                    }
                }
            });
        };
        return do_until(stop_when, do_resuls);
    });
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_paged_internal(::shared_ptr<internal_query_state> state) {
    return state->p->statement->execute(_proxy, *_internal_state, *state->opts).then(
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
                        shared_ptr<service::pager::paging_state> shrd = ::make_shared<service::pager::paging_state>(st);
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
        statements::prepared_statement::checked_weak_ptr p,
        const std::initializer_list<data_value>& values) {
    query_options opts = make_internal_options(p, values, db::consistency_level::ONE, infinite_timeout_config);
    return do_with(std::move(opts), [this, p = std::move(p)](auto& opts) {
        return p->statement->execute(
                _proxy,
                *_internal_state,
                opts).then([&opts, stmt = p->statement](auto msg) {
            return make_ready_future<::shared_ptr<untyped_result_set>>(::make_shared<untyped_result_set>(msg));
        });
    });
}

future<::shared_ptr<untyped_result_set>>
query_processor::process(
        const sstring& query_string,
        db::consistency_level cl,
        const timeout_config& timeout_config,
        const std::initializer_list<data_value>& values,
        bool cache) {
    if (cache) {
        return process(prepare_internal(query_string), cl, timeout_config, values);
    } else {
        auto p = parse_statement(query_string)->prepare(_db.local(), _cql_stats);
        p->statement->validate(_proxy, *_internal_state);
        auto checked_weak_ptr = p->checked_weak_from_this();
        return process(std::move(checked_weak_ptr), cl, timeout_config, values).finally([p = std::move(p)] {});
    }
}

future<::shared_ptr<untyped_result_set>>
query_processor::process(
        statements::prepared_statement::checked_weak_ptr p,
        db::consistency_level cl,
        const timeout_config& timeout_config,
        const std::initializer_list<data_value>& values) {
    auto opts = make_internal_options(p, values, cl, timeout_config);
    return do_with(std::move(opts), [this, p = std::move(p)](auto & opts) {
        return p->statement->execute(_proxy, *_internal_state, opts).then([](auto msg) {
            return make_ready_future<::shared_ptr<untyped_result_set>>(::make_shared<untyped_result_set>(msg));
        });
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
query_processor::process_batch(
        ::shared_ptr<statements::batch_statement> batch,
        service::query_state& query_state,
        query_options& options,
        std::unordered_map<prepared_cache_key_type, authorized_prepared_statements_cache::value_type> pending_authorization_entries) {
    return batch->check_access(query_state.get_client_state()).then([this, &query_state, &options, batch, pending_authorization_entries = std::move(pending_authorization_entries)] () mutable {
        return parallel_for_each(pending_authorization_entries, [this, &query_state] (auto& e) {
            return _authorized_prepared_cache.insert(*query_state.get_client_state().user(), e.first, std::move(e.second)).handle_exception([this] (auto eptr) {
                log.error("failed to cache the entry", eptr);
            });
        }).then([this, &query_state, &options, batch] {
            batch->validate();
            batch->validate(_proxy, query_state.get_client_state());
            return batch->execute(_proxy, query_state, options);
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

void query_processor::migration_subscriber::on_drop_keyspace(const sstring& ks_name) {
    remove_invalid_prepared_statements(ks_name, std::experimental::nullopt);
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
        std::experimental::optional<sstring> cf_name) {
    _qp->_prepared_cache.remove_if([&] (::shared_ptr<cql_statement> stmt) {
        return this->should_invalidate(ks_name, cf_name, stmt);
    });
}

bool query_processor::migration_subscriber::should_invalidate(
        sstring ks_name,
        std::experimental::optional<sstring> cf_name,
        ::shared_ptr<cql_statement> statement) {
    return statement->depends_on_keyspace(ks_name) && (!cf_name || statement->depends_on_column_family(*cf_name));
}

}
