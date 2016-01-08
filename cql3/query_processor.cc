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

#include "cql3/query_processor.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"
#include "cql3/statements/batch_statement.hh"

#include "transport/messages/result_message.hh"

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>

namespace cql3 {

using namespace statements;
using namespace transport::messages;

logging::logger log("query_processor");

distributed<query_processor> _the_query_processor;

const sstring query_processor::CQL_VERSION = "3.2.0";

class query_processor::internal_state {
    service::query_state _qs;
public:
    internal_state()
            : _qs(service::client_state{service::client_state::internal_tag()}) {
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

query_processor::query_processor(distributed<service::storage_proxy>& proxy,
        distributed<database>& db)
    : _migration_subscriber{std::make_unique<migration_subscriber>(this)}
    , _proxy(proxy)
    , _db(db)
    , _internal_state(new internal_state())
{
    service::get_local_migration_manager().register_listener(_migration_subscriber.get());
}

query_processor::~query_processor()
{}

future<> query_processor::stop()
{
    service::get_local_migration_manager().unregister_listener(_migration_subscriber.get());
    return make_ready_future<>();
}

future<::shared_ptr<result_message>>
query_processor::process(const sstring_view& query_string, service::query_state& query_state, query_options& options)
{
    log.trace("process: \"{}\"", query_string);
    auto p = get_statement(query_string, query_state.get_client_state());
    options.prepare(p->bound_names);
    auto cql_statement = p->statement;
    if (cql_statement->get_bound_terms() != options.get_values_count()) {
        throw exceptions::invalid_request_exception("Invalid amount of bind variables");
    }

    warn(unimplemented::cause::METRICS);
#if 0
        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();
#endif
    return process_statement(std::move(cql_statement), query_state, options);
}

future<::shared_ptr<result_message>>
query_processor::process_statement(::shared_ptr<cql_statement> statement, service::query_state& query_state,
        const query_options& options)
{
#if 0
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
#endif
    auto& client_state = query_state.get_client_state();
    statement->check_access(client_state);
    statement->validate(_proxy, client_state);

    future<::shared_ptr<transport::messages::result_message>> fut = make_ready_future<::shared_ptr<transport::messages::result_message>>();
    if (query_state.get_client_state()._is_internal) {
        fut = statement->execute_internal(_proxy, query_state, options);
    } else  {
        fut = statement->execute(_proxy, query_state, options);
    }

    return fut.then([statement] (auto msg) {
        if (msg) {
            return make_ready_future<::shared_ptr<result_message>>(std::move(msg));
        }
        return make_ready_future<::shared_ptr<result_message>>(
            ::make_shared<result_message::void_message>());
    });
}

future<::shared_ptr<transport::messages::result_message::prepared>>
query_processor::prepare(const std::experimental::string_view& query_string, service::query_state& query_state)
{
    auto& client_state = query_state.get_client_state();
    return prepare(query_string, client_state, client_state.is_thrift());
}

future<::shared_ptr<transport::messages::result_message::prepared>>
query_processor::prepare(const std::experimental::string_view& query_string, const service::client_state& client_state, bool for_thrift)
{
    auto existing = get_stored_prepared_statement(query_string, client_state.get_raw_keyspace(), for_thrift);
    if (existing) {
        return make_ready_future<::shared_ptr<transport::messages::result_message::prepared>>(existing);
    }
    auto prepared = get_statement(query_string, client_state);
    auto bound_terms = prepared->statement->get_bound_terms();
    if (bound_terms > std::numeric_limits<uint16_t>::max()) {
        throw exceptions::invalid_request_exception(sprint("Too many markers(?). %d markers exceed the allowed maximum of %d", bound_terms, std::numeric_limits<uint16_t>::max()));
    }
    assert(bound_terms == prepared->bound_names.size());
    return store_prepared_statement(query_string, client_state.get_raw_keyspace(), std::move(prepared), for_thrift);
}

::shared_ptr<transport::messages::result_message::prepared>
query_processor::get_stored_prepared_statement(const std::experimental::string_view& query_string, const sstring& keyspace, bool for_thrift)
{
    if (for_thrift) {
        throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
#if 0
        Integer thriftStatementId = computeThriftId(queryString, keyspace);
        ParsedStatement.Prepared existing = thriftPreparedStatements.get(thriftStatementId);
        return existing == null ? null : ResultMessage.Prepared.forThrift(thriftStatementId, existing.boundNames);
#endif
    } else {
        auto statement_id = compute_id(query_string, keyspace);
        auto it = _prepared_statements.find(statement_id);
        if (it == _prepared_statements.end()) {
            return ::shared_ptr<result_message::prepared>();
        }
        return ::make_shared<result_message::prepared>(statement_id, it->second);
    }
}

future<::shared_ptr<transport::messages::result_message::prepared>>
query_processor::store_prepared_statement(const std::experimental::string_view& query_string, const sstring& keyspace,
        ::shared_ptr<statements::parsed_statement::prepared> prepared, bool for_thrift)
{
#if 0
    // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
    // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
    long statementSize = measure(prepared.statement);
    // don't execute the statement if it's bigger than the allowed threshold
    if (statementSize > MAX_CACHE_PREPARED_MEMORY)
        throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d bytes.",
                                                        statementSize,
                                                        MAX_CACHE_PREPARED_MEMORY));
#endif
    if (for_thrift) {
        throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
#if 0
        Integer statementId = computeThriftId(queryString, keyspace);
        thriftPreparedStatements.put(statementId, prepared);
        return ResultMessage.Prepared.forThrift(statementId, prepared.boundNames);
#endif
    } else {
        auto statement_id = compute_id(query_string, keyspace);
        _prepared_statements.emplace(statement_id, prepared);
        auto msg = ::make_shared<result_message::prepared>(statement_id, prepared);
        return make_ready_future<::shared_ptr<result_message::prepared>>(std::move(msg));
    }
}

void query_processor::invalidate_prepared_statement(bytes statement_id)
{
    _prepared_statements.erase(statement_id);
}

static bytes md5_calculate(const std::experimental::string_view& s)
{
    constexpr size_t size = CryptoPP::Weak1::MD5::DIGESTSIZE;
    CryptoPP::Weak::MD5 hash;
    unsigned char digest[size];
    hash.CalculateDigest(digest, reinterpret_cast<const unsigned char*>(s.data()), s.size());
    return std::move(bytes{reinterpret_cast<const int8_t*>(digest), size});
}

bytes query_processor::compute_id(const std::experimental::string_view& query_string, const sstring& keyspace)
{
    sstring to_hash;
    if (!keyspace.empty()) {
        to_hash += keyspace;
    }
    to_hash += query_string.to_string();
    return md5_calculate(to_hash);
}

::shared_ptr<parsed_statement::prepared>
query_processor::get_statement(const sstring_view& query, const service::client_state& client_state)
{
#if 0
        Tracing.trace("Parsing {}", queryStr);
#endif
    ::shared_ptr<parsed_statement> statement = parse_statement(query);

    // Set keyspace for statement that require login
    auto cf_stmt = dynamic_pointer_cast<cf_statement>(statement);
    if (cf_stmt) {
        cf_stmt->prepare_keyspace(client_state);
    }
#if 0
        Tracing.trace("Preparing statement");
#endif
    return statement->prepare(_db.local());
}

::shared_ptr<parsed_statement>
query_processor::parse_statement(const sstring_view& query)
{
    try {
        error_collector<cql3_parser::CqlLexer::RecognizerType> lexer_error_collector(query);
        error_collector<cql3_parser::CqlParser::RecognizerType> parser_error_collector(query);
        cql3_parser::CqlLexer::InputStreamType input{reinterpret_cast<const ANTLR_UINT8*>(query.begin()), ANTLR_ENC_UTF8, static_cast<ANTLR_UINT32>(query.size()), nullptr};
        cql3_parser::CqlLexer lexer{&input};
        lexer.set_error_listener(lexer_error_collector);
        cql3_parser::CqlParser::TokenStreamType tstream(ANTLR_SIZE_HINT, lexer.get_tokSource());
        cql3_parser::CqlParser parser{&tstream};
        parser.set_error_listener(parser_error_collector);

        auto statement = parser.query();

        lexer_error_collector.throw_first_syntax_error();
        parser_error_collector.throw_first_syntax_error();

        if (!statement) {
            throw exceptions::syntax_exception("Parsing failed");
        };
        return std::move(statement);
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
        ::shared_ptr<statements::parsed_statement::prepared> p,
        const std::initializer_list<data_value>& values) {
    if (p->bound_names.size() != values.size()) {
        throw std::invalid_argument(sprint("Invalid number of values. Expecting %d but got %d", p->bound_names.size(), values.size()));
    }
    auto ni = p->bound_names.begin();
    std::vector<bytes_opt> bound_values;
    for (auto& v : values) {
        auto& n = *ni++;
        if (v.type() == bytes_type) {
            bound_values.push_back({value_cast<bytes>(v)});
        } else if (v.is_null()) {
            bound_values.push_back({});
        } else {
            bound_values.push_back({n->type->decompose(v)});
        }
    }
    return query_options(bound_values);
}

::shared_ptr<statements::parsed_statement::prepared> query_processor::prepare_internal(
        const std::experimental::string_view& query_string) {

    auto& p = _internal_statements[sstring(query_string.begin(), query_string.end())];
    if (p == nullptr) {
        auto np = parse_statement(query_string)->prepare(_db.local());
        np->statement->validate(_proxy, *_internal_state);
        p = std::move(np); // inserts it into map
    }
    return p;
}

future<::shared_ptr<untyped_result_set>> query_processor::execute_internal(
        const std::experimental::string_view& query_string,
        const std::initializer_list<data_value>& values) {
    if (log.is_enabled(logging::log_level::trace)) {
        log.trace("execute_internal: \"{}\" ({})", query_string, ::join(", ", values));
    }
    auto p = prepare_internal(query_string);
    auto opts = make_internal_options(p, values);
    return do_with(std::move(opts),
            [this, p = std::move(p)](query_options & opts) {
                return p->statement->execute_internal(_proxy, *_internal_state, opts).then(
                        [](::shared_ptr<transport::messages::result_message> msg) {
                            return make_ready_future<::shared_ptr<untyped_result_set>>(::make_shared<untyped_result_set>(msg));
                        });
            });
}

future<::shared_ptr<transport::messages::result_message>>
query_processor::process_batch(::shared_ptr<statements::batch_statement> batch, service::query_state& query_state, query_options& options) {
    auto& client_state = query_state.get_client_state();
    batch->check_access(client_state);
    batch->validate();
    batch->validate(_proxy, client_state);
    return batch->execute(_proxy, query_state, options);
}

query_processor::migration_subscriber::migration_subscriber(query_processor* qp)
    : _qp{qp}
{
}

void query_processor::migration_subscriber::on_create_keyspace(const sstring& ks_name)
{
}

void query_processor::migration_subscriber::on_create_column_family(const sstring& ks_name, const sstring& cf_name)
{
}

void query_processor::migration_subscriber::on_create_user_type(const sstring& ks_name, const sstring& type_name)
{
}

void query_processor::migration_subscriber::on_create_function(const sstring& ks_name, const sstring& function_name)
{
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_update_keyspace(const sstring& ks_name)
{
}

void query_processor::migration_subscriber::on_update_column_family(const sstring& ks_name, const sstring& cf_name)
{
}

void query_processor::migration_subscriber::on_update_user_type(const sstring& ks_name, const sstring& type_name)
{
}

void query_processor::migration_subscriber::on_update_function(const sstring& ks_name, const sstring& function_name)
{
}

void query_processor::migration_subscriber::on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
}

void query_processor::migration_subscriber::on_drop_keyspace(const sstring& ks_name)
{
    remove_invalid_prepared_statements(ks_name, std::experimental::nullopt);
}

void query_processor::migration_subscriber::on_drop_column_family(const sstring& ks_name, const sstring& cf_name)
{
    remove_invalid_prepared_statements(ks_name, cf_name);
}

void query_processor::migration_subscriber::on_drop_user_type(const sstring& ks_name, const sstring& type_name)
{
}

void query_processor::migration_subscriber::on_drop_function(const sstring& ks_name, const sstring& function_name)
{
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name)
{
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::remove_invalid_prepared_statements(sstring ks_name, std::experimental::optional<sstring> cf_name)
{
    std::vector<bytes> invalid;
    for (auto& kv : _qp->_prepared_statements) {
        auto id   = kv.first;
        auto stmt = kv.second;
        if (should_invalidate(ks_name, cf_name, stmt->statement)) {
            invalid.emplace_back(id);
        }
    }
    for (auto& id : invalid) {
        get_query_processor().invoke_on_all([id] (auto& qp) {
            qp.invalidate_prepared_statement(id);
        });
    }
}

bool query_processor::migration_subscriber::should_invalidate(sstring ks_name, std::experimental::optional<sstring> cf_name, ::shared_ptr<cql_statement> statement)
{
    return statement->depends_on_keyspace(ks_name) && (!cf_name || statement->depends_on_column_family(*cf_name));
}

}
