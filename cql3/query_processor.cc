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

#include "cql3/query_processor.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"

#include "transport/messages/result_message.hh"

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>

namespace cql3 {

using namespace statements;
using namespace transport::messages;

thread_local logging::logger log("query_processor");

const sstring query_processor::CQL_VERSION = "3.2.0";

future<::shared_ptr<result_message>>
query_processor::process(const sstring_view& query_string, service::query_state& query_state, query_options& options)
{
    auto p = get_statement(query_string, query_state.get_client_state());
    options.prepare(p->bound_names);
    auto cql_statement = p->statement;
    if (cql_statement->get_bound_terms() != options.get_values().size()) {
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
    statement->validate(client_state);

    return statement->execute(_proxy, query_state, options)
        .then([statement] (auto msg) {
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
query_processor::prepare(const std::experimental::string_view& query_string, service::client_state& client_state, bool for_thrift)
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
        throw std::runtime_error("not implemented");
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
        throw std::runtime_error("not implemented");
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

static bytes md5_calculate(const std::experimental::string_view& s)
{
    constexpr size_t size = CryptoPP::Weak1::MD5::DIGESTSIZE;
    CryptoPP::Weak::MD5 hash;
    unsigned char digest[size];
    hash.CalculateDigest(digest, reinterpret_cast<const unsigned char*>(s.data()), s.size());
    return std::move(bytes{reinterpret_cast<const char*>(digest), size});
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
query_processor::get_statement(const sstring_view& query, service::client_state& client_state)
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
            // TODO: We should plug into get_rec()->displayRecognitionError and call error_collector from there
            throw exceptions::syntax_exception("Parsing failed");
        };
        return std::move(statement);
    } catch (const exceptions::recognition_exception& e) {
        throw exceptions::syntax_exception(sprint("Invalid or malformed CQL query string: %s", e.what()));
    } catch (const std::exception& e) {
        log.error("The statement: {} could not be parsed: {}", query, e.what());
        throw exceptions::syntax_exception(sprint("Failed parsing statement: [%s] reason: %s", query, e.what()));
    }
}

}
