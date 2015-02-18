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

namespace cql3 {

using namespace statements;
using namespace transport::messages;

thread_local logging::logger log("query_processor");

future<::shared_ptr<result_message>>
query_processor::process(const sstring_view& query_string, service::query_state& query_state, query_options& options)
{
    std::unique_ptr<parsed_statement::prepared> p = get_statement(query_string, query_state.get_client_state());
    options.prepare(p->bound_names);
    auto cql_statement = p->statement;
    if (cql_statement->get_bound_terms() != options.get_values().size()) {
        throw exceptions::invalid_request_exception("Invalid amount of bind variables");
    }

    unimplemented::metrics();
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
        .then([] (auto msg) {
            if (msg) {
                return make_ready_future<::shared_ptr<result_message>>(std::move(msg));
            }
            return make_ready_future<::shared_ptr<result_message>>(
                ::make_shared<result_message::void_message>());
        });
}

std::unique_ptr<parsed_statement::prepared>
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
