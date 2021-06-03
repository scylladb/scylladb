/*
 * Copyright 2019-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "alternator/executor.hh"
#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/tls.hh>
#include <optional>
#include "alternator/auth.hh"
#include "utils/small_vector.hh"
#include "utils/updateable_value.hh"
#include <seastar/core/units.hh>

namespace alternator {

using chunked_content = rjson::chunked_content;

class server {
    static constexpr size_t content_length_limit = 16*MB;
    using alternator_callback = std::function<future<executor::request_return_type>(executor&, executor::client_state&,
            tracing::trace_state_ptr, service_permit, rjson::value, std::unique_ptr<request>)>;
    using alternator_callbacks_map = std::unordered_map<std::string_view, alternator_callback>;

    http_server _http_server;
    http_server _https_server;
    executor& _executor;
    cql3::query_processor& _qp;

    key_cache _key_cache;
    bool _enforce_authorization;
    utils::small_vector<std::reference_wrapper<seastar::httpd::http_server>, 2> _enabled_servers;
    gate _pending_requests;
    alternator_callbacks_map _callbacks;

    semaphore* _memory_limiter;
    utils::updateable_value<uint32_t> _max_concurrent_requests;

    class json_parser {
        static constexpr size_t yieldable_parsing_threshold = 16*KB;
        chunked_content _raw_document;
        rjson::value _parsed_document;
        std::exception_ptr _current_exception;
        semaphore _parsing_sem{1};
        condition_variable _document_waiting;
        condition_variable _document_parsed;
        abort_source _as;
        future<> _run_parse_json_thread;
    public:
        json_parser();
        // Moving a chunked_content into parse() allows parse() to free each
        // chunk as soon as it is parsed, so when chunks are relatively small,
        // we don't need to store the sum of unparsed and parsed sizes.
        future<rjson::value> parse(chunked_content&& content);
        future<> stop();
    };
    json_parser _json_parser;

public:
    server(executor& executor, cql3::query_processor& qp);

    future<> init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds,
            bool enforce_authorization, semaphore* memory_limiter, utils::updateable_value<uint32_t> max_concurrent_requests);
    future<> stop();
private:
    void set_routes(seastar::httpd::routes& r);
    // If verification succeeds, returns the authenticated user's username
    future<std::string> verify_signature(const seastar::httpd::request&, const chunked_content&);
    future<executor::request_return_type> handle_api_request(std::unique_ptr<request> req);
};

}

