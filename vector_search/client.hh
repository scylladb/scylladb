/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/http/client.hh>

namespace vector_search {

class client {

public:
    using content_type = std::vector<seastar::temporary_buffer<char>>;

    struct response {
        seastar::http::reply::status_type status;
        content_type content;
    };

    struct endpoint_type {
        seastar::sstring host;
        std::uint16_t port;
        seastar::net::inet_address ip;
    };

    explicit client(endpoint_type endpoint_);

    seastar::future<> status(seastar::abort_source& as);

    seastar::future<response> ann(seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source& as);

    seastar::future<> close();

    const endpoint_type& endpoint() const {
        return _endpoint;
    }

private:
    seastar::future<response> request(seastar::http::request req, std::optional<seastar::http::reply::status_type> expected_status, seastar::abort_source& as);

    endpoint_type _endpoint;
    seastar::http::experimental::client _http_client;
};

} // namespace vector_search
