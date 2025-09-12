/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "endpoint.hh"
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/http/client.hh>
#include <boost/noncopyable.hpp>

namespace service::vector_search {

class client : private boost::noncopyable {
public:
    using ann_result = std::vector<seastar::temporary_buffer<char>>;

    explicit client(endpoint endpoint_);

    const endpoint& endpoint() const {
        return _endpoint;
    }

    seastar::future<ann_result> ann(
            seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source* as);

    seastar::future<> close() {
        return _http_client.close();
    }

private:
    seastar::future<std::vector<seastar::temporary_buffer<char>>> request(seastar::http::request req, seastar::abort_source* as);

    ::service::vector_search::endpoint _endpoint;
    seastar::http::experimental::client _http_client;
};

} // namespace service::vector_search
