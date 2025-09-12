/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "high_availability.hh"
#include "exception.hh"
#include "seastar/core/abort_source.hh"
#include <boost/algorithm/string.hpp>
#include <seastar/core/when_all.hh>
#include <seastar/core/loop.hh>

using namespace seastar;

namespace service::vector_search {
namespace {

constexpr auto HTTP_REQUEST_RETRIES = 3;

bool is_server_error(seastar::http::reply::status_type status) {
    return status >= seastar::http::reply::status_type(500) && status < seastar::http::reply::status_type(600);
}

} // namespace

seastar::future<client::ann_result> high_availability::ann(
        seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source* as) {

    if (!_uri) {
        throw service_disabled_exception{};
    }

    for (size_t i = 0; i < HTTP_REQUEST_RETRIES; i++) {
        auto client = co_await get_client(as);
        try {
            co_return co_await client->ann(std::move(keyspace), std::move(name), std::move(embedding), limit, as);
        } catch (const abort_requested_exception& e) {
            // Stop retrying if the request was aborted
            throw;
        } catch (const service_status_exception& e) {
            // Stop retrying if the error is not a server error
            // This means that client performed a bad request
            if (!is_server_error(e.status())) {
                throw;
            }
        } catch (...) {
        }
        // Refresh client address and retry
        co_await refresh_client_address(as);
        client = co_await get_client(as);
    }
    throw service_unavailable_exception();
}

seastar::future<> high_availability::set_uri(std::optional<uri> uri) {
    _uri = std::move(uri);
    co_await refresh_client_address(nullptr);
}

seastar::future<> high_availability::refresh_client_address(seastar::abort_source* as) {
    if (_uri) {
        auto addr = co_await _dns.resolve(_uri->host);
        if (addr) {
            co_await stop();
            _client = seastar::make_lw_shared<client>(endpoint{_uri->host, _uri->port, *addr});
            co_return;
        }
    }
    _client = nullptr;
    if (as && as->abort_requested()) {

        throw abort_requested_exception{};
    }
}

seastar::future<seastar::lw_shared_ptr<client>> high_availability::get_client(seastar::abort_source* as) {
    if (_client) {
        co_return _client;
    }
    co_await refresh_client_address(as);
    if (!_client) {
        throw service_address_unavailable_exception{};
    }
    co_return _client;
}

seastar::future<> high_availability::stop() {
    if (_client) {
        co_await _client->close();
    }
}

} // namespace service::vector_search
