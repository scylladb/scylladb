/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "node.hh"
#include "seastar/core/future.hh"
#include "utils/exceptions.hh"
#include "utils/exponential_backoff_retry.hh"
#include <optional>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/sleep.hh>
#include <chrono>
#include <system_error>

using namespace std::chrono_literals;

namespace vector_search {
namespace {

bool is_server_unavailable(std::exception_ptr& err) {
    return try_catch<std::system_error>(err) != nullptr;
}

auto constexpr BACKOFF_RETRY_MIN_TIME = 100ms;
auto constexpr BACKOFF_RETRY_MAX_TIME = 20s;

} // namespace

node::node(client::endpoint_type ep)
    : _client(std::move(ep)) {
}

bool node::is_up() const {
    return _is_up;
}

seastar::future<client::response> node::ann(
        seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source& as) {
    auto f = co_await seastar::coroutine::as_future(_client.ann(std::move(keyspace), std::move(name), std::move(embedding), limit, as));
    if (f.failed()) {
        auto err = f.get_exception();
        if (is_server_unavailable(err)) {
            _is_up = false;
            co_await restart_pinging();
        }
        co_await seastar::coroutine::return_exception_ptr(err);
    }
    auto resp = co_await std::move(f);
    if (resp.status == seastar::http::reply::status_type::internal_server_error) {
        co_await handle_ann_failed();
    }
    co_return resp;
}

seastar::future<> node::close() {
    co_await stop_pinging();
    co_await _client.close();
}

seastar::future<bool> node::ping() {
    auto f = co_await coroutine::as_future(_client.status(_as));
    auto status = co_await std::move(f).handle_exception([](const auto&) {
        return client::status_type::unknown;
    });
    co_return status == client::status_type::serving;
}

seastar::future<> node::restart_pinging() {
    co_await stop_pinging();
    co_await start_pinging();
}

seastar::future<> node::start_pinging() {
    struct stop_retry {};
    _pinging = exponential_backoff_retry::do_until_value(BACKOFF_RETRY_MIN_TIME, BACKOFF_RETRY_MAX_TIME, _as, [this] -> future<std::optional<stop_retry>> {
        bool success = co_await ping();
        if (success) {
            _is_up = true;
            co_return stop_retry{};
        }
        co_return std::nullopt;
    }).discard_result();
    return make_ready_future();
}

seastar::future<> node::stop_pinging() {
    _as.request_abort();
    co_await std::move(_pinging).handle_exception([](auto) {});
    _as = seastar::abort_source{};
    _pinging = seastar::make_ready_future<>();
}

seastar::future<> node::handle_ann_failed() {
    _is_up = false;
    co_await restart_pinging();
}


} // namespace vector_search
