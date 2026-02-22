/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dns.hh"
#include "utils/exceptions.hh"
#include <chrono>
#include <fmt/format.h>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/dns.hh>
#include <seastar/core/on_internal_error.hh>

using namespace seastar;

namespace vector_search {
namespace {

// Wait time before retrying after an exception occurred
constexpr auto EXCEPTION_OCCURRED_WAIT = std::chrono::seconds(5);

// Minimum interval between dns name refreshes
constexpr auto DNS_REFRESH_INTERVAL = std::chrono::seconds(5);

/// Wait for a timeout or abort signal.
auto wait_for_timeout(lowres_clock::duration timeout, abort_source& as) -> future<bool> {
    auto result = co_await coroutine::as_future(sleep_abortable(timeout, as));
    if (result.failed()) {
        auto err = result.get_exception();
        if (as.abort_requested()) {
            co_return false;
        }
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    co_return true;
}

auto to_address_type(const std::vector<net::hostent::address_entry>& entries) -> dns::address_type {
    dns::address_type addrs;
    addrs.reserve(entries.size());
    for (auto& e : entries) {
        addrs.push_back(e.addr);
    }
    return addrs;
}

} // namespace

dns::dns(logging::logger& logger, std::vector<seastar::sstring> hosts, listener_type listener, uint64_t& refreshes_counter)
    : _logger(logger)
    , _refresh_interval(DNS_REFRESH_INTERVAL)
    , _resolver([this](auto const& host) -> future<address_type> {
        auto f = co_await coroutine::as_future(net::dns::get_host_by_name(host));
        if (f.failed()) {
            auto err = f.get_exception();
            if (try_catch<std::system_error>(err) != nullptr) {
                co_return address_type{};
            }
            _logger.warn("Failed to resolve vector store service address: {}", err);
            co_await coroutine::return_exception_ptr(std::move(err));
        }
        auto entry = co_await std::move(f);
        co_return to_address_type(entry.addr_entries);
    })
    , _hosts(std::move(hosts))
    , _listener(std::move(listener))
    , _refreshes_counter(refreshes_counter) {
}

void dns::start_background_tasks() {
    // start the background task to refresh the host address
    (void)try_with_gate(_tasks_gate, [this] {
        return refresh_addr_task();
    }).handle_exception([this](std::exception_ptr eptr) {
        on_internal_error_noexcept(_logger, fmt::format("The Vector Store Client refresh task failed: {}", eptr));
    });
}

// A task for refreshing the vector store http client.
seastar::future<> dns::refresh_addr_task() {
    for (;;) {
        auto exception_occurred = false;
        try {
            if (_abort_refresh.abort_requested()) {
                break;
            }

            // Do not refresh the service address too often
            auto now = seastar::lowres_clock::now();
            auto current_duration = now - _last_refresh;
            if (current_duration > _refresh_interval) {
                _last_refresh = now;
                co_await refresh_addr();
            } else {
                // Wait till the end of the refreshing interval
                if (co_await wait_for_timeout(_refresh_interval - current_duration, _abort_refresh)) {
                    continue;
                }
                // If the wait was aborted, we stop refreshing
                break;
            }

            if (_abort_refresh.abort_requested()) {
                break;
            }

            co_await _refresh_cv.when();
        } catch (const std::exception& e) {
            _logger.error("Vector Store Client refresh task failed: {}", e.what());
            exception_occurred = true;
        } catch (...) {
            _logger.error("Vector Store Client refresh task failed with unknown exception");
            exception_occurred = true;
        }
        if (exception_occurred) {
            // If an exception occurred, we wait for the next signal to refresh the address
            co_await wait_for_timeout(EXCEPTION_OCCURRED_WAIT, _abort_refresh);
        }
    }
}

seastar::future<> dns::refresh_addr() {
    host_address_map new_addrs;
    auto copy = _hosts;
    co_await coroutine::parallel_for_each(std::move(copy), [this, &new_addrs](const sstring& host) -> future<> {
        ++_refreshes_counter;
        new_addrs[host] = co_await _resolver(host);
    });
    if (new_addrs != _addresses) {
        _addresses = new_addrs;
        co_await _listener(_addresses);
    }
}

} // namespace vector_search
