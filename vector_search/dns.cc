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

} // namespace

dns::dns(logging::logger& logger, std::vector<seastar::sstring> hosts, listener_type listener)
    : vslogger(logger)
    , _refresh_interval(DNS_REFRESH_INTERVAL)
    , _resolver([](auto const& host) -> future<address_type> {
        auto f = co_await coroutine::as_future(net::dns::get_host_by_name(host));

        if (f.failed()) {
            auto err = f.get_exception();
            if (try_catch<std::system_error>(err) != nullptr) {
                co_return address_type{};
            }
            co_await coroutine::return_exception_ptr(std::move(err));
        }
        auto addr = co_await std::move(f);
        co_return addr.addr_list;
    })
    , _hosts(std::move(hosts))
    , _listener(std::move(listener)) {
}

void dns::start_background_tasks() {
    // start the background task to refresh the host address
    (void)try_with_gate(tasks_gate, [this] {
        return refresh_addr_task();
    }).handle_exception([this](std::exception_ptr eptr) {
        on_internal_error_noexcept(vslogger, fmt::format("The Vector Store Client refresh task failed: {}", eptr));
    });
}

// A task for refreshing the vector store http client.
seastar::future<> dns::refresh_addr_task() {
    for (;;) {
        auto exception_occurred = false;
        try {
            if (abort_refresh.abort_requested()) {
                break;
            }

            // Do not refresh the service address too often
            auto now = seastar::lowres_clock::now();
            auto current_duration = now - last_refresh;
            if (current_duration > _refresh_interval) {
                last_refresh = now;
                co_await refresh_addr();
            } else {
                // Wait till the end of the refreshing interval
                if (co_await wait_for_timeout(_refresh_interval - current_duration, abort_refresh)) {
                    continue;
                }
                // If the wait was aborted, we stop refreshing
                break;
            }

            if (abort_refresh.abort_requested()) {
                break;
            }

            co_await refresh_cv.when();
        } catch (const std::exception& e) {
            vslogger.error("Vector Store Client refresh task failed: {}", e.what());
            exception_occurred = true;
        } catch (...) {
            vslogger.error("Vector Store Client refresh task failed with unknown exception");
            exception_occurred = true;
        }
        if (exception_occurred) {
            // If an exception occurred, we wait for the next signal to refresh the address
            co_await wait_for_timeout(EXCEPTION_OCCURRED_WAIT, abort_refresh);
        }
    }
}

seastar::future<> dns::refresh_addr() {
    host_address_map new_addrs;
    auto copy = _hosts;
    co_await coroutine::parallel_for_each(std::move(copy), [this, &new_addrs](const sstring& host) -> future<> {
        new_addrs[host] = co_await _resolver(host);
    });
    if (new_addrs != _addresses) {
        _addresses = new_addrs;
        co_await _listener(_addresses);
    }
}

} // namespace vector_search
