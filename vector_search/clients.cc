/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "clients.hh"
#include "load_balancer.hh"
#include "utils/exceptions.hh"
#include <random>
#include <expected>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/lowres_clock.hh>

using namespace seastar;

namespace vector_search {
namespace {

/// Timeout for waiting for a new client to be available
constexpr auto WAIT_FOR_CLIENT_TIMEOUT = std::chrono::seconds(5);

/// The number of times to retry an /ann request if all nodes fail with a system error.
constexpr auto ANN_RETRIES = 3;

static thread_local auto random_engine = std::default_random_engine(std::random_device{}());

/// Wait for a condition variable to be signaled or timeout.
auto wait_for_signal(condition_variable& cv, lowres_clock::time_point timeout) -> future<void> {
    auto result = co_await coroutine::as_future(cv.wait(timeout));
    if (result.failed()) {
        auto err = result.get_exception();
        if (try_catch<condition_variable_timed_out>(err) != nullptr) {
            co_return;
        }
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    co_return;
}

template <typename Variant>
auto make_unexpected(const auto& err) {
    return std::unexpected{std::visit(
            [](auto&& err) {
                return Variant{err};
            },
            err)};
};

} // namespace

clients::clients(refresh_trigger_callback trigger_refresh)
    : producer_([&]() -> future<clients_vec> {
        return try_with_gate(gate_, [this] -> future<clients_vec> {
            trigger_refresh_();
            co_await wait_for_signal(refresh_cv_, lowres_clock::now() + timeout_);
            co_return clients_;
        });
    })
    , trigger_refresh_(std::move(trigger_refresh))
    , timeout_(WAIT_FOR_CLIENT_TIMEOUT) {
}

future<clients::request_result> clients::request(
        seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as) {

    for (auto retries = 0; retries < ANN_RETRIES; ++retries) {
        auto clients = co_await get_clients(as);
        if (!clients) {
            co_return make_unexpected<clients::request_error>(clients.error());
        }

        load_balancer lb(std::move(*clients), random_engine);
        while (auto client = lb.next()) {
            auto result = co_await client->request(method, path, content, as);
            if (result) {
                co_return std::move(result.value());
            }
            if (!result && std::holds_alternative<service_unavailable_error>(result.error())) {
                // try next client
                continue;
            }
            co_return make_unexpected<clients::request_error>(result.error());
        }
        trigger_refresh_();
    }

    co_return std::unexpected{service_unavailable_error{}};
}

/// Get the current http client or wait for a new one to be available.
future<clients::get_clients_result> clients::get_clients(abort_source& as) {
    if (!clients_.empty()) {
        co_return clients_;
    }

    auto current_clients = co_await coroutine::as_future(producer_(as));

    if (current_clients.failed()) {
        auto err = current_clients.get_exception();
        if (as.abort_requested()) {
            co_return std::unexpected{aborted_error{}};
        }
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    auto clients = co_await std::move(current_clients);
    if (clients.empty()) {
        co_return std::unexpected{addr_unavailable_error{}};
    }
    co_return clients;
}

future<> clients::handle_changed(const std::vector<uri>& uris, const dns::host_address_map& addrs) {
    clear();
    for (const auto& uri : uris) {
        auto it = addrs.find(uri.host);
        if (it != addrs.end()) {
            for (const auto& addr : it->second) {
                clients_.push_back(make_lw_shared<client>(client::endpoint_type{uri.host, uri.port, addr}));
            }
        }
    }

    refresh_cv_.broadcast();
    co_await close_old_clients();
}

future<> clients::stop() {
    refresh_cv_.signal();
    co_await gate_.close();
    co_await close_clients();
    co_await close_old_clients();
}

void clients::clear() {
    old_clients_.insert(old_clients_.end(), std::make_move_iterator(clients_.begin()), std::make_move_iterator(clients_.end()));
    clients_.clear();
}

future<> clients::close_clients() {
    for (auto& client : clients_) {
        co_await client->close();
    }
    clients_.clear();
}

future<> clients::close_old_clients() {
    // iterate over old clients and close them. There is a co_await in the loop
    // so we need to use [] accessor and copying clients to avoid dangling references of iterators.
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (auto it = 0U; it < old_clients_.size(); ++it) {
        auto& client = old_clients_[it];
        if (client && client.owned()) {
            auto client_cloned = client;
            client = nullptr;
            co_await client_cloned->close();
        }
    }
    std::erase_if(old_clients_, [](auto const& client) {
        return !client;
    });
}

} // namespace vector_search
