/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "clients.hh"
#include "load_balancer.hh"
#include "utils/exceptions.hh"
#include <random>
#include <expected>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>

using namespace seastar;

namespace vector_search {
namespace {

/// Timeout for waiting for a new client to be available
constexpr auto WAIT_FOR_CLIENT_TIMEOUT = std::chrono::seconds(5);

/// The number of times to retry a request if all nodes fail with an unavailable error.
constexpr auto REQUEST_RETRIES = 3;

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

/// The `endpoint` label of the per-endpoint metrics: the configured host and port.
auto endpoint_name(const seastar::sstring& host, std::uint16_t port) -> seastar::sstring {
    return seastar::format("{}:{}", host, port);
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

clients::clients(logging::logger& logger, seastar::sstring role, refresh_trigger_callback trigger_refresh,
        utils::updateable_value<uint32_t> unreachable_node_detection_time_in_ms, truststore& truststore)
    : _producer([&]() -> future<clients_vec> {
        return try_with_gate(_gate, [this] -> future<clients_vec> {
            _trigger_refresh();
            co_await wait_for_signal(_refresh_cv, lowres_clock::now() + _timeout);
            co_return _clients;
        });
    })
    , _trigger_refresh(std::move(trigger_refresh))
    , _timeout(WAIT_FOR_CLIENT_TIMEOUT)
    , _logger(logger)
    , _unreachable_node_detection_time_in_ms(std::move(unreachable_node_detection_time_in_ms))
    , _truststore(truststore)
    , _role(std::move(role)) {
}

future<clients::request_result> clients::request(
        seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as) {

    for (auto retries = 0; retries < REQUEST_RETRIES; ++retries) {
        auto clients = co_await get_clients(as);
        if (!clients) {
            co_return make_unexpected<clients::request_error>(clients.error());
        }

        load_balancer lb(std::move(*clients), random_engine);
        while (auto client = lb.next()) {
            auto result = co_await client->request(method, path, content, as);
            // Count every attempt, so an endpoint that fails while another one
            // answers is visible. The endpoint may have been removed meanwhile.
            if (auto* m = find_endpoint_metrics(client->endpoint())) {
                if (result) {
                    ++m->ok;
                } else if (std::holds_alternative<service_unavailable_error>(result.error())) {
                    ++m->unavailable;
                }
            }
            if (result) {
                co_return std::move(result.value());
            }
            if (!result && std::holds_alternative<service_unavailable_error>(result.error())) {
                // try next client
                continue;
            }
            co_return make_unexpected<clients::request_error>(result.error());
        }
        _trigger_refresh();
    }

    co_return std::unexpected{service_unavailable_error{}};
}

/// Get the current http client or wait for a new one to be available.
future<clients::get_clients_result> clients::get_clients(abort_source& as) {
    if (!_clients.empty()) {
        co_return _clients;
    }

    auto current_clients = co_await coroutine::as_future(_producer(as));

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
    for (auto& [_, m] : _endpoint_metrics) {
        m->addresses = 0;
    }
    for (const auto& uri : uris) {
        auto it = addrs.find(uri.host);
        if (it != addrs.end()) {
            if (auto m = _endpoint_metrics.find(endpoint_name(uri.host, uri.port)); m != _endpoint_metrics.end()) {
                m->second->addresses = it->second.size();
            }
            for (const auto& addr : it->second) {
                _clients.push_back(co_await make_client(uri, addr));
            }
        }
    }

    _refresh_cv.broadcast();
    co_await close_old_clients();
}

void clients::set_endpoints(const std::vector<uri>& uris) {
    std::map<seastar::sstring, std::unique_ptr<endpoint_metrics>> kept;
    for (const auto& uri : uris) {
        auto name = endpoint_name(uri.host, uri.port);
        if (kept.contains(name)) {
            continue;
        }
        auto it = _endpoint_metrics.find(name);
        if (it != _endpoint_metrics.end()) {
            kept.emplace(name, std::move(it->second));
        } else {
            auto m = std::make_unique<endpoint_metrics>();
            register_endpoint_metrics(name, *m);
            kept.emplace(name, std::move(m));
        }
    }
    // The metrics of the URIs left out are unregistered when they are destroyed here.
    _endpoint_metrics = std::move(kept);
}

void clients::register_endpoint_metrics(const seastar::sstring& endpoint, endpoint_metrics& m) {
    namespace sm = seastar::metrics;
    sm::label role_label("role");
    sm::label endpoint_label("endpoint");
    sm::label status_label("status");
    std::vector<sm::label_instance> labels{role_label(_role), endpoint_label(endpoint)};
    auto with_status = [&](std::string_view status) {
        auto l = labels;
        l.push_back(status_label(status));
        return l;
    };
    m.metrics.add_group("vector_store", {sm::make_counter("endpoint_requests",
            sm::description("Number of attempts to send a request to one Vector Store endpoint, by outcome. "
                            "A request that fails over to another endpoint is counted once for each endpoint it tried"),
            with_status("ok"), m.ok).aggregate({sm::shard_label})});
    m.metrics.add_group("vector_store", {sm::make_counter("endpoint_requests",
            sm::description("Number of attempts to send a request to one Vector Store endpoint, by outcome. "
                            "A request that fails over to another endpoint is counted once for each endpoint it tried"),
            with_status("unavailable"), m.unavailable).aggregate({sm::shard_label})});
    // Every shard resolves the same addresses, so the gauge is exported once, from shard 0.
    if (this_shard_id() != 0) {
        return;
    }
    m.metrics.add_group("vector_store", {sm::make_gauge("endpoint_addresses",
            sm::description("Number of addresses resolved for a configured Vector Store endpoint; 0 when its host does not resolve"),
            labels, [&m] {
                return m.addresses;
            })});
}

clients::endpoint_metrics* clients::find_endpoint_metrics(const client::endpoint_type& endpoint) {
    auto it = _endpoint_metrics.find(endpoint_name(endpoint.host, endpoint.port));
    return it == _endpoint_metrics.end() ? nullptr : it->second.get();
}

future<> clients::stop() {
    _refresh_cv.signal();
    co_await _gate.close();
    co_await close_clients();
}

void clients::clear() {
    _old_clients.insert(_old_clients.end(), std::make_move_iterator(_clients.begin()), std::make_move_iterator(_clients.end()));
    _clients.clear();
}

future<> clients::close_clients() {
    for (auto& client : _clients) {
        co_await client->close();
    }
    for (auto& client : _old_clients) {
        if (client) {
            co_await client->close();
        }
    }
    _clients.clear();
    _old_clients.clear();
}

future<> clients::close_old_clients() {
    // iterate over old clients and close them. There is a co_await in the loop
    // so we need to use [] accessor and copying clients to avoid dangling references of iterators.
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (auto it = 0U; it < _old_clients.size(); ++it) {
        auto& client = _old_clients[it];
        if (client && client.owned()) {
            auto client_cloned = client;
            client = nullptr;
            co_await client_cloned->close();
        }
    }
    std::erase_if(_old_clients, [](auto const& client) {
        return !client;
    });
}

seastar::future<seastar::lw_shared_ptr<client>> clients::make_client(const uri& uri_, const seastar::net::inet_address& addr_) {
    auto creds = uri_.schema == uri::schema_type::https ? co_await _truststore.get() : nullptr;
    auto c = make_lw_shared<client>(_logger, client::endpoint_type{uri_.host, uri_.port, addr_}, _unreachable_node_detection_time_in_ms, creds);
    co_return c;
}

} // namespace vector_search
