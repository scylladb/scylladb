/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client_manager.hh"
#include "utils/exceptions.hh"
#include <seastar/coroutine/as_future.hh>

using namespace seastar;

namespace vector_search {
namespace {

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

std::vector<sstring> get_hosts(const std::vector<client_manager::uri>& primary, const std::vector<client_manager::uri>& secondary) {
    std::vector<sstring> ret;
    for (const auto& uri : primary) {
        ret.push_back(uri.host);
    }
    for (const auto& uri : secondary) {
        ret.push_back(uri.host);
    }
    return ret;
}

} // namespace

client_manager::client_manager(std::vector<uri> primary, std::vector<uri> secondary, logging::logger& logger, uint64_t& refreshes_counter)
    : _primary_uris(std::move(primary))
    , _secondary_uris(std::move(secondary))
    , _refresh_clients([&]() -> future<> {
        return try_with_gate(_client_producer_gate, [this] -> future<> {
            trigger_refresh();
            co_await wait_for_signal(_refresh_client_cv, lowres_clock::now() + _timeout);
        });
    })
    , _dns(
              logger, get_hosts(_primary_uris, _secondary_uris),
              [this](auto const& addrs) -> future<> {
                  co_await handle_addresses_changed(addrs);
              },
              refreshes_counter) {
}

void client_manager::update_uris(std::vector<uri> primary, std::vector<uri> secondary) {
    _primary_uris = std::move(primary);
    _secondary_uris = std::move(secondary);
    _dns.hosts(get_hosts(_primary_uris, _secondary_uris));
}

auto client_manager::get_primary_clients(abort_source& as) -> future<clients_type> {
    return get_or_refresh_if_empty(_primary_clients, as);
}

auto client_manager::get_secondary_clients(abort_source& as) -> future<clients_type> {
    return get_or_refresh_if_empty(_secondary_clients, as);
}

void client_manager::trigger_refresh() {
    _dns.trigger_refresh();
}

auto client_manager::handle_addresses_changed(const dns::host_address_map& addrs) -> future<> {
    auto populate_clients = [&](clients_type& clients, const std::vector<uri>& uris) {
        for (const auto& uri : uris) {
            if (auto it = addrs.find(uri.host); it != addrs.end()) {
                for (const auto& addr : it->second) {
                    clients.push_back(make_lw_shared<node>(client::endpoint_type{uri.host, uri.port, addr}));
                }
            }
        }
    };

    clear();
    populate_clients(_primary_clients, _primary_uris);
    populate_clients(_secondary_clients, _secondary_uris);

    _refresh_client_cv.broadcast();
    co_await cleanup_old_clients();
}

auto client_manager::get_or_refresh_if_empty(const clients_type& clients, abort_source& as) -> future<clients_type> {
    if (!clients.empty()) {
        co_return clients;
    }

    auto fut = co_await coroutine::as_future(_refresh_clients(as));
    if (fut.failed()) {
        auto err = fut.get_exception();
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    co_return clients;
}

void client_manager::clear() {
    auto move_to_old = [&](clients_type& clients) {
        _old_clients.insert(_old_clients.end(), std::make_move_iterator(clients.begin()), std::make_move_iterator(clients.end()));
        clients.clear();
    };
    move_to_old(_primary_clients);
    move_to_old(_secondary_clients);
}

auto client_manager::cleanup_clients() -> future<> {
    auto close_and_clear = [](clients_type& clients) -> future<> {
        for (auto& client : clients) {
            co_await client->close();
        }
        clients.clear();
    };

    co_await close_and_clear(_primary_clients);
    co_await close_and_clear(_secondary_clients);
}

auto client_manager::cleanup_old_clients() -> future<> {
    // iterate over old clients and close them. There is a co_await in the loop
    // so we need to use [] accessor and copying clients to avoid dangling references of iterators.
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (auto it = 0U; it < _old_clients.size(); ++it) {
        auto& client = _old_clients[it];
        if (client && client.owned()) {
            auto client_cloned = client;
            co_await client_cloned->close();
            client_cloned = nullptr;
        }
    }
    std::erase_if(_old_clients, [](auto const& client) {
        return !client;
    });
}

void client_manager::start_background_tasks() {
    _dns.start_background_tasks();
}

auto client_manager::stop() -> future<> {
    _refresh_client_cv.signal();
    co_await _client_producer_gate.close();
    co_await _dns.stop();
    co_await cleanup_old_clients();
    co_await cleanup_clients();
}

bool client_manager::has_primay() const {
    return !_primary_uris.empty();
}

bool client_manager::has_secondary() const {
    return !_secondary_uris.empty();
}

bool client_manager::empty() const {
    return _primary_uris.empty() && _secondary_uris.empty();
}

} // namespace vector_search
