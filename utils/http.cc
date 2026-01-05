/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

/*
 * Copyright (C) 2025-present ScyllaDB
 */

#include "http.hh"
#include <seastar/coroutine/all.hh>

future<shared_ptr<tls::certificate_credentials>> utils::http::system_trust_credentials() {
    static thread_local shared_ptr<tls::certificate_credentials> system_trust_credentials;
    if (!system_trust_credentials) {
        // can race, and overwrite the object. that is fine.
        auto cred = make_shared<tls::certificate_credentials>();
        co_await cred->set_system_trust();
        system_trust_credentials = std::move(cred);
    }
    co_return system_trust_credentials;
}
future<> utils::http::dns_connection_factory::initialize(lw_shared_ptr<address_provider> state, std::string host, bool use_https, logging::logger& logger) {
    co_await coroutine::all(
        [state, host]() -> future<> {
            auto hent = co_await net::dns::get_host_by_name(host, net::inet_address::family::INET);
            state->addr_list = std::move(hent.addr_list);
        },
        [state, use_https]() -> future<> {
            if (use_https) {
                state->creds = co_await system_trust_credentials();
            }
        });

    state->initialized = true;
    logger.debug("Initialized factory, address={} tls={}", state->addr_list, state->creds == nullptr ? "no" : "yes");
}

future<connected_socket> utils::http::dns_connection_factory::connect() {
    auto socket_addr = socket_address(_state->addr_list[_addr_pos++ % _state->addr_list.size()], _port);
    if (_state->creds) {
        _logger.debug("Making new HTTPS connection addr={} host={}", _state->addr_list, _host);
        co_return co_await tls::connect(_state->creds, socket_addr, tls::tls_options{.server_name = _host});
    }
    _logger.debug("Making new HTTP connection addr={} host={}", _state->addr_list, _host);
    co_return co_await seastar::connect(socket_addr, {}, transport::TCP);
}

utils::http::dns_connection_factory::dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger)
    : _host(std::move(host)), _port(port), _logger(logger), _state(make_lw_shared<address_provider>()), _done(initialize(_state, _host, use_https, _logger)) {
}
future<connected_socket> utils::http::dns_connection_factory::make(abort_source*) {
    if (!_state->initialized) {
        _logger.debug("Waiting for factory to initialize");
        co_await _done.get_future();
    }

    co_return co_await connect();
}
