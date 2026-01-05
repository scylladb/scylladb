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

utils::http::address_provider::address_provider(const std::string& host, bool use_https) : _host(host), _use_https(use_https) {
    _ready = [this] -> future<> {
        co_await coroutine::all([this] -> future<> { co_await init_addresses(); }, [this] -> future<> { co_await init_credentials(); });
        _initialized = true;
    }();
}

future<> utils::http::address_provider::init_addresses() {
    auto hent = co_await net::dns::get_host_by_name(_host, net::inet_address::family::INET);
    addr_list = std::move(hent.addr_list);
}

future<> utils::http::address_provider::init_credentials() {
    if (_use_https) {
        _creds = co_await system_trust_credentials();
    }
}

future<net::inet_address> utils::http::address_provider::get_address() {
    if (!_initialized) {
        co_await _ready.get_future();
    }
    co_return addr_list[_addr_pos++ % addr_list.size()];
}

shared_ptr<tls::certificate_credentials> utils::http::address_provider::get_creds() const {
    return _creds;
}

future<> utils::http::address_provider::reset() {
    co_await init_addresses();
}

future<connected_socket> utils::http::dns_connection_factory::connect() {
    auto socket_addr = socket_address(co_await _addr_provider.get_address(), _port);
    if (auto creds = _addr_provider.get_creds()) {
        _logger.debug("Making new HTTPS connection addr={} host={}", socket_addr, _host);
        co_return co_await tls::connect(creds, socket_addr, tls::tls_options{.server_name = _host});
    }
    _logger.debug("Making new HTTP connection addr={} host={}", socket_addr, _host);
    co_return co_await seastar::connect(socket_addr, {}, transport::TCP);
}

utils::http::dns_connection_factory::dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger)
    : _host(std::move(host)), _port(port), _logger(logger), _addr_provider(_host, use_https) {
}
future<connected_socket> utils::http::dns_connection_factory::make(abort_source*) {
    co_return co_await connect();
}
