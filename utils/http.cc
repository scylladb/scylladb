/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

/*
 * Copyright (C) 2025-present ScyllaDB
 */

#include "http.hh"
#include "rest/client.hh"

#include <boost/regex.hpp>
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

static logging::logger dap("dns_address_provider");

utils::http::address_provider::address_provider(const std::string& host, bool use_https, shared_ptr<tls::certificate_credentials> creds)
    : _creds(std::move(creds)), _host(host), _use_https(use_https), _addr_update_timer([this] {
        std::ignore = [this]() -> future<> {
            dap.debug("Host resolution expired, re-resolving host {}", _host);
            co_await init_addresses();
        }();
    }) {
    _addr_update_timer.arm(lowres_clock::now());
}

utils::http::address_provider::~address_provider() {
    _addr_update_timer.cancel();
}

future<> utils::http::address_provider::init_addresses() {
    auto units = co_await get_units(_addr_sem, 1);
    auto hent = co_await net::dns::get_host_by_name(_host, net::inet_address::family::INET);
    _address_ttl_seconds = *std::ranges::min_element(hent.addr_ttls);
    _addr_list = std::move(hent.addr_list);
    dap.debug("Initialized addresses={}", _addr_list);

    if (_address_ttl_seconds == 0) {
        // Zero values are interpreted to mean that the Resource
        // Record can only be used for the transaction in progress,
        // and should not be cached.
        // https://datatracker.ietf.org/doc/html/rfc1035#section-3.2.1
        co_return;
    }
    _addr_update_timer.rearm(lowres_clock::now() + std::chrono::milliseconds(std::max(1ul, _address_ttl_seconds * 1000)));
}

future<> utils::http::address_provider::init_credentials() {
    if (_use_https && !_creds) {
        _creds = co_await system_trust_credentials();
    }
    if (!_use_https) {
        _creds = {};
    }
    dap.debug("Initialized credentials, tls={}", _creds == nullptr ? "no" : "yes");
}

future<net::inet_address> utils::http::address_provider::get_address() {

    if (!_addr_init || !_addr_update_timer.armed())  {
        co_await init_addresses();
        _addr_init = true;
    }
    co_return _addr_list[_addr_pos++ % _addr_list.size()];
}

future<shared_ptr<tls::certificate_credentials>> utils::http::address_provider::get_creds() {
    if (!_creds_init) [[unlikely]] {
        co_await init_credentials();
        _creds_init = true;
    }
    co_return _creds;
}

future<> utils::http::address_provider::reset() {
    co_await init_addresses();
}

future<connected_socket> utils::http::dns_connection_factory::connect() {
    auto socket_addr = socket_address(co_await _addr_provider.get_address(), _port);
    if (auto creds = co_await _addr_provider.get_creds()) {
        _logger.debug("Making new HTTPS connection addr={} host={}", socket_addr, _host);
        co_return co_await tls::connect(creds, socket_addr, tls::tls_options{.server_name = _host});
    }
    _logger.debug("Making new HTTP connection addr={} host={}", socket_addr, _host);
    co_return co_await seastar::connect(socket_addr, {}, transport::TCP);
}

utils::http::dns_connection_factory::dns_connection_factory(dns_connection_factory&&) = default;

utils::http::dns_connection_factory::dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs)
    : _host(std::move(host))
    , _port(port)
    , _logger(logger)
    , _addr_provider(_host, use_https, std::move(certs))
{}

utils::http::dns_connection_factory::dns_connection_factory(std::string uri, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs) 
    : dns_connection_factory([&] {
        auto url = parse_simple_url(uri);
        if (!url.path.empty()) {
            throw std::invalid_argument("Cannot handle path in URI: " + uri);
        }
        return dns_connection_factory(url.host, url.port, url.is_https(), logger, std::move(certs));
    }())
{}

future<connected_socket> utils::http::dns_connection_factory::make(abort_source*) {
    try {
        co_return co_await connect();
    } catch (...) {
        // On failure, reset the address provider and try again
        _logger.debug("Connection failed, resetting address provider and retrying: {}", std::current_exception());
    }
    co_await _addr_provider.reset();
    co_return co_await connect();
}

static const char HTTPS[] = "https";

utils::http::url_info utils::http::parse_simple_url(std::string_view uri) {
    /**
     * https://en.wikipedia.org/wiki/IPv6#Addressing
     * In case a port is included with a numerical ipv6 address, 
     * the address part is encases in a "[]" wrapper, like
     * http://[2001:db8:4006:812::200e]:8080
     */
    static boost::regex simple_url(R"foo(([a-zA-Z]+):\/\/((?:\[[^\]]+\])|[^\/:]+)(:\d+)?(\/.*)?)foo");

    boost::smatch m;
    std::string tmp(uri);

    if (!boost::regex_match(tmp, m, simple_url)) {
        throw std::invalid_argument(fmt::format("Could not parse URI {}", uri));
    }

    auto scheme = m[1].str();
    auto host = m[2].str();
    auto port = m[3].str();
    auto path = m[4].str();

    bool https = (strcasecmp(scheme.c_str(), HTTPS) == 0);

    // check for numeric ipv6 address + port case
    if (host.size() > 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    return url_info {
        .scheme = std::move(scheme),
        .host = std::move(host),
        .path = std::move(path),
        .port = uint16_t(port.empty() ? (https ? 443 : 80) : std::stoi(port.substr(1)))
    };
}

bool utils::http::url_info::is_https() const {
    return strcasecmp(scheme.c_str(), HTTPS) == 0;
}

