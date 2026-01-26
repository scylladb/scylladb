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

future<> utils::http::dns_connection_factory::init_credentials() {
    if (_use_https && !_creds) {
        _creds = co_await system_trust_credentials();
    }
    if (!_use_https) {
        _creds = {};
    }
    _logger.debug("Initialized credentials, tls={}", _creds == nullptr ? "no" : "yes");
}

future<net::inet_address> utils::http::dns_connection_factory::get_address() {
    auto get_addr = [this] -> net::inet_address {
        const auto& addresses = _addr_list.value();
        return addresses[_addr_pos++ % addresses.size()];
    };

    if (_addr_list) {
        co_return get_addr();
    }
    auto units = co_await get_units(_init_semaphore, 1);
    if (!_addr_list) {
        auto hent = co_await net::dns::get_host_by_name(_host, net::inet_address::family::INET);
        _address_ttl = std::ranges::min_element(hent.addr_entries, [](const net::hostent::address_entry& lhs, const net::hostent::address_entry& rhs) {
                           return lhs.ttl < rhs.ttl;
                       })->ttl;
        if (_address_ttl.count() == 0) {
            co_return hent.addr_entries[_addr_pos++ % hent.addr_entries.size()].addr;
        }
        _addr_list = hent.addr_entries | std::views::transform(&net::hostent::address_entry::addr) | std::ranges::to<std::vector>();
        _addr_update_timer.rearm(lowres_clock::now() + _address_ttl);
    }

    co_return get_addr();
}

future<shared_ptr<tls::certificate_credentials>> utils::http::dns_connection_factory::get_creds() {
    if (!_creds_init) [[unlikely]] {
        auto units = co_await get_units(_init_semaphore, 1);
        if (!_creds_init) {
            co_await init_credentials();
            _creds_init = true;
        }
    }
    co_return _creds;
}

future<connected_socket> utils::http::dns_connection_factory::connect(net::inet_address address) {
    auto socket_addr = socket_address(address, _port);
    if (auto creds = co_await get_creds()) {
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
    ,_creds(std::move(certs))
    , _use_https(use_https)
    , _addr_update_timer([this] {
        if (auto units = try_get_units(_init_semaphore, 1)) {
            _addr_list.reset();
        }
    }) {
    _addr_update_timer.arm(lowres_clock::now());
}

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
    auto address = co_await get_address();
    co_return co_await connect(address);
}

future<> utils::http::dns_connection_factory::close() {
    _addr_update_timer.cancel();
    co_await get_units(_init_semaphore, 1);
}

static const char HTTPS[] = "https";

utils::http::url_info utils::http::parse_simple_url(std::string_view uri) {
    static boost::regex simple_url(R"foo((https?):\/\/([^\/:]+)(:\d+)?(\/.*)?)foo");

    boost::smatch m;
    std::string tmp(uri);
    if (!boost::regex_match(tmp, m, simple_url)) {
        throw std::invalid_argument(fmt::format("Could not parse URI {}", uri));
    }

    auto scheme = m[1].str();
    auto host = m[2].str();
    auto port = m[3].str();
    auto path = m[4].str();

    bool https = scheme == HTTPS;

    return url_info {
        .scheme = std::move(scheme),
        .host = std::move(host),
        .path = std::move(path),
        .port = uint16_t(port.empty() ? (https ? 443 : 80) : std::stoi(port.substr(1)))
    };
}

bool utils::http::url_info::is_https() const {
    return scheme == HTTPS;
}

