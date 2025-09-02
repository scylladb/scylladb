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

utils::http::dns_connection_factory::state::state(shared_ptr<tls::certificate_credentials> cin) 
    : creds(std::move(cin))
{}

future<> utils::http::dns_connection_factory::initialize(lw_shared_ptr<state> state, std::string host, int port, bool use_https, logging::logger& logger) {
    co_await coroutine::all(
        [state, host, port] () -> future<> {
            auto hent = co_await net::dns::get_host_by_name(host, net::inet_address::family::INET);
            state->addr = socket_address(hent.addr_list.front(), port);
        },
        [state, use_https] () -> future<> {
            if (use_https && !state->creds) {
                state->creds = co_await system_trust_credentials();
            }
            if (!use_https) {
                state->creds = {};
            }
        }
    );

    state->initialized = true;
    logger.debug("Initialized factory, address={} tls={}", state->addr, state->creds == nullptr ? "no" : "yes");
}

utils::http::dns_connection_factory::dns_connection_factory(dns_connection_factory&&) = default;

utils::http::dns_connection_factory::dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs)
    : _host(std::move(host))
    , _port(port)
    , _logger(logger)
    , _state(make_lw_shared<state>(std::move(certs)))
    , _done(initialize(_state, _host, _port, use_https, _logger))
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
    if (!_state->initialized) {
        _logger.debug("Waiting for factory to initialize");
        co_await _done.get_future();
    }

    if (_state->creds) {
        _logger.debug("Making new HTTPS connection addr={} host={}", _state->addr, _host);
        co_return co_await tls::connect(_state->creds, _state->addr, tls::tls_options{.server_name = _host});
    } else {
        _logger.debug("Making new HTTP connection addr={} host={}", _state->addr, _host);
        co_return co_await seastar::connect(_state->addr, {}, transport::TCP);
    }
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

