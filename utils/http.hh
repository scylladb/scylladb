/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/http/client.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/tls.hh>

#include "seastarx.hh"
#include "log.hh"

namespace utils::http {

class dns_connection_factory : public seastar::http::experimental::connection_factory {
protected:
    std::string _host;
    int _port;
    logging::logger& _logger;
    struct state {
        bool initialized = false;
        socket_address addr;
        ::shared_ptr<tls::certificate_credentials> creds;
    };
    lw_shared_ptr<state> _state;
    shared_future<> _done;

    future<> initialize(bool use_https) {
        auto state = _state;

        co_await coroutine::all(
            [state, host = _host, port = _port] () -> future<> {
                auto hent = co_await net::dns::get_host_by_name(host, net::inet_address::family::INET);
                state->addr = socket_address(hent.addr_list.front(), port);
            },
            [state, use_https] () -> future<> {
                if (use_https) {
                    tls::credentials_builder cbuild;
                    co_await cbuild.set_system_trust();
                    state->creds = cbuild.build_certificate_credentials();
                }
            }
        );

        state->initialized = true;
        _logger.debug("Initialized factory, address={} tls={}", state->addr, state->creds == nullptr ? "no" : "yes");
    }

public:
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger)
        : _host(std::move(host))
        , _port(port)
        , _logger(logger)
        , _state(make_lw_shared<state>())
        , _done(initialize(use_https))
    {
    }

    virtual future<connected_socket> make() override {
        if (!_state->initialized) {
            _logger.debug("Waiting for factory to initialize");
            co_await _done.get_future();
        }

        if (_state->creds) {
            _logger.debug("Making new HTTPS connection addr={} host={}", _state->addr, _host);
            co_return co_await tls::connect(_state->creds, _state->addr, tls::tls_options{.server_name = _host});
        } else {
            _logger.debug("Making new HTTP connection");
            co_return co_await seastar::connect(_state->addr, {}, transport::TCP);
        }
    }
};

} // namespace utils::http
