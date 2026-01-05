/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/http/client.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/tls.hh>

#include "seastarx.hh"
#include "utils/log.hh"

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

    static future<shared_ptr<tls::certificate_credentials>> system_trust_credentials();

    // This method can out-live the factory instance, in case `make()` is never called before the instance is destroyed.
    static future<> initialize(lw_shared_ptr<state> state, std::string host, int port, bool use_https, logging::logger& logger);

public:
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger);

    virtual future<connected_socket> make(abort_source*) override;
};

} // namespace utils::http
