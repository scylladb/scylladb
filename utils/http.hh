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

future<shared_ptr<tls::certificate_credentials>> system_trust_credentials();

class address_provider {
    bool _initialized = false;
    std::vector<net::inet_address> addr_list;
    shared_ptr<tls::certificate_credentials> _creds;
    const std::string& _host;
    size_t _addr_pos{0};
    bool _use_https;
    shared_future<> _ready;

    future<> initialize();

public:
    address_provider(const std::string& host, bool use_https);

    future<net::inet_address> get_address();
    shared_ptr<tls::certificate_credentials> get_creds() const;
    future<> reset();
};

class dns_connection_factory : public seastar::http::experimental::connection_factory {
protected:
    std::string _host;
    int _port;
    size_t _addr_pos = 0;
    logging::logger& _logger;
    address_provider _addr_provider;

    future<connected_socket> connect();

public:
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger);

    virtual future<connected_socket> make(abort_source*) override;
};

} // namespace utils::http
