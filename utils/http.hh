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

class dns_connection_factory : public seastar::http::experimental::connection_factory {
protected:
    std::string _host;
    int _port;
    logging::logger& _logger;
    semaphore _init_semaphore{1};
    bool _addr_init = false;
    bool _creds_init = false;
    std::vector<net::inet_address> _addr_list;
    shared_ptr<tls::certificate_credentials> _creds;
    uint16_t _addr_pos{0};
    bool _use_https;

    future<> init_addresses();
    future<> init_credentials();
    future<net::inet_address> get_address();
    future<shared_ptr<tls::certificate_credentials>> get_creds();
    future<connected_socket> connect();
public:
    dns_connection_factory(dns_connection_factory&&);
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> = {});

    virtual future<connected_socket> make(abort_source*) override;
};

} // namespace utils::http
