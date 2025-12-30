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
    class address_provider : enable_lw_shared_from_this<address_provider> {
        shared_ptr<tls::certificate_credentials> _creds;
        const std::string& _host;
        std::vector<net::inet_address> addr_list;
        size_t _addr_pos{0};
        bool _use_https;
        bool _initialized{false};
        shared_future<> _ready;

        future<> init_addresses();
        future<> init_credentials();

    public:
        address_provider(const std::string& host, bool use_https, shared_ptr<tls::certificate_credentials> creds);

        future<net::inet_address> get_address();
        shared_ptr<tls::certificate_credentials> get_creds() const;
        future<> reset();
    };
protected:
    std::string _host;
    int _port;
    logging::logger& _logger;
    address_provider _addr_provider;

    future<connected_socket> connect();
public:
    dns_connection_factory(dns_connection_factory&&);
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> = {});
    dns_connection_factory(std::string endpoint_url, logging::logger& logger, shared_ptr<tls::certificate_credentials> = {});
    future<> reset_dns_resolution();

    virtual future<connected_socket> make(abort_source*) override;
};

// simple URL parser, just enough to handle required aspects for normal endpoint usage
// could use boost::url, but this requires additional libraries being added to 
// install-dependencies etc.
struct url_info {
    std::string scheme;
    std::string host;
    std::string path;
    uint16_t port;

    bool is_https() const;
};

url_info parse_simple_url(std::string_view uri);

} // namespace utils::http
