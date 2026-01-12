/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/seastar.hh>
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
    bool _creds_init = false;
    std::optional<std::vector<net::inet_address>> _addr_list;
    shared_ptr<tls::certificate_credentials> _creds;
    uint16_t _addr_pos{0};
    bool _use_https;
    std::chrono::seconds _address_ttl{0};
    timer<lowres_clock> _addr_update_timer;

    future<> init_credentials();
    future<net::inet_address> get_address();
    future<shared_ptr<tls::certificate_credentials>> get_creds();
    future<connected_socket> connect(net::inet_address address);
public:
    dns_connection_factory(dns_connection_factory&&) = default;
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> = {});
    dns_connection_factory(std::string endpoint_url, logging::logger& logger, shared_ptr<tls::certificate_credentials> = {});

    virtual future<connected_socket> make(abort_source*) override;
    future<> close() override;
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
