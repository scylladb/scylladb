/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "client.hh"
#include "dns.hh"
#include "seastar/core/future.hh"
#include <seastar/core/sstring.hh>
#include <vector>
#include <cstdint>
#include <boost/noncopyable.hpp>

namespace service::vector_search {

class high_availability : private boost::noncopyable {
public:
    struct uri {
        seastar::sstring host;
        std::uint16_t port;
    };

    using dns_resolver = std::function<seastar::future<std::optional<seastar::net::inet_address>>(seastar::sstring const&)>;

    explicit high_availability(dns_resolver resolver)
        : _dns(std::move(resolver)) {
    }

    seastar::future<client::ann_result> ann(
            seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source* as);

    seastar::future<> set_uri(std::optional<uri> uri);

    void set_dns_resolver(dns_resolver resolver) {
        _dns.set_resolver(resolver);
    }

    void set_dns_refresh_interval(std::chrono::milliseconds interval) {
        _dns.set_refresh_interval(interval);
    }

    seastar::future<> stop();

private:
    seastar::future<> refresh_client_address(seastar::abort_source* as);
    seastar::future<seastar::lw_shared_ptr<client>> get_client(seastar::abort_source* as);

    std::optional<uri> _uri;
    seastar::lw_shared_ptr<client> _client;
    dns _dns;
};

} // namespace service::vector_search
