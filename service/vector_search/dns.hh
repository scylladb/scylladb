/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <functional>
#include <optional>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/core/lowres_clock.hh>
#include <boost/noncopyable.hpp>

namespace service::vector_search {

// Ensures not to query the dns server too often.
class dns : private boost::noncopyable {
public:
    using dns_resolver = std::function<seastar::future<std::optional<seastar::net::inet_address>>(seastar::sstring const&)>;

    explicit dns(dns_resolver resolver);

    seastar::future<std::optional<seastar::net::inet_address>> resolve(seastar::sstring host);

    void set_resolver(dns_resolver resolver) {
        _resolver = std::move(resolver);
    }

    void set_refresh_interval(std::chrono::milliseconds interval) {
        _refresh_interval = interval;
    }

private:
    dns_resolver _resolver;
    seastar::lowres_clock::time_point _last_refresh;
    std::optional<seastar::net::inet_address> _addr;
    std::chrono::milliseconds _refresh_interval{std::chrono::seconds(5)};
};

} // namespace service::vector_search
