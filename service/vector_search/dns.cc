/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dns.hh"
#include <seastar/core/coroutine.hh>

namespace service::vector_search {

dns::dns(dns_resolver resolver)
    : _resolver(std::move(resolver)) {
}

seastar::future<std::optional<seastar::net::inet_address>> dns::resolve(seastar::sstring host) {
    auto now = seastar::lowres_clock::now();
    auto current_duration = now - _last_refresh;
    if (current_duration >= _refresh_interval) {
        _last_refresh = now;
        _addr = co_await _resolver(host);
    }
    co_return _addr;
}

} // namespace service::vector_search
