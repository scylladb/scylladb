/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/inet_address.hh>

namespace db {
class config;
}

namespace seastar::http::experimental {
class client;
}

namespace service {

/// An interface with the vector store service.
class vector_store final {
public:
    using config = db::config;
    using host_name = sstring;
    using port_number = std::uint16_t;
    using time_point = lowres_system_clock::time_point;

private:
    using client = http::experimental::client;
    using inet_address = seastar::net::inet_address;

    lw_shared_ptr<client> _client;                   ///< The currect http client
    std::vector<lw_shared_ptr<client>> _old_clients; ///< The old http clients
    host_name _host;                                 ///< The host name for the vector-store service.
    port_number _port{};                             ///< The port number for the vector-store service.
    inet_address _addr;                              ///< The address for the vector-store service.
    time_point _last_dns_refresh;                    ///< The last time the DNS service was refreshed to get the vector-store service address.

public:
    explicit vector_store(config const& cfg);
    ~vector_store();

    /// Stop the service.
    auto stop() -> future<>;

    /// Check if the vector store service is disabled.
    auto is_disabled() const -> bool {
        return _host.empty();
    }

    [[nodiscard]] auto host() const -> host_name const& {
        return _host;
    }
    [[nodiscard]] auto port() const {
        return _port;
    }

private:
    /// Refresh the vector store service IP address from the dns name. Returns true if the address was refreshed.
    auto refresh_service_addr() -> future<bool>;
};

} // namespace service

