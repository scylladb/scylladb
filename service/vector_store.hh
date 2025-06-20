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
#include <seastar/http/reply.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/inet_address.hh>
#include <expected>

class schema;

namespace cql3::statements {
class primary_key;
}

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
    using embedding = std::vector<float>;
    using host_name = sstring;
    using index_name = sstring;
    using keyspace_name = sstring;
    using limit = std::size_t;
    using port_number = std::uint16_t;
    using primary_key = cql3::statements::primary_key;
    using primary_keys = std::vector<primary_key>;
    using schema_ptr = lw_shared_ptr<schema const>;
    using status_type = http::reply::status_type;
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
    /// The vector-store service is disabled.
    struct disabled {};

    /// The vector-store addr is unavailable (not possible to get an addr from the dns service).
    struct addr_unavailable {};

    /// The vector-store service is unavailable.
    struct service_unavailable {};

    /// The error from the vector-store service.
    struct service_error {
        status_type status; ///< The HTTP status code from the vector-store service.
    };

    /// An unsupported reply format from the vector-store service.
    struct service_reply_format_error {};

    using ann_error = std::variant<disabled, addr_unavailable, service_unavailable, service_error, service_reply_format_error>;

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

    /// Request the vector store service for the primary keys of the nearest neighbors
    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit) -> future<std::expected<primary_keys, ann_error>>;

private:
    /// Refresh the vector store service IP address from the dns name. Returns true if the address was refreshed.
    auto refresh_service_addr() -> future<bool>;
};

} // namespace service

