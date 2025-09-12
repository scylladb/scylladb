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
#include <expected>

class schema;

namespace cql3::statements {
class primary_key;
}

namespace db {
class config;
}

namespace seastar::net {
class inet_address;
}

namespace service {

/// A client with the vector-store service.
class vector_store_client final {
    struct impl;
    std::unique_ptr<impl> _impl;

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

    /// The vector_store_client service is disabled.
    struct disabled {};

    /// The operation was aborted.
    struct aborted {};

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

    using ann_error = std::variant<disabled, aborted, addr_unavailable, service_unavailable, service_error, service_reply_format_error>;

    struct ann_error_visitor {
        sstring operator()(service::vector_store_client::service_error e) const {
            return fmt::format("Vector Store error: HTTP status {}", e.status);
        }
        sstring operator()(service::vector_store_client::disabled) const {
            return fmt::format("Vector Store is disabled");
        }
        sstring operator()(service::vector_store_client::aborted) const {
            return fmt::format("Vector Store request was aborted");
        }
        sstring operator()(service::vector_store_client::addr_unavailable) const {
            return fmt::format("Vector Store service address could not be fetched from DNS");
        }
        sstring operator()(service::vector_store_client::service_unavailable) const {
            return fmt::format("Vector Store service is unavailable");
        }
        sstring operator()(service::vector_store_client::service_reply_format_error) const {
            return fmt::format("Vector Store returned an invalid JSON");
        }
    };

    explicit vector_store_client(config const& cfg);
    ~vector_store_client();

    /// Start background tasks.
    void start_background_tasks();

    /// Start the service.
    auto start() -> future<>;

    /// Stop the service.
    auto stop() -> future<>;

    /// Request the vector store service for the primary keys of the nearest neighbors
    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit, abort_source& as)
            -> future<std::expected<primary_keys, ann_error>>;

private:
    friend struct vector_store_client_tester;
};

/// A tester for the vector_store_client, used for testing purposes.
struct vector_store_client_tester {
    static void set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval);
    static void set_dns_resolver(vector_store_client& vsc, std::function<future<std::optional<net::inet_address>>(sstring const&)> resolver);
};

} // namespace service
