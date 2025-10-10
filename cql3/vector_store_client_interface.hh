/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
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

namespace cql3 {

class vector_store_client_interface {

public:
    using config = db::config;
    using vs_vector = std::vector<float>;
    using host_name = seastar::sstring;
    using index_name = seastar::sstring;
    using keyspace_name = seastar::sstring;
    using limit = std::size_t;
    using port_number = std::uint16_t;
    using primary_key = cql3::statements::primary_key;
    using primary_keys = std::vector<primary_key>;
    using schema_ptr = seastar::lw_shared_ptr<schema const>;
    using status_type = seastar::http::reply::status_type;

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
        seastar::sstring operator()(vector_store_client_interface::service_error e) const {
            return fmt::format("Vector Store error: HTTP status {}", e.status);
        }
        seastar::sstring operator()(vector_store_client_interface::disabled) const {
            return fmt::format("Vector Store is disabled");
        }
        seastar::sstring operator()(vector_store_client_interface::aborted) const {
            return fmt::format("Vector Store request was aborted");
        }
        seastar::sstring operator()(vector_store_client_interface::addr_unavailable) const {
            return fmt::format("Vector Store service address could not be fetched from DNS");
        }
        seastar::sstring operator()(vector_store_client_interface::service_unavailable) const {
            return fmt::format("Vector Store service is unavailable");
        }
        seastar::sstring operator()(vector_store_client_interface::service_reply_format_error) const {
            return fmt::format("Vector Store returned an invalid JSON");
        }
    };

    virtual ~vector_store_client_interface() = default;

    /// Start background tasks.
    virtual void start_background_tasks() = 0;

    /// Stop the service.
    virtual auto stop() -> seastar::future<> = 0;

    /// Check if the vector_store_client is disabled.
    virtual auto is_disabled() const -> bool = 0;

    /// Request the vector store service for the primary keys of the nearest neighbors
    virtual auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, seastar::abort_source& as)
            -> seastar::future<std::expected<primary_keys, ann_error>> = 0;
};

}