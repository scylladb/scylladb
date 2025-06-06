/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/select_statement.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/http/reply.hh>

namespace seastar::http::experimental {
class client;
}

namespace service {

/// An interface with the vector store service.
class vector_store final {
    using client = seastar::http::experimental::client;
    using host_name = sstring;

    std::unique_ptr<client> _client;
    host_name _host; //< The host name for the HTTP protocol.

public:
    using keyspace_name = sstring;
    using index_name = sstring;
    using primary_key = cql3::statements::select_statement::primary_key;
    using primary_keys = std::vector<primary_key>;
    using embedding = std::vector<float>;
    using limit = std::size_t;
    using status_type = seastar::http::reply::status_type;

    /// The vector-store service is disabled.
    struct disabled {};

    /// The vector-store service is unavailable.
    struct service_unavailable {};

    /// The error from the vector-store service.
    struct service_error {
        status_type _status; ///< The HTTP status code from the vector-store service.
    };

    /// An unsupported reply format from the vector-store service.
    struct service_reply_format_error {};

    using ann_error = std::variant<disabled, service_unavailable, service_error, service_reply_format_error>;

    vector_store()
        : vector_store(std::getenv("VECTOR_STORE_IP"), std::getenv("VECTOR_STORE_PORT")) {
    }
    vector_store(char const* ip_env, char const* port_env);
    ~vector_store();

    [[nodiscard]] auto host() const -> host_name const& {
        return _host;
    }

    /// Change the service address.
    void set_service(socket_address const& addr);

    /// Request the vector store service for the primary keys of the nearest neighbors
    [[nodiscard]] auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit) const
            -> future<std::expected<primary_keys, ann_error>>;
};

} // namespace service

