/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "seastarx.hh"
#include "error.hh"
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

namespace vector_search {

/// A client with the vector-store service.
class vector_store_client final {
    struct impl;
    std::unique_ptr<impl> _impl;

public:
    using config = db::config;
    using vs_vector = std::vector<float>;
    using host_name = sstring;
    using index_name = sstring;
    using keyspace_name = sstring;
    using limit = std::size_t;
    using port_number = std::uint16_t;
    using primary_key = cql3::statements::primary_key;
    using primary_keys = std::vector<primary_key>;
    using schema_ptr = lw_shared_ptr<schema const>;
    using status_type = http::reply::status_type;

    using disabled = disabled_error;
    using aborted = aborted_error;
    using addr_unavailable = addr_unavailable_error;
    using service_unavailable = service_unavailable_error;
    using service_error = service_error;
    using service_reply_format_error = service_reply_format_error;

    using ann_error = std::variant<disabled, aborted, addr_unavailable, service_unavailable, service_error, service_reply_format_error>;
    using ann_error_visitor = error_visitor;

    explicit vector_store_client(config const& cfg);
    ~vector_store_client();

    /// Start background tasks.
    void start_background_tasks();

    /// Stop the service.
    auto stop() -> future<>;

    /// Check if the vector_store_client is disabled.
    auto is_disabled() const -> bool;

    /// Request the vector store service for the primary keys of the nearest neighbors
    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
            -> future<std::expected<primary_keys, ann_error>>;

private:
    friend struct vector_store_client_tester;
};

/// A tester for the vector_store_client, used for testing purposes.
struct vector_store_client_tester {
    static void set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval);
    static void set_wait_for_client_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout);
    static void set_dns_resolver(vector_store_client& vsc, std::function<future<std::vector<net::inet_address>>(sstring const&)> resolver);
    static void trigger_dns_resolver(vector_store_client& vsc);
    static auto resolve_hostname(vector_store_client& vsc, abort_source& as) -> future<std::vector<net::inet_address>>;
};

} // namespace vector_search
