/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/select_statement.hh"
#include <seastar/core/shared_future.hh>

namespace seastar::http::experimental {
class client;
}

namespace service {

/// An interface with the vector store service.
class vector_store final {
    using client_t = seastar::http::experimental::client;
    using host_t = sstring;

    std::unique_ptr<client_t> _client;
    host_t _host; //< The host name for the HTTP protocol.

public:
    using keyspace_name_t = sstring;
    using index_name_t = sstring;

    using primary_key_t = cql3::statements::select_statement::primary_key;
    using embedding_t = std::vector<float>;
    using limit_t = std::size_t;

    vector_store();
    ~vector_store();

    /// Change the service address.
    void set_service(socket_address const& addr);

    /// Request the vector store service for the primary keys of the nearest neighbors
    [[nodiscard]] auto ann(keyspace_name_t keyspace, index_name_t name, schema_ptr schema, embedding_t embedding, limit_t limit) const
            -> future<std::optional<std::vector<primary_key_t>>>;
};

} // namespace service

