/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "dht/decorated_key.hh"
#include "keys/keys.hh"
#include "seastarx.hh"
#include "error.hh"
#include "utils/rjson.hh"
#include <chrono>
#include <expected>
#include <functional>
#include <variant>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/reply.hh>

class schema;
namespace db {
class config;
}

namespace seastar::net {
class inet_address;
}

namespace vector_search {

struct primary_key {
    dht::decorated_key partition;
    clustering_key_prefix clustering;
    /// The similarity score returned by the vector store (higher = more
    /// similar, and earlier in the result set). Similarity is in the range
    /// [0.0, 1.0] for cosine and euclidean; unbounded for dot product on
    /// non-normalized vectors.
    float similarity = 0.0f;
};

/// A client with the vector-store service.
class vector_store_client final : public seastar::peering_sharded_service<vector_store_client> {
    struct impl;
    std::unique_ptr<impl> _impl;

public:
    using config = db::config;
    using vs_vector = std::vector<float>;
    using query_string = std::string;
    using host_name = sstring;
    using index_name = sstring;
    using keyspace_name = sstring;
    using limit = std::size_t;
    using port_number = std::uint16_t;
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
    using fts_error = ann_error;
    using fts_error_visitor = ann_error_visitor;

    explicit vector_store_client(config const& cfg);
    ~vector_store_client();

    /// Start background tasks.
    void start_background_tasks();

    /// Stop the service.
    auto stop() -> future<>;

    /// Check if the vector_store_client is disabled.
    auto is_disabled() const -> bool;

    /// The operational status of a single vector index, as reported by the vector store.
    enum class index_status {
        /// The index is not yet ready: initializing, not yet discovered, or the
        /// vector store is unreachable.
        creating,
        /// The index is performing the initial full scan of the base table
        /// (backfilling). Queries may be served but results are incomplete.
        backfilling,
        /// The index has completed the initial scan and is fully operational.
        serving,
    };

    /// Query the vector store for the current status of a specific vector index.
    auto get_index_status(keyspace_name keyspace, index_name name, abort_source& as) -> future<index_status>;

    /// Request the vector store service for the primary keys of the nearest
    /// neighbors. Each returned primary_key has its similarity field set to
    /// the similarity score returned by the vector store, which sorts the
    /// results in decreasing similarity order (higher similarity score = more
    /// similar).
    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, const rjson::value& filter, abort_source& as)
            -> future<std::expected<primary_keys, ann_error>>;

    /// Request the vector store service for the primary keys of the top
    /// full-text search results. Each returned primary_key has its similarity
    /// field set to the BM25 relevance score returned by the vector store,
    /// which sorts the results in decreasing relevance order (higher score =
    /// more relevant).
    auto bm25(keyspace_name keyspace, index_name name, schema_ptr schema, query_string fts_query, limit limit, abort_source& as)
            -> future<std::expected<primary_keys, fts_error>>;

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
    static unsigned truststore_reload_count(vector_store_client& vsc);
};

} // namespace vector_search
