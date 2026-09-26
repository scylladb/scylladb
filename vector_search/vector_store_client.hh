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
#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/inet_address.hh>

class schema;
namespace db {
class config;
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
    /// Values of the columns requested via ann()'s return_columns
    /// parameter, keyed by column name, exactly as returned by the vector
    /// store (a raw JSON value - the caller is responsible for decoding it
    /// according to that column's actual type). A column absent from this
    /// map had no stored value for this row (e.g. the attribute didn't
    /// exist in the item when it was indexed). Always empty when
    /// return_columns was empty.
    std::unordered_map<std::string, rjson::value> column_values;
};

/// A client with the vector-store service.
class vector_store_client final : public seastar::peering_sharded_service<vector_store_client> {
    struct impl;
    std::unique_ptr<impl> _impl;

public:
    using config = db::config;
    using vs_vector = std::vector<float>;
    using query_string = std::string;
    using document = sstring;
    using documents = std::vector<document>;
    using highlights = std::vector<std::optional<sstring>>;
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
        /// The status could not be determined: the vector store is unreachable,
        /// the index is not known to it yet, or the reply could not be parsed.
        unknown,
        /// The index has been discovered and is being initialized, but the
        /// initial table scan has not started yet.
        initializing,
        /// The index is performing the initial full scan of the base table
        /// (backfilling). Queries may be served but results are incomplete.
        bootstrapping,
        /// The index has completed the initial scan and is fully operational.
        serving,
    };

    /// Query the vector store for the current status of a specific vector index.
    auto get_index_status(keyspace_name keyspace, index_name name, abort_source& as) -> future<index_status>;

    /// The role of a vector store node, derived from which configuration list
    /// (vector_store_primary_uri / vector_store_secondary_uri) it came from.
    enum class node_role {
        primary,
        secondary,
    };

    /// Connectivity of a vector store node from the perspective of this Scylla
    /// node, i.e. whether this node can reach and query it.
    enum class node_connectivity {
        /// The node is reachable and can be queried.
        up,
        /// The node is currently considered unreachable.
        down,
    };

    /// A resolved address of a vector store node and whether it is reachable.
    struct resolved_endpoint {
        seastar::net::inet_address ip;
        node_connectivity connectivity;
    };

    /// What a single vector store node reports about a single index.
    struct index_state {
        /// `unknown` when the node was not queried, does not know the index,
        /// or returned an unparsable reply.
        index_status status;
        /// Number of vectors currently indexed. Empty when not available.
        std::optional<uint64_t> count;
        /// Backfill progress in the range [0, 100]. Empty when not available.
        std::optional<double> build_progress;
    };

    /// The state of one index on one known vector store node.
    struct index_node_status {
        node_role role;
        host_name host;
        port_number port;
        /// Empty while the host has not been resolved, in which case its
        /// reachability is not known either.
        std::optional<resolved_endpoint> endpoint;
        index_state state;
    };

    /// The state of the given index on every known vector store node: one
    /// entry per resolved address of every configured URI, plus one `unknown`
    /// entry for each configured host that has not been resolved yet.
    ///
    /// Only reachable nodes are queried; the others carry a default (unknown)
    /// state. The node list is built from what the background DNS refresh has
    /// resolved so far, so the call never resolves on demand and cannot block
    /// on it; a host configured since the last refresh, or one that cannot be
    /// resolved, is reported as `unknown`.
    auto get_index_status_per_node(keyspace_name keyspace, index_name name, abort_source& as) -> future<std::vector<index_node_status>>;

    /// Request the vector store service for the primary keys of the nearest
    /// neighbors. Each returned primary_key has its similarity field set to
    /// the similarity score returned by the vector store, which sorts the
    /// results in decreasing similarity order (higher similarity score = more
    /// similar).
    ///
    /// If `routing` is true (the default), the vector store may serve the
    /// request from a different, better-matching index on the same column
    /// than the one named by `name` - this is what CQL relies on, since it
    /// has no way to pick between several indexes on the same column. Pass
    /// `false` when the caller (e.g. Alternator, which lets the user name
    /// the exact index to query) must not have its choice of index
    /// second-guessed.
    ///
    /// `return_columns` names filtering columns (as added to the index's
    /// "fc" target - see Alternator's compute_extra_fc_attributes()) whose
    /// stored values should be returned alongside the primary keys, in each
    /// result's primary_key::column_values. Empty (the default) means
    /// return no column values, matching CQL's use of ann(), which doesn't
    /// need this.
    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, const rjson::value& filter, abort_source& as,
            bool routing = true, std::vector<std::string> return_columns = {}) -> future<std::expected<primary_keys, ann_error>>;

    /// Request the vector store service for the primary keys of the top
    /// full-text search results. Each returned primary_key has its similarity
    /// field set to the BM25 relevance score returned by the vector store,
    /// which sorts the results in decreasing relevance order (higher score =
    /// more relevant).
    auto bm25(keyspace_name keyspace, index_name name, schema_ptr schema, query_string fts_query, limit limit, abort_source& as)
            -> future<std::expected<primary_keys, fts_error>>;

    /// Request a fragment of each of the given documents, with the terms of `fts_query` marked.
    ///
    /// The index is asked because choosing which of a document's terms matter needs the corpus
    /// statistics and the analyzer that only it has - not because it can look the documents up.
    /// It stores none of their text, which is why the caller has to send it.
    ///
    /// The answer is positional: entry i belongs to documents[i], std::nullopt where the reply
    /// carried no fragment for it. Nothing in the reply pairs a fragment with its document -
    /// hence no schema - so we expect the index to answer in the order it was asked, and pass
    /// the reply through unchanged.
    auto highlight(keyspace_name keyspace, index_name name, query_string fts_query, documents documents, abort_source& as)
            -> future<std::expected<highlights, fts_error>>;

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

template <>
struct fmt::formatter<vector_search::vector_store_client::index_status> : fmt::formatter<string_view> {
    auto format(vector_search::vector_store_client::index_status, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<vector_search::vector_store_client::node_role> : fmt::formatter<string_view> {
    auto format(vector_search::vector_store_client::node_role, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<vector_search::vector_store_client::node_connectivity> : fmt::formatter<string_view> {
    auto format(vector_search::vector_store_client::node_connectivity, fmt::format_context& ctx) const -> decltype(ctx.out());
};
