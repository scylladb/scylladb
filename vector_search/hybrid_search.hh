/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "vector_search/vector_store_client.hh"
#include "utils/rjson.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <cstdint>
#include <expected>
#include <optional>
#include <variant>
#include <vector>

/// Several searches run as one, their answers joined by primary key: for every key any answer
/// names, what each answer said about it.
namespace vector_search {

/// A request for one vector search: the rows nearest `vector`, among those `filter` admits.
struct ann_request {
    vector_store_client::keyspace_name keyspace;
    vector_store_client::index_name index;
    vector_store_client::vs_vector vector;
    /// How many keys the index is asked for: more than `limit` when it oversamples.
    vector_store_client::limit fetch;
    /// How many keys of the answer are joined: the first ones.
    vector_store_client::limit limit;
    rjson::value filter;
};

/// A request for one full-text search: the rows best matching `term`.
struct bm25_request {
    vector_store_client::keyspace_name keyspace;
    vector_store_client::index_name index;
    vector_store_client::query_string term;
    /// How many keys the index is asked for, all of them joined.
    vector_store_client::limit limit;
};

using search_request = std::variant<ann_request, bm25_request>;

/// What one answer said about a key it names.
struct search_hit {
    /// The similarity or relevance the index gave the key.
    float score;
    /// The key's position in that index's answer, counted from 1.
    uint32_t rank;
};

/// A key some answer names, and what every answer said about it.
struct search_candidate {
    dht::decorated_key partition;
    clustering_key_prefix clustering;
    /// One entry per request, in the order the requests were given: `hits[i]` is what the answer to
    /// `requests[i]` said about the key. Nothing where that answer does not name the key, or scored
    /// it with something that is not a number.
    std::vector<std::optional<search_hit>> hits;
};

/// Runs every request at once, cuts each answer to its request's `limit` keys, and joins the
/// answers: one candidate per distinct key, in the order the keys were first seen walking the
/// answers in request order. A key an answer names twice keeps its first rank. A hit whose score is
/// NaN or infinite is a malformed reply and is recorded as absent; the key is still worth reading
/// for the other answers' sake.
///
/// If any request fails, the first failure is returned, the requests still running are aborted,
/// and the answers already in are discarded. A request that fails with an exception aborts the
/// others the same way, and its exception fails the call.
seastar::future<std::expected<std::vector<search_candidate>, vector_store_client::ann_error>> search_all(
        vector_store_client& client, schema_ptr schema, std::vector<search_request> requests, seastar::abort_source& as);

} // namespace vector_search
