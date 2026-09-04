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

#include <expected>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

/// Several searches run as one, their answers joined by primary key.
///
/// The Vector Store answers one search at a time. A query served by more than one search - a
/// vector search and a full-text search over the same table - needs the answers side by side: for
/// every key any search returned, what each search said about it. What to make of a key one
/// search did not return, and how to order the keys, is the caller's business.
namespace vector_search {

/// One vector search: the rows nearest `vector`, among those `filter` admits.
struct ann_request {
    vector_store_client::keyspace_name keyspace;
    vector_store_client::index_name index;
    vector_store_client::vs_vector vector;
    vector_store_client::limit limit;
    rjson::value filter;
};

/// One full-text search: the rows best matching `term`.
struct bm25_request {
    vector_store_client::keyspace_name keyspace;
    vector_store_client::index_name index;
    vector_store_client::query_string term;
    vector_store_client::limit limit;
};

using search_request = std::variant<ann_request, bm25_request>;

/// What one search said about a key it returned.
struct search_hit {
    /// The similarity or relevance the index gave the key.
    float score;
    /// The key's position in that index's answer, counted from 1.
    uint32_t rank;
};

/// A key some search returned, and what every search said about it.
struct hybrid_candidate {
    dht::decorated_key partition;
    clustering_key_prefix clustering;
    /// One entry per search, in the order the searches were given. Nothing where the search did not
    /// return the key, or scored it with something that is not a number.
    std::vector<std::optional<search_hit>> hits;
};

/// A primary key as bytes, for looking a row up: two keys are equal exactly when they name the same
/// row. A table with no clustering columns leaves the second half empty.
using serialized_primary_key = std::pair<bytes, bytes>;
serialized_primary_key serialize_primary_key(const schema& schema, const partition_key& partition, const clustering_key_prefix& clustering);

/// Joins the answers of several searches by primary key: one candidate per distinct key, with
/// `hits[i]` filled from `answers[i]`. The candidates are in the order the keys were first seen,
/// walking the answers in turn: one answer alone gives its own keys in its own order, ranked 1, 2, 3.
///
/// A key an answer names twice is taken the first time: that is where the index ranked it, and
/// the rank its position means. A hit whose score is NaN or infinite is recorded as absent: the
/// Vector Store cannot send Inf over JSON and should not send NaN, so such a score is a malformed
/// reply, and the key is still worth reading for the other searches' sake.
std::vector<hybrid_candidate> join_answers(const schema& schema, std::span<const vector_store_client::primary_keys> answers);

/// Runs every request at once and joins the answers. A hybrid query costs the slowest search rather
/// than their sum. If any request fails, the first failure is returned and the rest are discarded.
seastar::future<std::expected<std::vector<hybrid_candidate>, vector_store_client::ann_error>> search_all(
        vector_store_client& client, schema_ptr schema, std::vector<search_request> requests, seastar::abort_source& as);

} // namespace vector_search
