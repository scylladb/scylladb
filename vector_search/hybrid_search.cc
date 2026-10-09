/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "vector_search/hybrid_search.hh"

#include "keys/keys.hh"
#include "schema/schema.hh"
#include "utils/chain_abort_source.hh"
#include "utils/overloaded_functor.hh"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <cmath>
#include <map>
#include <optional>
#include <ranges>
#include <span>

namespace vector_search {

namespace {

/// A primary key as bytes, for looking a row up: two keys are equal exactly when they name the same
/// row. A table with no clustering columns leaves the second half empty.
using serialized_primary_key = std::pair<bytes, bytes>;

serialized_primary_key serialize_primary_key(const schema& schema, const partition_key& partition, const clustering_key_prefix& clustering) {
    auto clustering_bytes = schema.clustering_key_size() > 0 ? to_bytes(clustering.representation()) : bytes{};
    return serialized_primary_key{to_bytes(partition.representation()), std::move(clustering_bytes)};
}

/// The candidates of search_all(), with `hits[i]` filled from `answers[i]`, the answer to
/// `requests[i]`.
std::vector<search_candidate> join_answers(const schema& schema, std::span<const vector_store_client::primary_keys> answers) {
    auto candidates = std::vector<search_candidate>{};
    auto index_of = std::map<serialized_primary_key, size_t>{};

    for (size_t i = 0; i < answers.size(); ++i) {
        for (size_t rank = 0; rank < answers[i].size(); ++rank) {
            const auto& key = answers[i][rank];
            auto [it, inserted] = index_of.emplace(serialize_primary_key(schema, key.partition.key(), key.clustering), candidates.size());
            if (inserted) {
                candidates.push_back(search_candidate{.partition = key.partition,
                        .clustering = key.clustering,
                        .hits = std::vector<std::optional<search_hit>>(answers.size(), std::nullopt)});
            }
            auto& hit = candidates[it->second].hits[i];
            if (!hit && std::isfinite(key.similarity)) {
                hit = search_hit{.score = key.similarity, .rank = static_cast<uint32_t>(rank + 1)};
            }
        }
    }
    return candidates;
}

} // anonymous namespace

seastar::future<std::expected<std::vector<search_candidate>, vector_store_client::ann_error>> search_all(
        vector_store_client& client, schema_ptr schema, std::vector<search_request> requests, seastar::abort_source& as) {
    auto answers = std::vector<vector_store_client::primary_keys>(requests.size());
    // The first failure fails the whole query, so the requests still running are aborted rather
    // than waited for. Only the first failure is kept: the aborted siblings fail too, and their
    // errors would only say that they were aborted. A request can also fail with an exception
    // rather than an error, e.g. on a key of the wrong type in its answer; it aborts the others
    // the same way.
    auto first_error = std::optional<vector_store_client::ann_error>{};
    auto searches_as = seastar::abort_source{};
    const auto query_abort = utils::chain_abort_source(searches_as, as);

    co_await seastar::coroutine::parallel_for_each(std::views::iota(size_t(0), requests.size()), [&] (size_t i) -> seastar::future<> {
        auto answered = co_await seastar::coroutine::as_future(std::visit(overloaded_functor{
                [&] (ann_request& request) {
                    return client.ann(request.keyspace, request.index, schema, std::move(request.vector), request.fetch, request.filter, searches_as);
                },
                [&] (bm25_request& request) {
                    return client.bm25(request.keyspace, request.index, schema, std::move(request.term), request.limit, searches_as);
                },
        }, requests[i]));
        if (answered.failed()) {
            searches_as.request_abort();
            co_await seastar::coroutine::return_exception_ptr(answered.get_exception());
        }
        auto answer = answered.get();
        if (answer) {
            const auto kept = std::visit([] (const auto& request) { return request.limit; }, requests[i]);
            if (answer->size() > kept) {
                answer->erase(answer->begin() + kept, answer->end());
            }
            answers[i] = std::move(*answer);
        } else if (!first_error) {
            first_error = std::move(answer.error());
            searches_as.request_abort();
        }
    });

    if (first_error) {
        co_return std::unexpected(std::move(*first_error));
    }
    co_return join_answers(*schema, answers);
}

} // namespace vector_search
