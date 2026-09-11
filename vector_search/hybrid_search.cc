/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "vector_search/hybrid_search.hh"

#include "keys/keys.hh"
#include "schema/schema.hh"

#include <seastar/coroutine/parallel_for_each.hh>

#include <cmath>
#include <map>
#include <ranges>

namespace vector_search {

serialized_primary_key serialize_primary_key(const schema& schema, const partition_key& partition, const clustering_key_prefix& clustering) {
    auto clustering_bytes = schema.clustering_key_size() > 0 ? to_bytes(clustering.representation()) : bytes{};
    return serialized_primary_key{to_bytes(partition.representation()), std::move(clustering_bytes)};
}

std::vector<hybrid_candidate> join_answers(const schema& schema, std::span<const vector_store_client::primary_keys> answers) {
    auto candidates = std::vector<hybrid_candidate>{};
    auto index_of = std::map<serialized_primary_key, size_t>{};

    for (size_t search = 0; search < answers.size(); ++search) {
        for (size_t rank = 0; rank < answers[search].size(); ++rank) {
            const auto& key = answers[search][rank];
            auto [it, inserted] = index_of.emplace(serialize_primary_key(schema, key.partition.key(), key.clustering), candidates.size());
            if (inserted) {
                candidates.push_back(hybrid_candidate{.partition = key.partition,
                        .clustering = key.clustering,
                        .hits = std::vector<std::optional<search_hit>>(answers.size(), std::nullopt)});
            }
            auto& hit = candidates[it->second].hits[search];
            if (!hit && std::isfinite(key.similarity)) {
                hit = search_hit{.score = key.similarity, .rank = static_cast<uint32_t>(rank + 1)};
            }
        }
    }
    return candidates;
}

seastar::future<std::expected<std::vector<hybrid_candidate>, vector_store_client::ann_error>> search_all(
        vector_store_client& client, schema_ptr schema, std::vector<search_request> requests, seastar::abort_source& as) {
    auto answers = std::vector<std::expected<vector_store_client::primary_keys, vector_store_client::ann_error>>(requests.size());

    co_await seastar::coroutine::parallel_for_each(std::views::iota(size_t(0), requests.size()), [&] (size_t i) -> seastar::future<> {
        answers[i] = co_await std::visit(
                [&] (auto& request) {
                    using request_type = std::decay_t<decltype(request)>;
                    if constexpr (std::is_same_v<request_type, ann_request>) {
                        return client.ann(request.keyspace, request.index, schema, std::move(request.vector), request.limit, request.filter, as);
                    } else {
                        return client.bm25(request.keyspace, request.index, schema, std::move(request.term), request.limit, as);
                    }
                },
                requests[i]);
    });

    auto keys = std::vector<vector_store_client::primary_keys>{};
    keys.reserve(answers.size());
    for (auto& answer : answers) {
        if (!answer) {
            co_return std::unexpected(std::move(answer.error()));
        }
        keys.push_back(std::move(*answer));
    }
    co_return join_answers(*schema, keys);
}

} // namespace vector_search
