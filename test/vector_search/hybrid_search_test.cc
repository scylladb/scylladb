/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Unit tests for join_answers(): the join of several searches' answers by primary key. A CQL test
// against the Vector Store mock sees only the fused order and the values, not which key was matched
// to which, and cannot make the mock hand back a malformed score. Keys are built from a schema, which
// needs a reactor, so the cases run in a seastar thread.

#include "vector_search/hybrid_search.hh"

#include "dht/i_partitioner.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"

#include <seastar/testing/thread_test_case.hh>

#include <limits>

using namespace vector_search;
using primary_keys = vector_store_client::primary_keys;

BOOST_AUTO_TEST_SUITE(hybrid_search_test)

namespace {

schema_ptr make_schema(bool with_clustering_key) {
    auto builder = schema_builder(this_smp_shard_count(), "ks", "cf")
                           .with_column("pk", int32_type, column_kind::partition_key)
                           .with_column("v", int32_type, column_kind::regular_column);
    if (with_clustering_key) {
        builder.with_column("ck", int32_type, column_kind::clustering_key);
    }
    return builder.build();
}

primary_key key(const schema& s, int32_t pk, int32_t ck, float score) {
    return {dht::decorate_key(s, partition_key::from_single_value(s, int32_type->decompose(pk))),
            clustering_key::from_single_value(s, int32_type->decompose(ck)), score};
}

int32_t pk_of(const schema& s, const hybrid_candidate& candidate) {
    return value_cast<int32_t>(int32_type->deserialize(candidate.partition.key().get_component(s, 0)));
}

std::optional<uint32_t> rank_of(const hybrid_candidate& candidate, size_t search) {
    return candidate.hits[search] ? std::optional(candidate.hits[search]->rank) : std::nullopt;
}

} // anonymous namespace

// Every key any search returned is a candidate once, in the order first seen, with each search's
// score and rank beside it and nothing where a search did not return it.
SEASTAR_THREAD_TEST_CASE(test_answers_are_joined_by_key) {
    auto s = make_schema(true);
    auto answers = std::vector<primary_keys>{
            {key(*s, 1, 1, 0.9f), key(*s, 2, 1, 0.8f), key(*s, 3, 1, 0.7f)},
            {key(*s, 3, 1, 5.0f), key(*s, 4, 1, 4.0f), key(*s, 1, 1, 3.0f)},
    };

    auto candidates = join_answers(*s, answers);

    BOOST_REQUIRE_EQUAL(candidates.size(), 4u);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[0]), 1);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[1]), 2);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[2]), 3);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[3]), 4);

    for (const auto& candidate : candidates) {
        BOOST_REQUIRE_EQUAL(candidate.hits.size(), 2u);
    }
    BOOST_REQUIRE(rank_of(candidates[0], 0) == 1u);
    BOOST_REQUIRE(rank_of(candidates[0], 1) == 3u);
    BOOST_REQUIRE_EQUAL(candidates[0].hits[0]->score, 0.9f);
    BOOST_REQUIRE_EQUAL(candidates[0].hits[1]->score, 3.0f);
    BOOST_REQUIRE(rank_of(candidates[1], 0) == 2u);
    BOOST_REQUIRE(!candidates[1].hits[1]);
    BOOST_REQUIRE(rank_of(candidates[2], 0) == 3u);
    BOOST_REQUIRE(rank_of(candidates[2], 1) == 1u);
    BOOST_REQUIRE(!candidates[3].hits[0]);
    BOOST_REQUIRE(rank_of(candidates[3], 1) == 2u);
}

// Rows of one partition are told apart by their clustering key.
SEASTAR_THREAD_TEST_CASE(test_clustering_key_tells_rows_apart) {
    auto s = make_schema(true);
    auto answers = std::vector<primary_keys>{
            {key(*s, 1, 10, 0.9f), key(*s, 1, 20, 0.8f)},
            {key(*s, 1, 20, 5.0f)},
    };

    auto candidates = join_answers(*s, answers);

    BOOST_REQUIRE_EQUAL(candidates.size(), 2u);
    BOOST_REQUIRE(!candidates[0].hits[1]);
    BOOST_REQUIRE(rank_of(candidates[1], 0) == 2u);
    BOOST_REQUIRE(rank_of(candidates[1], 1) == 1u);
}

// A table with no clustering columns is joined on the partition key alone.
SEASTAR_THREAD_TEST_CASE(test_join_without_a_clustering_key) {
    auto s = make_schema(false);
    auto first = primary_key{dht::decorate_key(*s, partition_key::from_single_value(*s, int32_type->decompose(1))),
            clustering_key_prefix::make_empty(), 0.9f};
    auto second = primary_key{dht::decorate_key(*s, partition_key::from_single_value(*s, int32_type->decompose(2))),
            clustering_key_prefix::make_empty(), 0.8f};
    auto answers = std::vector<primary_keys>{{first, second}, {second}};

    auto candidates = join_answers(*s, answers);

    BOOST_REQUIRE_EQUAL(candidates.size(), 2u);
    BOOST_REQUIRE(!candidates[0].hits[1]);
    BOOST_REQUIRE(rank_of(candidates[1], 1) == 1u);
}

// A key an answer names twice is taken where the index ranked it first.
SEASTAR_THREAD_TEST_CASE(test_a_repeated_key_keeps_its_first_rank) {
    auto s = make_schema(true);
    auto answers = std::vector<primary_keys>{{key(*s, 1, 1, 0.9f), key(*s, 1, 1, 0.1f)}};

    auto candidates = join_answers(*s, answers);

    BOOST_REQUIRE_EQUAL(candidates.size(), 1u);
    BOOST_REQUIRE(rank_of(candidates[0], 0) == 1u);
    BOOST_REQUIRE_EQUAL(candidates[0].hits[0]->score, 0.9f);
}

// A score that is not a number is a malformed reply: the search's hit is absent, but the key is still
// a candidate for the other searches' sake.
SEASTAR_THREAD_TEST_CASE(test_a_score_that_is_not_a_number_is_no_hit) {
    auto s = make_schema(true);
    auto answers = std::vector<primary_keys>{
            {key(*s, 1, 1, std::numeric_limits<float>::quiet_NaN()), key(*s, 2, 1, std::numeric_limits<float>::infinity())},
            {key(*s, 1, 1, 5.0f)},
    };

    auto candidates = join_answers(*s, answers);

    BOOST_REQUIRE_EQUAL(candidates.size(), 2u);
    BOOST_REQUIRE(!candidates[0].hits[0]);
    BOOST_REQUIRE(rank_of(candidates[0], 1) == 1u);
    BOOST_REQUIRE(!candidates[1].hits[0]);
    BOOST_REQUIRE(!candidates[1].hits[1]);
}

// One answer alone gives its own keys in its own order, ranked 1, 2, 3.
SEASTAR_THREAD_TEST_CASE(test_one_answer_alone_keeps_its_order) {
    auto s = make_schema(true);
    auto answers = std::vector<primary_keys>{{key(*s, 3, 1, 0.9f), key(*s, 1, 1, 0.8f), key(*s, 2, 1, 0.7f)}};

    auto candidates = join_answers(*s, answers);

    BOOST_REQUIRE_EQUAL(candidates.size(), 3u);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[0]), 3);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[1]), 1);
    BOOST_REQUIRE_EQUAL(pk_of(*s, candidates[2]), 2);
    for (size_t i = 0; i < candidates.size(); ++i) {
        BOOST_REQUIRE(rank_of(candidates[i], 0) == i + 1);
    }
}

BOOST_AUTO_TEST_SUITE_END()
