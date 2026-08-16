/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <random>
#include "keys/keys.hh"
#include "keys/clustering_bounds_comparator.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"
#include "utils/lexicographical_compare.hh"

#include "idl/keys.dist.hh"
#include "serializer_impl.hh"
#include "idl/keys.dist.impl.hh"

BOOST_AUTO_TEST_CASE(test_key_is_prefixed_by) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();
    const schema& s = *s_ptr;

    auto key = clustering_key::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")});

    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a")})));
    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("b")})));
    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")})));

    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes()})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("b"), bytes("c")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("c"), bytes("b")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("abc")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("ab")})));
}

BOOST_AUTO_TEST_CASE(test_key_component_iterator) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();
    const schema& s = *s_ptr;

    auto key = clustering_key::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")});

    auto i = key.begin(s);
    auto end = key.end(s);

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("a")));
    ++i;

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("b")));
    ++i;

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("c")));
    ++i;

    BOOST_REQUIRE(i == end);
}

BOOST_AUTO_TEST_CASE(test_legacy_ordering_for_non_composite_key) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    auto to_key = [&s] (sstring value) {
        return partition_key::from_single_value(s, to_bytes(value));
    };

    auto cmp = [&s] (const partition_key& k1, const partition_key& k2) {
        return k1.legacy_tri_compare(s, k2);
    };

    BOOST_REQUIRE(cmp(to_key("A"), to_key("B"))  < 0);
    BOOST_REQUIRE(cmp(to_key("AA"), to_key("B")) < 0);
    BOOST_REQUIRE(cmp(to_key("B"), to_key("AB")) > 0);
    BOOST_REQUIRE(cmp(to_key("B"), to_key("A"))  > 0);
    BOOST_REQUIRE(cmp(to_key("A"), to_key("A")) == 0);
}

BOOST_AUTO_TEST_CASE(test_legacy_ordering_for_composite_keys) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    auto to_key = [&s] (sstring v1, sstring v2) {
        return partition_key::from_exploded(s, std::vector<bytes>{to_bytes(v1), to_bytes(v2)});
    };

    auto cmp = [&s] (const partition_key& k1, const partition_key& k2) {
        return k1.legacy_tri_compare(s, k2);
    };

    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("A", "B")) == 0);
    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("A", "C")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("B", "B")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", "C"), to_key("B", "B")) < 0);
    BOOST_REQUIRE(cmp(to_key("B", "A"), to_key("A", "A")) > 0);

    BOOST_REQUIRE(cmp(to_key("AA", "B"), to_key("B", "B")) > 0);
    BOOST_REQUIRE(cmp(to_key("A", "AA"), to_key("A", "A")) > 0);

    BOOST_REQUIRE(cmp(to_key("", "A"), to_key("A", "A")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", ""), to_key("A", "A")) < 0);
}

BOOST_AUTO_TEST_CASE(test_conversions_between_view_and_wrapper) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    auto key = partition_key::from_deeply_exploded(s, {data_value(bytes("value"))});
    partition_key_view key_view = key;

    BOOST_REQUIRE(key_view.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key_view));

    partition_key key2 = key_view;

    BOOST_REQUIRE(key2.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key2));

    BOOST_REQUIRE(*key.begin(s) == to_managed_bytes("value"));
}

template<typename T>
inline
T reserialize(const T& v) {
    auto buf = ser::serialize_to_buffer<bytes>(v);
    auto in = ser::as_input_stream(buf);
    return ser::deserialize(in, std::type_identity<T>());
}

BOOST_AUTO_TEST_CASE(test_serialization) {
    auto s = schema_builder(1, "ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type)
            .build();

    auto pk_value = bytes("value");
    partition_key key(std::vector<bytes>({pk_value}));

    BOOST_REQUIRE(key.equal(*s, reserialize(key)));
}


BOOST_AUTO_TEST_CASE(test_from_nodetool_style_string_single_partition_key) {
    auto s1 = schema_builder(1, "", "")
            .with_column("c1", utf8_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();

    auto pk_value = bytes("value");
    partition_key key1(std::vector<bytes>({pk_value}));
    auto key2 = partition_key::from_nodetool_style_string(s1, "value");
    BOOST_REQUIRE(key1.equal(*s1, key2));

    auto pk_with_col_value = bytes("val:ue");
    partition_key key_with_col(std::vector<bytes>({pk_with_col_value}));
    BOOST_REQUIRE(key_with_col.equal(*s1, partition_key::from_nodetool_style_string(s1, "val:ue")));
}

BOOST_AUTO_TEST_CASE(test_from_nodetool_style_string_composite_partition_key) {
    auto s2 = schema_builder(1, "", "")
            .with_column("c1", utf8_type, column_kind::partition_key)
            .with_column("c2", utf8_type, column_kind::partition_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();

    auto pk_value1 = bytes("value1");
    auto pk_value2 = bytes("value2");
    partition_key key3(std::vector<bytes>({pk_value1, pk_value2}));
    auto key4 = partition_key::from_nodetool_style_string(s2, "value1:value2");
    BOOST_REQUIRE(key3.equal(*s2, key4));

    BOOST_REQUIRE_THROW(partition_key::from_nodetool_style_string(s2, "value1:value2:extra"), std::invalid_argument);
    BOOST_REQUIRE_THROW(partition_key::from_nodetool_style_string(s2, "value1"), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Differential test for bound_view::tri_compare.
//
// bound_view::tri_compare derives the relative component counts of the two
// prefixes from the exhaustion flags of the prefix walk. The implementation it
// replaced called clustering_key_prefix::size() on both prefixes, which
// re-decodes every component's length header. That older implementation is
// kept below verbatim as a reference oracle, and the tests assert the two
// agree bit-for-bit over a corpus that covers component counts 0..5 on both
// sides, empty prefixes, genuine prefix-of relationships, mixed component
// types and every weight combination.
// ---------------------------------------------------------------------------

namespace {

// The pre-optimization body of bound_view::tri_compare::operator()(p1, w1, p2, w2).
std::strong_ordering reference_bound_view_tri_compare(const schema& s,
        const clustering_key_prefix& p1, int32_t w1,
        const clustering_key_prefix& p2, int32_t w2) {
    auto type = s.clustering_key_prefix_type();
    auto res = prefix_equality_tri_compare(type->types().begin(),
        type->begin(p1.representation()), type->end(p1.representation()),
        type->begin(p2.representation()), type->end(p2.representation()),
        ::tri_compare);
    if (res != 0) {
        return res;
    }
    auto d1 = p1.size(s);
    auto d2 = p2.size(s);
    return ((d1 <= d2) ? w1 << 1 : 1) <=> ((d2 <= d1) ? w2 << 1 : 1);
}

int to_int(std::strong_ordering o) {
    return o < 0 ? -1 : (o > 0 ? 1 : 0);
}

constexpr size_t ck_columns = 5;

// Mixed types, multi-component clustering key. Fixed-width (int32/long) and
// variable-width (utf8/bytes) components exercise both length-header shapes.
schema_ptr make_multi_component_ck_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("c1", utf8_type, column_kind::clustering_key)
            .with_column("c2", int32_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", long_type, column_kind::clustering_key)
            .with_column("c5", utf8_type, column_kind::clustering_key)
            .with_column("v", bytes_type)
            .build();
}

// Deliberately tiny per-position alphabets, so that equal prefixes - the case
// that reaches the length-comparing tail - are common rather than rare.
std::vector<std::vector<bytes>> ck_candidate_values() {
    return {
        {bytes(), to_bytes("a"), to_bytes("b")},
        {int32_type->decompose(int32_t(-1)), int32_type->decompose(int32_t(0)), int32_type->decompose(int32_t(7))},
        {bytes(), to_bytes("x"), to_bytes("xy")},
        {long_type->decompose(int64_t(0)), long_type->decompose(int64_t(-5)), long_type->decompose(int64_t(1) << 40)},
        {bytes(), to_bytes("p"), to_bytes("q")},
    };
}

// For every random full-length key, all of its prefixes are added, so
// prefix-of pairs and the empty prefix are guaranteed present, not merely likely.
std::vector<clustering_key_prefix> make_ck_pool(const schema& s, std::mt19937& rnd, size_t full_keys) {
    auto candidates = ck_candidate_values();
    std::vector<clustering_key_prefix> pool;
    pool.reserve(full_keys * (ck_columns + 1));
    for (size_t i = 0; i < full_keys; ++i) {
        std::vector<bytes> full;
        for (size_t c = 0; c < ck_columns; ++c) {
            auto& values = candidates[c];
            std::uniform_int_distribution<size_t> pick(0, values.size() - 1);
            full.push_back(values[pick(rnd)]);
        }
        for (size_t n = 0; n <= ck_columns; ++n) {
            pool.push_back(clustering_key_prefix::from_exploded(s, std::vector<bytes>(full.begin(), full.begin() + n)));
        }
    }
    return pool;
}

constexpr int32_t bound_weights[] = {-1, 0, 1};

// Compares every weight combination for one key pair; returns the mismatch count.
size_t check_pair(const schema& s, const bound_view::tri_compare& cmp,
        const clustering_key_prefix& p1, const clustering_key_prefix& p2) {
    size_t mismatches = 0;
    for (auto w1 : bound_weights) {
        for (auto w2 : bound_weights) {
            auto expected = reference_bound_view_tri_compare(s, p1, w1, p2, w2);
            auto actual = cmp(p1, w1, p2, w2);
            if (actual != expected) {
                ++mismatches;
                if (mismatches <= 8) {
                    BOOST_ERROR(fmt::format("bound_view::tri_compare({}, {}, {}, {}) = {}, reference = {}",
                            p1.with_schema(s), w1, p2.with_schema(s), w2, to_int(actual), to_int(expected)));
                }
            }
        }
    }
    return mismatches;
}

}

// Exhaustive over a small deterministic corpus: all prefixes of length 0..3
// over a 2-symbol-per-position alphabet, every ordered pair, every weight pair.
BOOST_AUTO_TEST_CASE(test_bound_view_tri_compare_exhaustive_small_corpus) {
    auto s_ptr = make_multi_component_ck_schema();
    const schema& s = *s_ptr;
    bound_view::tri_compare cmp(s);

    const std::vector<bytes> a0 = {bytes(), to_bytes("a")};
    const std::vector<bytes> a1 = {int32_type->decompose(int32_t(0)), int32_type->decompose(int32_t(1))};
    const std::vector<bytes> a2 = {bytes(), to_bytes("z")};

    std::vector<clustering_key_prefix> keys;
    keys.push_back(clustering_key_prefix::from_exploded(s, std::vector<bytes>{}));
    for (auto& v0 : a0) {
        keys.push_back(clustering_key_prefix::from_exploded(s, std::vector<bytes>{v0}));
        for (auto& v1 : a1) {
            keys.push_back(clustering_key_prefix::from_exploded(s, std::vector<bytes>{v0, v1}));
            for (auto& v2 : a2) {
                keys.push_back(clustering_key_prefix::from_exploded(s, std::vector<bytes>{v0, v1, v2}));
            }
        }
    }
    BOOST_REQUIRE_EQUAL(keys.size(), 15u);

    size_t mismatches = 0;
    for (auto& p1 : keys) {
        for (auto& p2 : keys) {
            mismatches += check_pair(s, cmp, p1, p2);
        }
    }
    BOOST_REQUIRE_EQUAL(mismatches, 0u);
}

// Randomized corpus: component counts 0..5 on both sides, mixed types.
BOOST_AUTO_TEST_CASE(test_bound_view_tri_compare_matches_reference_implementation) {
    auto s_ptr = make_multi_component_ck_schema();
    const schema& s = *s_ptr;
    bound_view::tri_compare cmp(s);

    std::mt19937 rnd(20260816);
    auto pool = make_ck_pool(s, rnd, 400);
    BOOST_REQUIRE_EQUAL(pool.size(), 400u * (ck_columns + 1));

    constexpr size_t iterations = 200000;
    std::uniform_int_distribution<size_t> pick(0, pool.size() - 1);

    size_t mismatches = 0;
    for (size_t i = 0; i < iterations; ++i) {
        mismatches += check_pair(s, cmp, pool[pick(rnd)], pool[pick(rnd)]);
    }
    BOOST_TEST_MESSAGE(fmt::format("compared {} (key pair, weight pair) combinations",
            iterations * std::size(bound_weights) * std::size(bound_weights)));
    BOOST_REQUIRE_EQUAL(mismatches, 0u);
}

// The equivalence the optimization rests on: after an equal prefix walk, the
// exhaustion flags are exactly the d1<=d2 / d2<=d1 predicates.
BOOST_AUTO_TEST_CASE(test_prefix_equality_exhaustion_flags_match_component_counts) {
    auto s_ptr = make_multi_component_ck_schema();
    const schema& s = *s_ptr;
    auto type = s.clustering_key_prefix_type();

    std::mt19937 rnd(4236);
    auto pool = make_ck_pool(s, rnd, 100);

    size_t mismatches = 0;
    size_t equal_cases = 0;
    for (auto& p1 : pool) {
        for (auto& p2 : pool) {
            auto res = prefix_equality_tri_compare_with_exhaustion(type->types().begin(),
                type->begin(p1.representation()), type->end(p1.representation()),
                type->begin(p2.representation()), type->end(p2.representation()),
                ::tri_compare);
            auto plain = prefix_equality_tri_compare(type->types().begin(),
                type->begin(p1.representation()), type->end(p1.representation()),
                type->begin(p2.representation()), type->end(p2.representation()),
                ::tri_compare);
            mismatches += (res.order != plain);
            if (res.order == 0) {
                ++equal_cases;
                auto d1 = p1.size(s);
                auto d2 = p2.size(s);
                mismatches += (res.exhausted1 != (d1 <= d2));
                mismatches += (res.exhausted2 != (d2 <= d1));
            }
        }
    }
    BOOST_REQUIRE_GT(equal_cases, 0u);
    BOOST_REQUIRE_EQUAL(mismatches, 0u);
}
