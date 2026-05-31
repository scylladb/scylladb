/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>
#include <optional>

#include "replica/logstor/types.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/index.hh"
#include "db/cache_tracker.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/simple_schema.hh"

using namespace replica::logstor;

namespace {

struct shared_logstor_cache {
    ::cache_tracker shared_tracker;
    replica::logstor::cache_tracker logstor_tracker;

    shared_logstor_cache()
        : shared_tracker(utils::updateable_value<double>(1.0), ::cache_tracker::register_metrics::no)
        , logstor_tracker(shared_tracker) {
    }
};

index_entry make_index_entry(uint32_t segment, uint32_t offset, uint32_t size, api::timestamp_type timestamp) {
    return index_entry{
        .location = log_location{
            .segment = log_segment_id{segment},
            .offset = offset,
            .size = size,
        },
        .timestamp = timestamp,
    };
}

mutation make_mutation(simple_schema& schema, sstring pk, sstring value) {
    auto m = schema.new_mutation(std::move(pk));
    auto ts = schema.new_timestamp();
    auto ck = schema.make_ckey(0);
    m.partition().clustered_row(*schema.schema(), ck).apply(row_marker(ts));
    schema.add_row(m, ck, std::move(value), ts);
    return m;
}

mutation make_mutation(simple_schema& schema, const dht::decorated_key& dk, sstring value) {
    auto m = mutation(schema.schema(), dk);
    auto ts = schema.new_timestamp();
    auto ck = schema.make_ckey(0);
    m.partition().clustered_row(*schema.schema(), ck).apply(row_marker(ts));
    schema.add_row(m, ck, std::move(value), ts);
    return m;
}

void populate(primary_index& index, const dht::decorated_key& dk, const mutation& m) {
    auto it = index.find(primary_index_key{dk});
    BOOST_REQUIRE(it != index.end());
    auto* ct = index.cache_tracker();
    BOOST_REQUIRE(ct);
    ct->populate(*it, m);
}

mutation lookup(primary_index& index, replica::logstor::cache_tracker& tracker, const dht::decorated_key& dk, schema_ptr schema) {
    auto it = index.find(primary_index_key{dk});
    BOOST_REQUIRE(it != index.end());

    auto result = tracker.lookup(*it, schema);
    BOOST_REQUIRE(result);
    return mutation(std::move(schema), dk, std::move(*result));
}

bool lookup_exists(primary_index& index, replica::logstor::cache_tracker& tracker, const dht::decorated_key& dk, schema_ptr schema) {
    auto it = index.find(primary_index_key{dk});
    BOOST_REQUIRE(it != index.end());

    return bool(tracker.lookup(*it, std::move(schema)));
}

bool evict_one(replica::logstor::cache_tracker& tracker) {
    return tracker.region().evict_some() == seastar::memory::reclaiming_result::reclaimed_something;
}

dht::decorated_key make_fixed_token_key(simple_schema& schema, int64_t token, sstring pk) {
    auto pkey = partition_key::from_single_value(*schema.schema(), serialized(pk));
    return dht::decorated_key(dht::token::from_int64(token), std::move(pkey));
}

std::vector<dht::decorated_key> insert_same_token_keys(primary_index& index, simple_schema& schema, int64_t token, int start, int count) {
    std::vector<dht::decorated_key> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        auto dk = make_fixed_token_key(schema, token, format("fixed-pk-{:06d}", start + i));
        auto [ok, prev] = index.insert(primary_index_key{dk}, make_index_entry(500 + i, i * 8, 8, 1));
        BOOST_REQUIRE(ok && !prev);
        keys.push_back(std::move(dk));
    }
    return keys;
}

}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_cache_invalidation_and_eviction) {
    simple_schema schema(simple_schema::with_static::no);
    primary_index index(schema.schema());
    shared_logstor_cache cache;
    index.set_cache_tracker(&cache.logstor_tracker);

    auto dk0 = schema.make_pkey("pk0");
    auto dk1 = schema.make_pkey("pk1");
    auto dk2 = schema.make_pkey("pk2");
    auto dk3 = schema.make_pkey("pk3");

    BOOST_REQUIRE(!index.insert(primary_index_key{dk0}, make_index_entry(1, 0, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(primary_index_key{dk1}, make_index_entry(1, 10, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(primary_index_key{dk2}, make_index_entry(1, 20, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(primary_index_key{dk3}, make_index_entry(1, 30, 10, 1)).second);

    auto mut0 = make_mutation(schema, "pk0", "v0");
    auto mut1 = make_mutation(schema, "pk1", "v1");
    auto mut2 = make_mutation(schema, "pk2", "v2");
    auto mut3 = make_mutation(schema, "pk3", "v3");

    populate(index, dk0, mut0);
    populate(index, dk1, mut1);
    populate(index, dk2, mut2);

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 3);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, 0);
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk0, schema.schema()));
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk1, schema.schema()));
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk2, schema.schema()));

    assert_that(lookup(index, cache.logstor_tracker, dk0, schema.schema())).is_equal_to(mut0);

    auto [inserted1, old_entry1] = index.insert(primary_index_key{dk1}, make_index_entry(2, 10, 12, 2));
    BOOST_REQUIRE(inserted1);
    BOOST_REQUIRE(old_entry1);
    BOOST_REQUIRE(!lookup_exists(index, cache.logstor_tracker, dk1, schema.schema()));

    auto [inserted2, old_entry2] = index.insert(primary_index_key{dk2}, make_index_entry(2, 20, 12, 2));
    BOOST_REQUIRE(inserted2);
    BOOST_REQUIRE(old_entry2);
    BOOST_REQUIRE(!lookup_exists(index, cache.logstor_tracker, dk2, schema.schema()));

    auto [inserted0, old_entry0] = index.insert(primary_index_key{dk0}, make_index_entry(3, 0, 14, 0));
    BOOST_REQUIRE(!inserted0);
    BOOST_REQUIRE(old_entry0);
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk0, schema.schema()));

    BOOST_REQUIRE(index.erase(primary_index_key{dk1}, make_index_entry(1, 10, 10, 1).location) == false);
    BOOST_REQUIRE(index.erase(primary_index_key{dk1}, make_index_entry(2, 10, 12, 2).location));
    BOOST_REQUIRE(index.find(primary_index_key{dk1}) == index.end());

    populate(index, dk2, mut2);
    populate(index, dk3, mut3);

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 5);
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk0, schema.schema()));
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk2, schema.schema()));
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk3, schema.schema()));

    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk2, schema.schema()));
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk3, schema.schema()));

    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + 1);

    auto cached_after_eviction = int(lookup_exists(index, cache.logstor_tracker, dk0, schema.schema()))
            + int(index.find(primary_index_key{dk2}) != index.end() && lookup_exists(index, cache.logstor_tracker, dk2, schema.schema()))
            + int(lookup_exists(index, cache.logstor_tracker, dk3, schema.schema()));
    BOOST_REQUIRE_EQUAL(cached_after_eviction, 2);

    populate(index, dk0, mut0);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 6);
    assert_that(lookup(index, cache.logstor_tracker, dk0, schema.schema())).is_equal_to(mut0);

    BOOST_REQUIRE(index.erase(primary_index_key{dk2}, make_index_entry(2, 20, 12, 2).location));
    BOOST_REQUIRE(index.find(primary_index_key{dk2}) == index.end());

    BOOST_REQUIRE(index.find(primary_index_key{dk0}) != index.end());
    BOOST_REQUIRE(index.find(primary_index_key{dk3}) != index.end());

    index.clear().get();
    BOOST_REQUIRE(index.empty());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_drain_cache_preserves_index_entries) {
    simple_schema schema(simple_schema::with_static::no);
    primary_index index(schema.schema());
    shared_logstor_cache cache;
    index.set_cache_tracker(&cache.logstor_tracker);

    auto dk0 = schema.make_pkey("pk0");
    auto dk1 = schema.make_pkey("pk1");

    BOOST_REQUIRE(!index.insert(primary_index_key{dk0}, make_index_entry(1, 0, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(primary_index_key{dk1}, make_index_entry(1, 10, 10, 1)).second);

    auto mut0 = make_mutation(schema, "pk0", "v0");
    auto mut1 = make_mutation(schema, "pk1", "v1");
    populate(index, dk0, mut0);
    populate(index, dk1, mut1);

    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk0, schema.schema()));
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk1, schema.schema()));

    index.drain_cache().get();

    BOOST_REQUIRE(index.find(primary_index_key{dk0}) != index.end());
    BOOST_REQUIRE(index.find(primary_index_key{dk1}) != index.end());
    BOOST_REQUIRE(!lookup_exists(index, cache.logstor_tracker, dk0, schema.schema()));
    BOOST_REQUIRE(!lookup_exists(index, cache.logstor_tracker, dk1, schema.schema()));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_cache_survives_index_rebalancing) {
    simple_schema schema(simple_schema::with_static::no);
    primary_index index(schema.schema());
    shared_logstor_cache cache;
    index.set_cache_tracker(&cache.logstor_tracker);

    constexpr int64_t fixed_token = 7;
    auto hot_keys = insert_same_token_keys(index, schema, fixed_token, 0, 24);

    std::vector<mutation> hot_mutations;
    hot_mutations.reserve(hot_keys.size());
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        auto m = make_mutation(schema, hot_keys[i], format("v{}", i));
        populate(index, hot_keys[i], m);
        hot_mutations.push_back(std::move(m));
    }

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, hot_keys.size());

    // Touch all cached entries first, then force same-token bucket growth and
    // in-bucket compaction. Without cache-pointer rebinding on moves, these
    // operations can leave cached entries with stale _owner_cached_ptr values.
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        assert_that(lookup(index, cache.logstor_tracker, hot_keys[i], schema.schema())).is_equal_to(hot_mutations[i]);
    }

    auto grown_keys = insert_same_token_keys(index, schema, fixed_token, 10'000, 96);
    BOOST_REQUIRE_EQUAL(index.get_key_count(), hot_keys.size() + grown_keys.size());

    for (int i = 0; i < 48; ++i) {
        BOOST_REQUIRE(index.erase(primary_index_key{grown_keys[i]}, make_index_entry(500 + i, i * 8, 8, 1).location));
    }

    BOOST_REQUIRE_EQUAL(index.get_key_count(), hot_keys.size() + grown_keys.size() - 48);

    for (size_t i = 0; i < hot_keys.size(); ++i) {
        BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, hot_keys[i], schema.schema()));
        assert_that(lookup(index, cache.logstor_tracker, hot_keys[i], schema.schema())).is_equal_to(hot_mutations[i]);
    }

    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    }
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + hot_keys.size());

    auto insertions_before_repopulate = cache.shared_tracker.get_stats().partition_insertions;
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        populate(index, hot_keys[i], hot_mutations[i]);
    }
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, insertions_before_repopulate + hot_keys.size());

    for (size_t i = 0; i < hot_keys.size(); ++i) {
        auto it = index.find(primary_index_key{hot_keys[i]});
        BOOST_REQUIRE(it != index.end());
        assert_that(lookup(index, cache.logstor_tracker, hot_keys[i], schema.schema())).is_equal_to(hot_mutations[i]);
    }

    BOOST_REQUIRE(index.find(primary_index_key{grown_keys.back()}) != index.end());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_survives_lsa_compaction_before_exchange) {
    simple_schema schema(simple_schema::with_static::no);
    primary_index index(schema.schema());
    shared_logstor_cache cache;
    index.set_cache_tracker(&cache.logstor_tracker);

    constexpr int64_t fixed_token = 19;
    auto keys = insert_same_token_keys(index, schema, fixed_token, 20'000, 8);

    std::vector<mutation> mutations;
    mutations.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto m = make_mutation(schema, keys[i], format("compaction-v{}", i));
        populate(index, keys[i], m);
        mutations.push_back(std::move(m));
    }

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        assert_that(lookup(index, cache.logstor_tracker, keys[i], schema.schema())).is_equal_to(mutations[i]);
    }

    // Create fragmentation so full_compaction() has live cached objects to move.
    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + 2);

    cache.logstor_tracker.region().full_compaction();

    for (size_t i = 2; i < keys.size(); ++i) {
        BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, keys[i], schema.schema()));
        assert_that(lookup(index, cache.logstor_tracker, keys[i], schema.schema())).is_equal_to(mutations[i]);
    }

    auto [replaced, old_entry] = index.insert(primary_index_key{keys[2]}, make_index_entry(900, 0, 32, 2));
    BOOST_REQUIRE(replaced);
    BOOST_REQUIRE(old_entry);
    BOOST_REQUIRE(!lookup_exists(index, cache.logstor_tracker, keys[2], schema.schema()));

    auto replacement = make_mutation(schema, keys[2], "compaction-replaced");
    populate(index, keys[2], replacement);
    assert_that(lookup(index, cache.logstor_tracker, keys[2], schema.schema())).is_equal_to(replacement);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_upgrades_cached_partition_after_schema_change) {
    simple_schema schema(simple_schema::with_static::no);
    primary_index index(schema.schema());
    shared_logstor_cache cache;
    index.set_cache_tracker(&cache.logstor_tracker);

    auto dk = schema.make_pkey("pk0");
    BOOST_REQUIRE(!index.insert(primary_index_key{dk}, make_index_entry(1, 0, 10, 1)).second);

    auto original = make_mutation(schema, dk, "v0");
    populate(index, dk, original);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 1);

    auto new_schema = schema_builder(schema.schema()).with_column("v2", utf8_type).build();
    schema.set_schema(new_schema);
    index.set_schema(new_schema);

    auto hits_before = cache.shared_tracker.get_stats().partition_hits;
    BOOST_REQUIRE(lookup_exists(index, cache.logstor_tracker, dk, new_schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_hits, hits_before + 1);

    auto cached = lookup(index, cache.logstor_tracker, dk, new_schema);
    auto expected = original;
    expected.upgrade(new_schema);
    BOOST_REQUIRE_EQUAL(cached.schema()->version(), new_schema->version());
    assert_that(cached).is_equal_to(expected);
}
