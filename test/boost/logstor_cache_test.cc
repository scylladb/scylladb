/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>

#include "replica/logstor/types.hh"
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/index.hh"
#include "db/cache_tracker.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_assertions.hh"

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

schema_ptr make_logstor_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

primary_index_key make_primary_index_key(const schema& schema, sstring pk) {
    auto pkey = partition_key::from_single_value(schema, serialized(pk));
    auto dk = dht::decorate_key(schema, pkey);
    return primary_index_key{dk};
}

primary_index_key make_fixed_token_key(const schema& schema, int64_t token, sstring pk) {
    auto pkey = partition_key::from_single_value(schema, serialized(pk));
    auto dk = dht::decorated_key(dht::token::from_int64(token), std::move(pkey));
    return primary_index_key{dk};
}

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

mutation make_mutation(schema_ptr schema, sstring pk, sstring value) {
    auto key = partition_key::from_single_value(*schema, serialized(std::move(pk)));
    auto dk = dht::decorate_key(*schema, key);
    mutation m(schema, dk);
    auto ts = api::timestamp_type(1);
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(row_marker(ts));
    const auto& v_def = *schema->get_column_definition("v");
    row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, ts, serialized(std::move(value))));
    return m;
}

void populate(primary_index& index, const primary_index_key& key, const mutation& m) {
    BOOST_REQUIRE(index.populate_cache(key, m));
}

mutation_partition lookup(primary_index& index, const primary_index_key& key, schema_ptr schema) {
    auto result = index.lookup_for_read(key, schema, true);
    BOOST_REQUIRE(result);
    BOOST_REQUIRE(result->cached_mutation);
    return std::move(result->cached_mutation->partition());
}

bool lookup_exists(primary_index& index, const primary_index_key& key, schema_ptr schema) {
    auto result = index.lookup_for_read(key, std::move(schema), true);
    BOOST_REQUIRE(result);
    return bool(result->cached_mutation);
}

bool evict_one(replica::logstor::cache_tracker& tracker) {
    return tracker.region().evict_some() == seastar::memory::reclaiming_result::reclaimed_something;
}

std::vector<primary_index_key> insert_same_token_keys(primary_index& index, const schema& schema, int64_t token, int start, int count) {
    std::vector<primary_index_key> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        auto key = make_fixed_token_key(schema, token, format("fixed-pk-{:06d}", start + i));
        auto [ok, prev] = index.insert(key, make_index_entry(500 + i, i * 8, 8, 1));
        BOOST_REQUIRE(ok && !prev);
        keys.push_back(std::move(key));
    }
    return keys;
}

}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_cache_invalidation_and_eviction) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(schema, &cache.logstor_tracker);

    auto key0 = make_primary_index_key(*schema, "pk0");
    auto key1 = make_primary_index_key(*schema, "pk1");
    auto key2 = make_primary_index_key(*schema, "pk2");
    auto key3 = make_primary_index_key(*schema, "pk3");

    BOOST_REQUIRE(!index.insert(key0, make_index_entry(1, 0, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(key1, make_index_entry(1, 10, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(key2, make_index_entry(1, 20, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(key3, make_index_entry(1, 30, 10, 1)).second);

    auto mut0 = make_mutation(schema, "pk0", "v0");
    auto mut1 = make_mutation(schema, "pk1", "v1");
    auto mut2 = make_mutation(schema, "pk2", "v2");
    auto mut3 = make_mutation(schema, "pk3", "v3");

    populate(index, key0, mut0);
    populate(index, key1, mut1);
    populate(index, key2, mut2);

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 3);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, 0);
    BOOST_REQUIRE(lookup_exists(index, key0, schema));
    BOOST_REQUIRE(lookup_exists(index, key1, schema));
    BOOST_REQUIRE(lookup_exists(index, key2, schema));

    assert_that(schema, lookup(index, key0, schema)).is_equal_to(mut0.partition());

    auto [inserted1, old_entry1] = index.insert(key1, make_index_entry(2, 10, 12, 2));
    BOOST_REQUIRE(inserted1);
    BOOST_REQUIRE(old_entry1);
    BOOST_REQUIRE(!lookup_exists(index, key1, schema));

    auto [inserted2, old_entry2] = index.insert(key2, make_index_entry(2, 20, 12, 2));
    BOOST_REQUIRE(inserted2);
    BOOST_REQUIRE(old_entry2);
    BOOST_REQUIRE(!lookup_exists(index, key2, schema));

    auto [inserted0, old_entry0] = index.insert(key0, make_index_entry(3, 0, 14, 0));
    BOOST_REQUIRE(!inserted0);
    BOOST_REQUIRE(old_entry0);
    BOOST_REQUIRE(lookup_exists(index, key0, schema));

    BOOST_REQUIRE(index.erase(key1, make_index_entry(1, 10, 10, 1).location) == false);
    BOOST_REQUIRE(index.erase(key1, make_index_entry(2, 10, 12, 2).location));
    BOOST_REQUIRE(!index.get(key1));

    populate(index, key2, mut2);
    populate(index, key3, mut3);

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 5);
    BOOST_REQUIRE(lookup_exists(index, key0, schema));
    BOOST_REQUIRE(lookup_exists(index, key2, schema));
    BOOST_REQUIRE(lookup_exists(index, key3, schema));

    BOOST_REQUIRE(lookup_exists(index, key2, schema));
    BOOST_REQUIRE(lookup_exists(index, key3, schema));

    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + 1);

    auto cached_after_eviction = int(lookup_exists(index, key0, schema))
        + int(index.get(key2) && lookup_exists(index, key2, schema))
        + int(lookup_exists(index, key3, schema));
    BOOST_REQUIRE_EQUAL(cached_after_eviction, 2);

    populate(index, key0, mut0);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 6);
    assert_that(schema, lookup(index, key0, schema)).is_equal_to(mut0.partition());

    BOOST_REQUIRE(index.erase(key2, make_index_entry(2, 20, 12, 2).location));
    BOOST_REQUIRE(!index.get(key2));

    BOOST_REQUIRE(index.get(key0));
    BOOST_REQUIRE(index.get(key3));

    index.clear().get();
    BOOST_REQUIRE(index.empty());
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_drain_cache_preserves_index_entries) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(schema, &cache.logstor_tracker);

    auto key0 = make_primary_index_key(*schema, "pk0");
    auto key1 = make_primary_index_key(*schema, "pk1");

    BOOST_REQUIRE(!index.insert(key0, make_index_entry(1, 0, 10, 1)).second);
    BOOST_REQUIRE(!index.insert(key1, make_index_entry(1, 10, 10, 1)).second);

    auto mut0 = make_mutation(schema, "pk0", "v0");
    auto mut1 = make_mutation(schema, "pk1", "v1");
    populate(index, key0, mut0);
    populate(index, key1, mut1);

    BOOST_REQUIRE(lookup_exists(index, key0, schema));
    BOOST_REQUIRE(lookup_exists(index, key1, schema));

    index.drain_cache().get();

    BOOST_REQUIRE(index.get(key0));
    BOOST_REQUIRE(index.get(key1));
    BOOST_REQUIRE(!lookup_exists(index, key0, schema));
    BOOST_REQUIRE(!lookup_exists(index, key1, schema));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_cache_survives_index_rebalancing) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(schema, &cache.logstor_tracker);

    constexpr int64_t fixed_token = 7;
    auto hot_keys = insert_same_token_keys(index, *schema, fixed_token, 0, 24);

    std::vector<mutation> hot_mutations;
    hot_mutations.reserve(hot_keys.size());
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        auto m = make_mutation(schema, format("fixed-pk-{:06d}", i), format("v{}", i));
        populate(index, hot_keys[i], m);
        hot_mutations.push_back(std::move(m));
    }

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, hot_keys.size());

    // Touch all cached entries first, then force same-token bucket growth and
    // in-bucket compaction. Without cache-pointer rebinding on moves, these
    // operations can leave cached entries with stale _owner_cached_ptr values.
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        assert_that(schema, lookup(index, hot_keys[i], schema)).is_equal_to(hot_mutations[i].partition());
    }

    auto grown_keys = insert_same_token_keys(index, *schema, fixed_token, 10'000, 96);
    BOOST_REQUIRE_EQUAL(index.get_key_count(), hot_keys.size() + grown_keys.size());

    for (int i = 0; i < 48; ++i) {
        BOOST_REQUIRE(index.erase(grown_keys[i], make_index_entry(500 + i, i * 8, 8, 1).location));
    }

    BOOST_REQUIRE_EQUAL(index.get_key_count(), hot_keys.size() + grown_keys.size() - 48);

    for (size_t i = 0; i < hot_keys.size(); ++i) {
        BOOST_REQUIRE(lookup_exists(index, hot_keys[i], schema));
        assert_that(schema, lookup(index, hot_keys[i], schema)).is_equal_to(hot_mutations[i].partition());
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
        auto entry = index.get(hot_keys[i]);
        BOOST_REQUIRE(entry);
        assert_that(schema, lookup(index, hot_keys[i], schema)).is_equal_to(hot_mutations[i].partition());
    }

    BOOST_REQUIRE(index.get(grown_keys.back()));
    index.drain_cache().get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_scan_resumes_across_batches_by_token) {
    auto schema = make_logstor_schema();
    primary_index index(schema, nullptr);

    insert_same_token_keys(index, *schema, 7, 0, 2);
    insert_same_token_keys(index, *schema, 11, 100, 3);
    insert_same_token_keys(index, *schema, 19, 200, 2);

    auto scan = index.scan();

    auto batch0 = scan.next_batch(3);
    BOOST_REQUIRE(batch0);
    BOOST_REQUIRE_EQUAL(batch0->first_token, dht::token::from_int64(7));
    BOOST_REQUIRE_EQUAL(batch0->last_token, dht::token::from_int64(7));
    BOOST_REQUIRE_EQUAL(batch0->entry_count, 2u);
    BOOST_REQUIRE(!batch0->exhausted);

    auto batch1 = scan.next_batch(3);
    BOOST_REQUIRE(batch1);
    BOOST_REQUIRE_EQUAL(batch1->first_token, dht::token::from_int64(11));
    BOOST_REQUIRE_EQUAL(batch1->last_token, dht::token::from_int64(11));
    BOOST_REQUIRE_EQUAL(batch1->entry_count, 3u);
    BOOST_REQUIRE(!batch1->exhausted);

    auto batch2 = scan.next_batch(3);
    BOOST_REQUIRE(batch2);
    BOOST_REQUIRE_EQUAL(batch2->first_token, dht::token::from_int64(19));
    BOOST_REQUIRE_EQUAL(batch2->last_token, dht::token::from_int64(19));
    BOOST_REQUIRE_EQUAL(batch2->entry_count, 2u);
    BOOST_REQUIRE(batch2->exhausted);

    BOOST_REQUIRE(!scan.next_batch(3));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_scan_keeps_oversized_same_token_batch_intact) {
    auto schema = make_logstor_schema();
    primary_index index(schema, nullptr);

    insert_same_token_keys(index, *schema, 23, 0, 5);
    insert_same_token_keys(index, *schema, 29, 100, 1);

    auto scan = index.scan();

    auto batch0 = scan.next_batch(2);
    BOOST_REQUIRE(batch0);
    BOOST_REQUIRE_EQUAL(batch0->first_token, dht::token::from_int64(23));
    BOOST_REQUIRE_EQUAL(batch0->last_token, dht::token::from_int64(23));
    BOOST_REQUIRE_EQUAL(batch0->entry_count, 5u);
    BOOST_REQUIRE(!batch0->exhausted);

    auto batch1 = scan.next_batch(2);
    BOOST_REQUIRE(batch1);
    BOOST_REQUIRE_EQUAL(batch1->first_token, dht::token::from_int64(29));
    BOOST_REQUIRE_EQUAL(batch1->last_token, dht::token::from_int64(29));
    BOOST_REQUIRE_EQUAL(batch1->entry_count, 1u);
    BOOST_REQUIRE(batch1->exhausted);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_survives_lsa_compaction_before_exchange) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(schema, &cache.logstor_tracker);

    constexpr int64_t fixed_token = 19;
    auto keys = insert_same_token_keys(index, *schema, fixed_token, 20'000, 8);

    std::vector<mutation> mutations;
    mutations.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto m = make_mutation(schema, format("fixed-pk-{:06d}", 20'000 + i), format("compaction-v{}", i));
        populate(index, keys[i], m);
        mutations.push_back(std::move(m));
    }

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        assert_that(schema, lookup(index, keys[i], schema)).is_equal_to(mutations[i].partition());
    }

    // Create fragmentation so full_compaction() has live cached objects to move.
    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    BOOST_REQUIRE(evict_one(cache.logstor_tracker));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + 2);

    cache.logstor_tracker.region().full_compaction();

    for (size_t i = 2; i < keys.size(); ++i) {
        BOOST_REQUIRE(lookup_exists(index, keys[i], schema));
        assert_that(schema, lookup(index, keys[i], schema)).is_equal_to(mutations[i].partition());
    }

    auto [replaced, old_entry] = index.insert(keys[2], make_index_entry(900, 0, 32, 2));
    BOOST_REQUIRE(replaced);
    BOOST_REQUIRE(old_entry);
    BOOST_REQUIRE(!lookup_exists(index, keys[2], schema));

    auto replacement = make_mutation(schema, "fixed-pk-020002", "compaction-replaced");
    populate(index, keys[2], replacement);
    assert_that(schema, lookup(index, keys[2], schema)).is_equal_to(replacement.partition());
    index.drain_cache().get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_upgrades_cached_partition_after_schema_change) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(schema, &cache.logstor_tracker);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).second);

    auto original = make_mutation(schema, "pk0", "v0");
    populate(index, key, original);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 1);

    auto new_schema = schema_builder(schema).with_column("v2", utf8_type).build();
    index.set_schema(new_schema);

    auto hits_before = cache.shared_tracker.get_stats().partition_hits;
    BOOST_REQUIRE(lookup_exists(index, key, new_schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_hits, hits_before + 1);

    auto cached = lookup(index, key, new_schema);
    auto expected = original;
    expected.upgrade(new_schema);
    assert_that(new_schema, cached).is_equal_to(expected.partition());
    index.drain_cache().get();
}
