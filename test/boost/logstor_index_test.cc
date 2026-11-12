/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// combined_tests has the main function.
#undef SEASTAR_TESTING_MAIN

#include <boost/test/unit_test.hpp>

#include "replica/logstor/types.hh"
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/index.hh"
#include "replica/logstor/key_utils.hh"
#include "replica/logstor/single_row_partition.hh"
#include "db/cache_tracker.hh"
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>
#include <variant>
#include "schema/schema_builder.hh"
#include "test/lib/log.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/mutation_assertions.hh"

using namespace replica::logstor;
using namespace tests::logstor;

BOOST_AUTO_TEST_SUITE(logstor_index_test)

namespace {

schema_ptr make_logstor_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

dht::decorated_key make_decorated_key(const schema& schema, sstring pk) {
    return dht::decorate_key(schema, partition_key::from_single_value(schema, serialized(std::move(pk))));
}

primary_index_key make_primary_index_key(const schema& schema, sstring pk) {
    return primary_index_key{make_decorated_key(schema, std::move(pk))};
}

primary_index_key make_fixed_token_key(const schema& schema, int64_t token, sstring pk) {
    auto pkey = partition_key::from_single_value(schema, serialized(pk));
    auto dk = dht::decorated_key(dht::token::from_int64(token), std::move(pkey));
    return primary_index_key{dk};
}

index_entry make_index_entry(uint32_t segment, uint32_t offset, uint32_t size, api::timestamp_type timestamp) {
    return index_entry{
        .location = record_location{
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
    auto where = lookup_guarded_location(index, key, *m.schema());
    BOOST_REQUIRE(where);
    BOOST_REQUIRE(index.populate_cache(where->guard, key, where->location, m.schema(), m.key(), to_single_row_partition(mutation(m))));
}

mutation lookup_mutation(primary_index& index, const primary_index_key& key, schema_ptr schema) {
    auto result = index.lookup_for_read(key, *schema, use_cache::yes);
    BOOST_REQUIRE(result);
    auto* cached = std::get_if<keyed_single_row_partition>(&*result);
    BOOST_REQUIRE(cached);
    return to_mutation(std::move(schema), std::move(cached->key), std::move(cached->partition));
}

mutation_partition lookup(primary_index& index, const primary_index_key& key, schema_ptr schema) {
    return std::move(lookup_mutation(index, key, std::move(schema)).partition());
}

bool lookup_exists(primary_index& index, const primary_index_key& key, schema_ptr schema) {
    auto result = index.lookup_for_read(key, *schema, use_cache::yes);
    BOOST_REQUIRE(result);
    return std::holds_alternative<keyed_single_row_partition>(*result);
}

bool evict_one(shared_logstor_cache& cache) {
    return cache.shared_tracker.region().evict_some() == seastar::memory::reclaiming_result::reclaimed_something;
}

std::vector<primary_index_key> insert_same_token_keys(primary_index& index, const schema& schema, int64_t token, int start, int count) {
    std::vector<primary_index_key> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        auto key = make_fixed_token_key(schema, token, format("fixed-pk-{:06d}", start + i));
        auto outcome = index.insert(key, make_index_entry(500 + i, i * 8, 8, 1));
        BOOST_REQUIRE(outcome.inserted() && !outcome.previous_entry);
        keys.push_back(std::move(key));
    }
    return keys;
}

}

// Checks that the index key hashes the internal representation of the partition key and keeps its token.
SEASTAR_THREAD_TEST_CASE(test_logstor_index_key_hashes_key_representation) {
    auto schema = make_logstor_schema();
    auto dk0 = make_decorated_key(*schema, "pk0");
    auto dk1 = make_decorated_key(*schema, "pk1");

    auto key0 = primary_index_key(dk0);
    BOOST_REQUIRE_EQUAL(key0.token(), dk0.token());
    BOOST_REQUIRE(key0.hash() == compute_key_hash(dk0.key().representation()));
    BOOST_REQUIRE(key0 == primary_index_key(dk0));

    auto key1 = primary_index_key(dk1);
    BOOST_REQUIRE(key1.hash() != key0.hash());
    BOOST_REQUIRE(key1 != key0);
}

// Checks that primary_index accounting callbacks track live bytes across
// inserts, overwrites, relocations, erases, range erases, and clear().
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_space_accounting) {
    auto schema = make_logstor_schema();
    struct accounting_subscriber : space_accounting_subscriber {
        ssize_t live_bytes = 0;
        size_t add_calls = 0;
        size_t free_calls = 0;
        size_t move_calls = 0;
        std::vector<record_location> added_locations;
        std::vector<record_location> freed_locations;

        bool is_live(record_location loc) const {
            return std::count(added_locations.begin(), added_locations.end(), loc)
                 > std::count(freed_locations.begin(), freed_locations.end(), loc);
        }

        size_t live_location_count() const {
            return std::count_if(added_locations.begin(), added_locations.end(), [&] (record_location loc) {
                return is_live(loc);
            });
        }

        void on_add_record(record_location loc) noexcept override {
            live_bytes += loc.size;
            ++add_calls;
            added_locations.push_back(loc);
        }

        void on_free_record(record_location loc) noexcept override {
            live_bytes -= loc.size;
            ++free_calls;
            BOOST_REQUIRE(is_live(loc));
            freed_locations.push_back(loc);
        }

        void on_move_record(record_location from, record_location to) noexcept override {
            live_bytes += ssize_t(to.size) - ssize_t(from.size);
            ++move_calls;
            BOOST_REQUIRE(is_live(from));
            freed_locations.push_back(from);
            added_locations.push_back(to);
        }
    } accounting;

    primary_index index(accounting, nullptr, false);

    // The index counts the bytes of the records it points at for the table it belongs to, which is
    // the same number the subscriber accumulates, so the two must agree after every operation.
    auto check_live_bytes = [&] (ssize_t expected) {
        BOOST_REQUIRE_EQUAL(accounting.live_bytes, expected);
        BOOST_REQUIRE_EQUAL(index.get_usage().live_record_bytes, uint64_t(expected));
    };

    const auto pk0 = make_primary_index_key(*schema, "pk0");
    const auto pk1 = make_primary_index_key(*schema, "pk1");
    const auto pk2 = make_primary_index_key(*schema, "pk2");

    const record_location loc0{.segment = log_segment_id{1}, .offset = 0, .size = 11};
    const record_location loc0_old{.segment = log_segment_id{1}, .offset = 16, .size = 7};
    const record_location loc1{.segment = log_segment_id{2}, .offset = 0, .size = 17};
    const record_location loc2{.segment = log_segment_id{3}, .offset = 0, .size = 13};
    const record_location loc3{.segment = log_segment_id{4}, .offset = 0, .size = 19};
    const record_location loc4{.segment = log_segment_id{5}, .offset = 0, .size = 23};
    const record_location loc5{.segment = log_segment_id{6}, .offset = 0, .size = 29};

    // insert(pk0, loc0): new entry, succeeds, no previous entry to free  →  {pk0: loc0}
    auto outcome0 = index.insert(pk0, index_entry{.location = loc0, .timestamp = api::timestamp_type(10)});
    BOOST_REQUIRE(outcome0.inserted());
    BOOST_REQUIRE(!outcome0.previous_entry);
    check_live_bytes(ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc0_old): older timestamp, rejected, no accounting change  →  {pk0: loc0}
    auto outcome_old = index.insert(pk0, index_entry{.location = loc0_old, .timestamp = api::timestamp_type(9)});
    BOOST_REQUIRE(outcome_old.result == primary_index::insert_result::superseded);
    BOOST_REQUIRE(outcome_old.previous_entry);
    BOOST_REQUIRE(outcome_old.previous_entry->location == loc0);
    BOOST_REQUIRE_EQUAL(outcome_old.previous_entry->timestamp, api::timestamp_type(10));
    check_live_bytes(ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc1): newer timestamp, replaces loc0, old location freed via accounting  →  {pk0: loc1}
    auto outcome1 = index.insert(pk0, index_entry{.location = loc1, .timestamp = api::timestamp_type(11)});
    BOOST_REQUIRE(outcome1.inserted());
    BOOST_REQUIRE(outcome1.previous_entry);
    BOOST_REQUIRE(outcome1.previous_entry->location == loc0);
    BOOST_REQUIRE_EQUAL(outcome1.previous_entry->timestamp, api::timestamp_type(10));
    check_live_bytes(ssize_t(loc1.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(!accounting.is_live(loc0));
    BOOST_REQUIRE(accounting.added_locations.back() == loc1);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc0);

    // insert(pk1, loc2): new key, succeeds, adds to live bytes  →  {pk0: loc1, pk1: loc2}
    auto outcome2 = index.insert(pk1, index_entry{.location = loc2, .timestamp = api::timestamp_type(7)});
    BOOST_REQUIRE(outcome2.inserted());
    BOOST_REQUIRE(!outcome2.previous_entry);
    check_live_bytes(ssize_t(loc1.size + loc2.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(accounting.is_live(loc2));

    // erase(pk1, loc1): location mismatch, returns false, no accounting change  →  {pk0: loc1, pk1: loc2}
    BOOST_REQUIRE(!index.erase(pk1, loc1));
    check_live_bytes(ssize_t(loc1.size + loc2.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);

    // update_record_location(pk0, loc1 -> loc3): old location matches, succeeds, moves the record from loc1 to loc3  →  {pk0: loc3, pk1: loc2}
    BOOST_REQUIRE(index.update_record_location(pk0, loc1, loc3));
    check_live_bytes(ssize_t(loc2.size + loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.move_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(accounting.is_live(loc2));
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(!accounting.is_live(loc1));
    BOOST_REQUIRE(accounting.added_locations.back() == loc3);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc1);

    // update_record_location(pk0, loc1 -> loc4): old location no longer current, fails, no accounting change  →  {pk0: loc3, pk1: loc2}
    BOOST_REQUIRE(!index.update_record_location(pk0, loc1, loc4));
    check_live_bytes(ssize_t(loc2.size + loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.move_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);

    // erase(pk1, loc2): location matches, succeeds, frees loc2  →  {pk0: loc3}
    BOOST_REQUIRE(index.erase(pk1, loc2));
    check_live_bytes(ssize_t(loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(!accounting.is_live(loc2));
    BOOST_REQUIRE(accounting.freed_locations.back() == loc2);

    // insert(pk1, loc4): new entry for pk1 (previously erased), succeeds  →  {pk0: loc3, pk1: loc4}
    auto outcome4 = index.insert(pk1, index_entry{.location = loc4, .timestamp = api::timestamp_type(12)});
    BOOST_REQUIRE(outcome4.inserted());
    BOOST_REQUIRE(!outcome4.previous_entry);
    // insert(pk2, loc5): new key, succeeds  →  {pk0: loc3, pk1: loc4, pk2: loc5}
    auto outcome5 = index.insert(pk2, index_entry{.location = loc5, .timestamp = api::timestamp_type(13)});
    BOOST_REQUIRE(outcome5.inserted());
    BOOST_REQUIRE(!outcome5.previous_entry);
    check_live_bytes(ssize_t(loc3.size + loc4.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 3);

    // range erase pk1: removes pk1 entry (loc4), frees via accounting  →  {pk0: loc3, pk2: loc5}
    dht::token_range pk1_range(
            std::optional(interval_bound(pk1.token(), true)),
            std::optional(interval_bound(pk1.token(), true)));
    index.erase(pk1_range).get();
    check_live_bytes(ssize_t(loc3.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(!accounting.is_live(loc4));
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(accounting.is_live(loc5));
    BOOST_REQUIRE(accounting.freed_locations.back() == loc4);

    // clear(): removes all remaining entries (pk0->loc3, pk2->loc5), all freed  →  {}
    index.clear().get();
    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, 0u);
    check_live_bytes(0);
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.move_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 0);
    BOOST_REQUIRE_EQUAL(accounting.freed_locations.size(), 6u);
}

// Checks that range erase and clear() account for freed log locations correctly.
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_range_erase_and_clear_space_accounting) {
    auto schema = make_logstor_schema();
    struct accounting_subscriber : space_accounting_subscriber {
        ssize_t live_bytes = 0;
        size_t add_calls = 0;
        size_t free_calls = 0;
        std::vector<record_location> freed_locations;

        void on_add_record(record_location loc) noexcept override {
            live_bytes += loc.size;
            ++add_calls;
        }

        void on_free_record(record_location loc) noexcept override {
            live_bytes -= loc.size;
            ++free_calls;
            freed_locations.push_back(loc);
        }

        void on_move_record(record_location from, record_location to) noexcept override {
            live_bytes += ssize_t(to.size) - ssize_t(from.size);
        }
    } accounting;

    primary_index index(accounting, nullptr, false);

    auto check_live_bytes = [&] (ssize_t expected) {
        BOOST_REQUIRE_EQUAL(accounting.live_bytes, expected);
        BOOST_REQUIRE_EQUAL(index.get_usage().live_record_bytes, uint64_t(expected));
    };

    struct entry {
        primary_index_key key;
        record_location loc;
    };

    std::vector<entry> entries = {
        { make_primary_index_key(*schema, "pk0"), {.segment = log_segment_id{11}, .offset = 0, .size = 5} },
        { make_primary_index_key(*schema, "pk1"), {.segment = log_segment_id{12}, .offset = 0, .size = 7} },
        { make_primary_index_key(*schema, "pk2"), {.segment = log_segment_id{13}, .offset = 0, .size = 11} },
        { make_primary_index_key(*schema, "pk3"), {.segment = log_segment_id{14}, .offset = 0, .size = 13} },
        { make_primary_index_key(*schema, "pk4"), {.segment = log_segment_id{15}, .offset = 0, .size = 17} },
    };

    std::sort(entries.begin(), entries.end(), [&] (const entry& a, const entry& b) {
        return a.key.token() < b.key.token();
    });

    auto insert = [&] (const entry& e, api::timestamp_type ts) {
        auto outcome = index.insert(e.key, index_entry{.location = e.loc, .timestamp = ts});
        BOOST_REQUIRE(outcome.inserted());
        BOOST_REQUIRE(!outcome.previous_entry);
    };

    insert(entries[0], api::timestamp_type(10));
    insert(entries[1], api::timestamp_type(11));
    insert(entries[2], api::timestamp_type(12));
    insert(entries[3], api::timestamp_type(13));
    insert(entries[4], api::timestamp_type(14));

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    check_live_bytes(ssize_t(entries[0].loc.size + entries[1].loc.size + entries[2].loc.size + entries[3].loc.size + entries[4].loc.size));

    dht::token_range range(
            std::optional(interval_bound(entries[1].key.token(), true)),
            std::optional(interval_bound(entries[3].key.token(), true)));
    index.erase(range).get();

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    check_live_bytes(ssize_t(entries[0].loc.size + entries[4].loc.size));
    BOOST_REQUIRE(lookup_location(index, entries[0].key, *schema).has_value());
    BOOST_REQUIRE(!lookup_location(index, entries[1].key, *schema).has_value());
    BOOST_REQUIRE(!lookup_location(index, entries[2].key, *schema).has_value());
    BOOST_REQUIRE(!lookup_location(index, entries[3].key, *schema).has_value());
    BOOST_REQUIRE(lookup_location(index, entries[4].key, *schema).has_value());
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[1].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[2].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[3].loc), 1);

    index.clear().get();

    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, 0u);
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 5u);
    check_live_bytes(0);
    BOOST_REQUIRE_EQUAL(accounting.freed_locations.size(), 5u);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[0].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[4].loc), 1);
}

// The `memory_usage` of an index counts the B+tree, and its `entries_memory_usage` does not, so the
// first has to be the larger of the two, and it has to follow the index up and back down.
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_memory_usage) {
    auto schema = make_logstor_schema();
    primary_index index(noop_space_accounting, nullptr, false);

    // The tree builds its root on the first insert, so an untouched index holds nothing.
    BOOST_REQUIRE_EQUAL(index.get_usage().entries_memory_usage, 0u);
    BOOST_REQUIRE_EQUAL(index.get_usage().memory_usage, 0u);

    constexpr size_t key_count = 2000;
    std::vector<primary_index_key> keys;
    keys.reserve(key_count);
    for (size_t i = 0; i < key_count; ++i) {
        keys.push_back(make_primary_index_key(*schema, format("pk{:06d}", i)));
    }

    for (size_t i = 0; i < key_count; ++i) {
        index.insert(keys[i], index_entry{
                .location = record_location{.segment = log_segment_id{1}, .offset = uint32_t(i * 64), .size = 32},
                .timestamp = api::timestamp_type(i + 1)});
    }
    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, key_count);

    const size_t entries = index.get_usage().entries_memory_usage;
    const size_t allocated = index.get_usage().memory_usage;
    BOOST_REQUIRE_GT(entries, 0u);
    BOOST_REQUIRE_GT(allocated, entries);
    testlog.info("primary index over {} keys: entries {} bytes, allocated {} bytes", key_count, entries, allocated);

    // Erasing gives back both.
    for (size_t i = 0; i < key_count / 2; ++i) {
        BOOST_REQUIRE(index.erase(keys[i],
                record_location{.segment = log_segment_id{1}, .offset = uint32_t(i * 64), .size = 32}));
    }
    BOOST_REQUIRE_LT(index.get_usage().entries_memory_usage, entries);
    BOOST_REQUIRE_LT(index.get_usage().memory_usage, allocated);
    BOOST_REQUIRE_GT(index.get_usage().memory_usage, index.get_usage().entries_memory_usage);

    // Clearing gives back everything but the root, which an element-wise erase leaves standing.
    // More than that left behind would mean the accounting had drifted.
    index.clear().get();
    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, 0u);
    BOOST_REQUIRE_EQUAL(index.get_usage().entries_memory_usage, 0u);
    const size_t after_clear = index.get_usage().memory_usage;
    testlog.info("primary index after clear: allocated {} bytes", after_clear);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_cache_invalidation_and_eviction) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key0 = make_primary_index_key(*schema, "pk0");
    auto key1 = make_primary_index_key(*schema, "pk1");
    auto key2 = make_primary_index_key(*schema, "pk2");
    auto key3 = make_primary_index_key(*schema, "pk3");

    BOOST_REQUIRE(!index.insert(key0, make_index_entry(1, 0, 10, 1)).previous_entry);
    BOOST_REQUIRE(!index.insert(key1, make_index_entry(1, 10, 10, 1)).previous_entry);
    BOOST_REQUIRE(!index.insert(key2, make_index_entry(1, 20, 10, 1)).previous_entry);
    BOOST_REQUIRE(!index.insert(key3, make_index_entry(1, 30, 10, 1)).previous_entry);

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

    auto outcome1 = index.insert(key1, make_index_entry(2, 10, 12, 2));
    BOOST_REQUIRE(outcome1.inserted());
    BOOST_REQUIRE(outcome1.previous_entry);
    BOOST_REQUIRE(!lookup_exists(index, key1, schema));

    auto outcome2 = index.insert(key2, make_index_entry(2, 20, 12, 2));
    BOOST_REQUIRE(outcome2.inserted());
    BOOST_REQUIRE(outcome2.previous_entry);
    BOOST_REQUIRE(!lookup_exists(index, key2, schema));

    auto outcome0 = index.insert(key0, make_index_entry(3, 0, 14, 0));
    BOOST_REQUIRE(outcome0.result == primary_index::insert_result::superseded);
    BOOST_REQUIRE(outcome0.previous_entry);
    BOOST_REQUIRE(lookup_exists(index, key0, schema));

    BOOST_REQUIRE(index.erase(key1, make_index_entry(1, 10, 10, 1).location) == false);
    BOOST_REQUIRE(index.erase(key1, make_index_entry(2, 10, 12, 2).location));
    BOOST_REQUIRE(!lookup_location(index, key1, *schema));

    populate(index, key2, mut2);
    populate(index, key3, mut3);

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 5);
    BOOST_REQUIRE(lookup_exists(index, key0, schema));
    BOOST_REQUIRE(lookup_exists(index, key2, schema));
    BOOST_REQUIRE(lookup_exists(index, key3, schema));

    BOOST_REQUIRE(lookup_exists(index, key2, schema));
    BOOST_REQUIRE(lookup_exists(index, key3, schema));

    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    BOOST_REQUIRE(evict_one(cache));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + 1);

    auto cached_after_eviction = int(lookup_exists(index, key0, schema))
        + int(lookup_location(index, key2, *schema) && lookup_exists(index, key2, schema))
        + int(lookup_exists(index, key3, schema));
    BOOST_REQUIRE_EQUAL(cached_after_eviction, 2);

    populate(index, key0, mut0);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 6);
    assert_that(schema, lookup(index, key0, schema)).is_equal_to(mut0.partition());

    BOOST_REQUIRE(index.erase(key2, make_index_entry(2, 20, 12, 2).location));
    BOOST_REQUIRE(!lookup_location(index, key2, *schema));

    BOOST_REQUIRE(lookup_location(index, key0, *schema));
    BOOST_REQUIRE(lookup_location(index, key3, *schema));

    index.clear().get();
    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, 0u);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_drain_cache_preserves_index_entries) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key0 = make_primary_index_key(*schema, "pk0");
    auto key1 = make_primary_index_key(*schema, "pk1");

    BOOST_REQUIRE(!index.insert(key0, make_index_entry(1, 0, 10, 1)).previous_entry);
    BOOST_REQUIRE(!index.insert(key1, make_index_entry(1, 10, 10, 1)).previous_entry);

    auto mut0 = make_mutation(schema, "pk0", "v0");
    auto mut1 = make_mutation(schema, "pk1", "v1");
    populate(index, key0, mut0);
    populate(index, key1, mut1);

    BOOST_REQUIRE(lookup_exists(index, key0, schema));
    BOOST_REQUIRE(lookup_exists(index, key1, schema));

    index.drain_cache().get();

    BOOST_REQUIRE(lookup_location(index, key0, *schema));
    BOOST_REQUIRE(lookup_location(index, key1, *schema));
    BOOST_REQUIRE(!lookup_exists(index, key0, schema));
    BOOST_REQUIRE(!lookup_exists(index, key1, schema));
}

// Checks that the destructor of the index evicts the cached partitions of its entries, so that an
// index can be destroyed without drain_cache() first.
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_destructor_evicts_cached_partitions) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    std::optional<primary_index> index(std::in_place, noop_space_accounting, &cache.logstor_tracker, true);

    auto key0 = make_primary_index_key(*schema, "pk0");
    auto key1 = make_primary_index_key(*schema, "pk1");
    index->insert(key0, make_index_entry(1, 0, 10, 1));
    index->insert(key1, make_index_entry(1, 10, 10, 1));
    populate(*index, key0, make_mutation(schema, "pk0", "v0"));
    populate(*index, key1, make_mutation(schema, "pk1", "v1"));

    const auto& stats = cache.shared_tracker.get_stats();
    BOOST_REQUIRE_EQUAL(stats.partitions, 2u);
    const auto removals_before = stats.partition_removals;

    index.reset();

    BOOST_REQUIRE_EQUAL(stats.partitions, 0u);
    BOOST_REQUIRE_EQUAL(stats.partition_removals, removals_before + 2);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_cache_survives_index_rebalancing) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

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
    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, hot_keys.size() + grown_keys.size());

    for (int i = 0; i < 48; ++i) {
        BOOST_REQUIRE(index.erase(grown_keys[i], make_index_entry(500 + i, i * 8, 8, 1).location));
    }

    BOOST_REQUIRE_EQUAL(index.get_usage().key_count, hot_keys.size() + grown_keys.size() - 48);

    for (size_t i = 0; i < hot_keys.size(); ++i) {
        BOOST_REQUIRE(lookup_exists(index, hot_keys[i], schema));
        assert_that(schema, lookup(index, hot_keys[i], schema)).is_equal_to(hot_mutations[i].partition());
    }

    auto evictions_before = cache.shared_tracker.get_stats().partition_evictions;
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        BOOST_REQUIRE(evict_one(cache));
    }
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + hot_keys.size());

    auto insertions_before_repopulate = cache.shared_tracker.get_stats().partition_insertions;
    for (size_t i = 0; i < hot_keys.size(); ++i) {
        populate(index, hot_keys[i], hot_mutations[i]);
    }
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, insertions_before_repopulate + hot_keys.size());

    for (size_t i = 0; i < hot_keys.size(); ++i) {
        BOOST_REQUIRE(lookup_location(index, hot_keys[i], *schema));
        assert_that(schema, lookup(index, hot_keys[i], schema)).is_equal_to(hot_mutations[i].partition());
    }

    BOOST_REQUIRE(lookup_location(index, grown_keys.back(), *schema));
    index.drain_cache().get();
}

// Returns a visit that counts the entries of a batch into `count`, for the scan tests that look
// only at what a batch covered.
static auto count_visit(size_t& count) {
    return [&count] (const primary_index::scanned_entry&) { ++count; };
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_scan_resumes_across_batches_by_token) {
    auto schema = make_logstor_schema();
    primary_index index(noop_space_accounting, nullptr, false);

    insert_same_token_keys(index, *schema, 7, 0, 2);
    insert_same_token_keys(index, *schema, 11, 100, 3);
    insert_same_token_keys(index, *schema, 19, 200, 2);

    auto scan = index.scan();
    size_t visited = 0;

    auto batch0 = scan.next_batch(3, count_visit(visited));
    BOOST_REQUIRE(batch0);
    BOOST_REQUIRE_EQUAL(batch0->first_token, dht::token::from_int64(7));
    BOOST_REQUIRE_EQUAL(batch0->last_token, dht::token::from_int64(7));
    BOOST_REQUIRE_EQUAL(visited, 2u);

    visited = 0;
    auto batch1 = scan.next_batch(3, count_visit(visited));
    BOOST_REQUIRE(batch1);
    BOOST_REQUIRE_EQUAL(batch1->first_token, dht::token::from_int64(11));
    BOOST_REQUIRE_EQUAL(batch1->last_token, dht::token::from_int64(11));
    BOOST_REQUIRE_EQUAL(visited, 3u);

    visited = 0;
    auto batch2 = scan.next_batch(3, count_visit(visited));
    BOOST_REQUIRE(batch2);
    BOOST_REQUIRE_EQUAL(batch2->first_token, dht::token::from_int64(19));
    BOOST_REQUIRE_EQUAL(batch2->last_token, dht::token::from_int64(19));
    BOOST_REQUIRE_EQUAL(visited, 2u);

    BOOST_REQUIRE(!scan.next_batch(3, count_visit(visited)));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_scan_keeps_oversized_same_token_batch_intact) {
    auto schema = make_logstor_schema();
    primary_index index(noop_space_accounting, nullptr, false);

    insert_same_token_keys(index, *schema, 23, 0, 5);
    insert_same_token_keys(index, *schema, 29, 100, 1);

    auto scan = index.scan();
    size_t visited = 0;

    auto batch0 = scan.next_batch(2, count_visit(visited));
    BOOST_REQUIRE(batch0);
    BOOST_REQUIRE_EQUAL(batch0->first_token, dht::token::from_int64(23));
    BOOST_REQUIRE_EQUAL(batch0->last_token, dht::token::from_int64(23));
    BOOST_REQUIRE_EQUAL(visited, 5u);

    visited = 0;
    auto batch1 = scan.next_batch(2, count_visit(visited));
    BOOST_REQUIRE(batch1);
    BOOST_REQUIRE_EQUAL(batch1->first_token, dht::token::from_int64(29));
    BOOST_REQUIRE_EQUAL(batch1->last_token, dht::token::from_int64(29));
    BOOST_REQUIRE_EQUAL(visited, 1u);

    BOOST_REQUIRE(!scan.next_batch(2, count_visit(visited)));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_survives_lsa_compaction_before_exchange) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

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
    BOOST_REQUIRE(evict_one(cache));
    BOOST_REQUIRE(evict_one(cache));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_evictions, evictions_before + 2);

    cache.shared_tracker.region().full_compaction();

    for (size_t i = 2; i < keys.size(); ++i) {
        BOOST_REQUIRE(lookup_exists(index, keys[i], schema));
        assert_that(schema, lookup(index, keys[i], schema)).is_equal_to(mutations[i].partition());
    }

    auto replace_outcome = index.insert(keys[2], make_index_entry(900, 0, 32, 2));
    BOOST_REQUIRE(replace_outcome.inserted());
    BOOST_REQUIRE(replace_outcome.previous_entry);
    BOOST_REQUIRE(!lookup_exists(index, keys[2], schema));

    auto replacement = make_mutation(schema, "fixed-pk-020002", "compaction-replaced");
    populate(index, keys[2], replacement);
    assert_that(schema, lookup(index, keys[2], schema)).is_equal_to(replacement.partition());
    index.drain_cache().get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_upgrades_cached_partition_after_schema_change) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    auto original = make_mutation(schema, "pk0", "v0");
    populate(index, key, original);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_insertions, 1);

    auto new_schema = schema_builder(schema).with_column("v2", utf8_type).build();

    auto hits_before = cache.shared_tracker.get_stats().partition_hits;
    BOOST_REQUIRE(lookup_exists(index, key, new_schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_hits, hits_before + 1);

    auto cached = lookup(index, key, new_schema);
    auto expected = original;
    expected.upgrade(new_schema);
    assert_that(new_schema, cached).is_equal_to(expected.partition());
    index.drain_cache().get();
}

static mutation to_mutation(schema_ptr schema, sstring pk, single_row_partition&& partition) {
    auto dk = make_decorated_key(*schema, std::move(pk));
    return to_mutation(std::move(schema), std::move(dk), std::move(partition));
}

// to_single_row_partition() takes the one row out of a mutation and to_mutation() puts it back: the
// round trip is the identity for every shape a partition takes.
SEASTAR_THREAD_TEST_CASE(test_logstor_single_row_partition_round_trips_every_partition_shape) {
    auto schema = make_logstor_schema();
    const auto ts = api::timestamp_type(7);
    const auto now = gc_clock::now();
    const auto& v_def = *schema->get_column_definition("v");

    // The shapes a logstor partition takes: a live row, a row with a tombstone older than its marker,
    // a row whose marker and cell expire, a row whose marker is dead, and a partition that is only a
    // tombstone.
    std::vector<mutation> ms;

    ms.push_back(make_mutation(schema, "live", "v0"));

    auto shadowed = make_mutation(schema, "shadowed", "v0");
    shadowed.partition().clustered_row(*schema, clustering_key::make_empty()).apply(row_tombstone(tombstone(api::timestamp_type(0), now)));
    ms.push_back(std::move(shadowed));

    mutation expiring(schema, make_decorated_key(*schema, "expiring"));
    {
        auto& row = expiring.partition().clustered_row(*schema, clustering_key::make_empty());
        row.apply(row_marker(ts, std::chrono::hours(1), now + std::chrono::hours(1)));
        row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, ts, serialized(sstring("v0")),
                now + std::chrono::hours(1), std::chrono::hours(1)));
    }
    ms.push_back(std::move(expiring));

    mutation dead_marker(schema, make_decorated_key(*schema, "dead"));
    dead_marker.partition().clustered_row(*schema, clustering_key::make_empty()).apply(row_marker(tombstone(ts, now)));
    ms.push_back(std::move(dead_marker));

    mutation deleted(schema, make_decorated_key(*schema, "deleted"));
    deleted.partition().apply(tombstone(ts, now));
    ms.push_back(std::move(deleted));

    for (const auto& m : ms) {
        auto partition = to_single_row_partition(mutation(m));
        BOOST_REQUIRE(partition.partition_tombstone == m.partition().partition_tombstone());
        BOOST_REQUIRE_EQUAL(partition.row.has_value(), !m.partition().clustered_rows().empty());
        assert_that(to_mutation(schema, m.decorated_key(), std::move(partition))).is_equal_to(m);
    }
}

// A partition of a shape a logstor schema cannot produce is an internal error, not a fallback.
SEASTAR_THREAD_TEST_CASE(test_logstor_single_row_partition_rejects_a_partition_of_another_shape) {
    const auto abort_ie = set_abort_on_internal_error(false);
    auto reset_abort_ie = defer([abort_ie] noexcept {
        set_abort_on_internal_error(abort_ie);
    });
    auto schema = make_logstor_schema();
    auto m = make_mutation(schema, "pk0", "v0");
    m.partition().ensure_last_dummy(*schema);
    BOOST_REQUIRE_THROW(to_single_row_partition(std::move(m)), std::runtime_error);
}

// Reads the partition of `pk` as a point read does: by the hash of the key, asking for the key
// itself.
static std::optional<single_row_partition> read_partition(primary_index& index, sstring pk, const schema& schema) {
    auto dk = make_decorated_key(schema, std::move(pk));
    auto result = index.lookup_for_read(primary_index_key(dk), schema, use_cache::yes);
    BOOST_REQUIRE(result);
    if (auto* cached = std::get_if<keyed_single_row_partition>(&*result)) {
        BOOST_REQUIRE(cached->key.equal(schema, dk));
        return std::move(cached->partition);
    }
    return std::nullopt;
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_read_partition_hands_out_the_row) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    // A row with a marker, a live cell and a row tombstone older than both, so that every part of
    // a row is there to be handed out.
    auto m = make_mutation(schema, "pk0", "v0");
    m.partition().clustered_row(*schema, clustering_key::make_empty()).apply(row_tombstone(tombstone(api::timestamp_type(0), gc_clock::now())));
    populate(index, key, m);

    const auto hits_before = cache.shared_tracker.get_stats().partition_hits;
    auto partition = read_partition(index, "pk0", *schema);
    BOOST_REQUIRE(partition);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_hits, hits_before + 1);
    BOOST_REQUIRE(!partition->partition_tombstone);
    BOOST_REQUIRE(partition->row);
    BOOST_REQUIRE(partition->row->marker().is_live());
    BOOST_REQUIRE_EQUAL(partition->row->cells().size(), 1);
    assert_that(to_mutation(schema, "pk0", std::move(*partition))).is_equal_to(m);
    assert_that(to_mutation(schema, "pk0", *read_partition(index, "pk0", *schema))).is_equal_to(lookup_mutation(index, key, schema));
    index.drain_cache().get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_read_partition_of_a_tombstone_has_no_row) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    auto deleted = make_mutation(schema, "pk0", "v0");
    deleted.partition() = mutation_partition(*schema);
    deleted.partition().apply(tombstone(api::timestamp_type(2), gc_clock::now()));
    populate(index, key, deleted);

    auto partition = read_partition(index, "pk0", *schema);
    BOOST_REQUIRE(partition);
    BOOST_REQUIRE(partition->partition_tombstone == deleted.partition().partition_tombstone());
    BOOST_REQUIRE(!partition->row);
    assert_that(to_mutation(schema, "pk0", std::move(*partition))).is_equal_to(deleted);
    index.drain_cache().get();
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_read_partition_counts_a_miss) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    const auto misses_before = cache.shared_tracker.get_stats().partition_misses;
    BOOST_REQUIRE(!read_partition(index, "pk0", *schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_misses, misses_before + 1);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_cache_read_partition_upgrades_the_entry) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    auto original = make_mutation(schema, "pk0", "v0");
    populate(index, key, original);

    auto new_schema = schema_builder(schema).with_column("v2", utf8_type).build();
    auto partition = read_partition(index, "pk0", *new_schema);
    BOOST_REQUIRE(partition);
    auto expected = original;
    expected.upgrade(new_schema);
    assert_that(to_mutation(new_schema, "pk0", std::move(*partition))).is_equal_to(expected);
    // The entry was upgraded in place, so a read under the new schema finds it there already.
    assert_that(lookup_mutation(index, key, new_schema)).is_equal_to(expected);
    index.drain_cache().get();
}

// peek() is the dump's read of the cache: the snapshot is what the cache holds, upgraded to the
// schema asked for, and it counts as neither a hit nor a miss.
SEASTAR_THREAD_TEST_CASE(test_logstor_cache_peek_snapshots_without_counting) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);
    auto m = make_mutation(schema, "pk0", "v0");
    populate(index, key, m);

    const auto stats_before = cache.shared_tracker.get_stats();
    auto snapshot = index.lookup_for_dump(key, schema);
    BOOST_REQUIRE(snapshot && snapshot->cached_mutation);
    assert_that(*snapshot->cached_mutation).is_equal_to(m);

    auto new_schema = schema_builder(schema).with_column("v2", utf8_type).build();
    auto upgraded = index.lookup_for_dump(key, new_schema);
    BOOST_REQUIRE(upgraded && upgraded->cached_mutation);
    auto expected = m;
    expected.upgrade(new_schema);
    assert_that(*upgraded->cached_mutation).is_equal_to(expected);

    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_hits, stats_before.partition_hits);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_misses, stats_before.partition_misses);
    index.drain_cache().get();
}

// Returns the cached partition of a key, and reads it the way a scan reads it, inside the visit of
// the entry and through the batch of its token.
static std::optional<keyed_single_row_partition> scan_read_cached(primary_index& index, const primary_index_key& key,
        const schema& s) {
    std::optional<keyed_single_row_partition> cached;
    bool found = false;
    auto scan = index.scan(dht::token_range::make_singular(key.token()));
    while (!found && scan.next_batch(16, [&] (const primary_index::scanned_entry& entry) {
        if ((entry.key() <=> key) == 0) {
            found = true;
            cached = entry.read_cached(s);
        }
    })) {
    }
    BOOST_REQUIRE(found);
    return cached;
}

// The scan's read of the cache: the key comes too, since the scan holds only the entry, and it
// counts as a hit or a miss like a point read does.
SEASTAR_THREAD_TEST_CASE(test_logstor_cache_read_cached_hands_out_the_key_too) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    const auto stats_before = cache.shared_tracker.get_stats();
    BOOST_REQUIRE(!scan_read_cached(index, key, *schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_misses, stats_before.partition_misses + 1);

    auto m = make_mutation(schema, "pk0", "v0");
    populate(index, key, m);
    auto cached = scan_read_cached(index, key, *schema);
    BOOST_REQUIRE(cached);
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_hits, stats_before.partition_hits + 1);
    BOOST_REQUIRE(cached->key.equal(*schema, m.decorated_key()));
    assert_that(to_mutation(schema, std::move(cached->key), std::move(cached->partition))).is_equal_to(m);

    index.drain_cache().get();
}

// With the cache disabled, a populate caches nothing and a lookup neither hits nor counts a miss.
SEASTAR_THREAD_TEST_CASE(test_logstor_cache_disabled_index_does_not_read_or_populate) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, false);

    auto key = make_primary_index_key(*schema, "pk0");
    BOOST_REQUIRE(!index.insert(key, make_index_entry(1, 0, 10, 1)).previous_entry);

    auto m = make_mutation(schema, "pk0", "v0");
    auto where = lookup_guarded_location(index, key, *schema);
    BOOST_REQUIRE(where);
    BOOST_REQUIRE(!index.populate_cache(where->guard, key, where->location, schema, m.key(), to_single_row_partition(mutation(m))));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.partitions(), 0);
    BOOST_REQUIRE(!lookup_exists(index, key, schema));
    BOOST_REQUIRE(!scan_read_cached(index, key, *schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.get_stats().partition_misses, 0);
}

// Partitions cached before the cache is disabled are not read, but an overwrite, an erase and the
// drain still evict them, so a read after the cache is enabled again does not get old data.
SEASTAR_THREAD_TEST_CASE(test_logstor_cache_disabled_index_still_evicts) {
    auto schema = make_logstor_schema();
    shared_logstor_cache cache;
    primary_index index(noop_space_accounting, &cache.logstor_tracker, true);

    auto key0 = make_primary_index_key(*schema, "pk0");
    auto key1 = make_primary_index_key(*schema, "pk1");
    auto key2 = make_primary_index_key(*schema, "pk2");
    BOOST_REQUIRE(!index.insert(key0, make_index_entry(1, 0, 10, 1)).previous_entry);
    BOOST_REQUIRE(!index.insert(key1, make_index_entry(1, 10, 10, 1)).previous_entry);
    BOOST_REQUIRE(!index.insert(key2, make_index_entry(1, 20, 10, 1)).previous_entry);
    auto m0 = make_mutation(schema, "pk0", "v0");
    auto m1 = make_mutation(schema, "pk1", "v1");
    auto m2 = make_mutation(schema, "pk2", "v2");
    populate(index, key0, m0);
    populate(index, key1, m1);
    populate(index, key2, m2);

    index.set_cache_enabled(false);
    BOOST_REQUIRE(!lookup_exists(index, key0, schema));
    BOOST_REQUIRE(!scan_read_cached(index, key0, *schema));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.partitions(), 3);

    // An overwrite or an erase while the cache is disabled evicts, so the old partition is not
    // read when the cache is enabled again.
    BOOST_REQUIRE(index.insert(key0, make_index_entry(2, 0, 10, 2)).inserted());
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.partitions(), 2);
    BOOST_REQUIRE(index.erase(key1, make_index_entry(1, 10, 10, 1).location));
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.partitions(), 1);

    index.set_cache_enabled(true);
    BOOST_REQUIRE(!lookup_exists(index, key0, schema));
    assert_that(schema, lookup(index, key2, schema)).is_equal_to(m2.partition());

    // The drain on stop does not depend on the flag.
    index.set_cache_enabled(false);
    index.drain_cache().get();
    BOOST_REQUIRE_EQUAL(cache.shared_tracker.partitions(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
