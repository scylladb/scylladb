/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/when_all.hh>

#include "replica/logstor/index.hh"
#include "replica/logstor/types.hh"
#include "test/lib/tmpdir.hh"
#include "replica/logstor/logstor.hh"
#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"
#include "test/lib/mutation_assertions.hh"
#include "serializer_impl.hh"
#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"

using namespace replica::logstor;

namespace replica::logstor {
inline std::ostream& operator<<(std::ostream& os, const log_segment_id& id) {
    return os << id.value;
}
inline std::ostream& operator<<(std::ostream& os, const log_location& loc) {
    return os << "log_location(segment=" << loc.segment << ", offset=" << loc.offset << ", size=" << loc.size << ")";
}
inline std::ostream& operator<<(std::ostream& os, const index_entry& entry) {
    return os << "index_entry(" << entry.location << ")";
}
}

BOOST_AUTO_TEST_SUITE(logstor_test)

static schema_ptr make_kv_schema(const char* table_name = "test_cf") {
    return schema_builder("test_ks", table_name)
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("v", utf8_type)
        .build();
}

static partition_key make_key(const schema& s, sstring key_value) {
    return partition_key::from_single_value(s, to_bytes(key_value));
}

static mutation make_mutation(schema_ptr s, sstring key_value, sstring value, api::timestamp_type ts) {
    auto key = make_key(*s, key_value);
    mutation m(s, key);
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes(value)), ts);
    return m;
}

static future<> do_logstor_test(logstor_config cfg, noncopyable_function<future<> (logstor&)> f) {
    auto ls = logstor(cfg);
    co_await ls.start();
    co_await f(ls).finally([&] {
        return ls.stop();
    });
}

static future<> do_logstor_test(noncopyable_function<future<> (logstor&)> f) {
    tmpdir tmp;
    logstor_config cfg = {
        .segment_manager_cfg = {
            .base_dir = tmp.path(),
            .disk_size = 512 * 1024 * 1024,
        }
    };
    co_await do_logstor_test(cfg, std::move(f));
}

// write a mutation and read it back using the key, verify we get the same mutation.
SEASTAR_TEST_CASE(test_single_write_and_read) {
    return do_logstor_test([](logstor& ls) -> future<> {
        auto s = make_kv_schema();

        auto m = make_mutation(s, "test_key", "test_value", 1);
        auto dk = dht::decorate_key(*s, m.key());

        // Write the mutation
        co_await ls.write(m, group_id{});

        // Read it back using the decorated key
        auto read_result = co_await ls.read(*s, dk);

        // Verify we got the mutation back
        BOOST_REQUIRE(read_result.has_value());
        auto read_mutation = read_result->to_mutation(s);

        assert_that(read_mutation).is_equal_to(m);
    });
}

// write and read multiple mutations with different keys.
SEASTAR_TEST_CASE(test_write_and_read_multiple_keys) {
    return do_logstor_test([](logstor& ls) -> future<> {
        auto s = make_kv_schema();

        // Create first mutation
        auto m1 = make_mutation(s, "test_key1", "test_value1", 1);
        auto dk1 = dht::decorate_key(*s, m1.key());

        // Create second mutation with different key
        auto m2 = make_mutation(s, "test_key2", "test_value2", 2);
        auto dk2 = dht::decorate_key(*s, m2.key());

        // Write both mutations
        co_await ls.write(m1, group_id{});
        co_await ls.write(m2, group_id{});

        // Read back first mutation
        auto read_result1 = co_await ls.read(*s, dk1);
        BOOST_REQUIRE(read_result1.has_value());
        auto read_mutation1 = read_result1->to_mutation(s);
        assert_that(read_mutation1).is_equal_to(m1);

        // Read back second mutation
        auto read_result2 = co_await ls.read(*s, dk2);
        BOOST_REQUIRE(read_result2.has_value());
        auto read_mutation2 = read_result2->to_mutation(s);
        assert_that(read_mutation2).is_equal_to(m2);
    });
}

// write a mutation, then write another mutation with the same key but different value and higher timestamp.
// read and verify we get the second mutation back.
SEASTAR_TEST_CASE(test_overwrite_same_key) {
    return do_logstor_test([](logstor& ls) -> future<> {
        auto s = make_kv_schema();

        // Create first mutation with initial value
        auto m1 = make_mutation(s, "test_key", "initial_value", 1);
        auto dk = dht::decorate_key(*s, m1.key());

        // Write first mutation
        co_await ls.write(m1, group_id{});

        // Create second mutation with same key but different value and higher timestamp
        auto m2 = make_mutation(s, "test_key", "updated_value", 2);

        // Write second mutation (should overwrite the first)
        co_await ls.write(m2, group_id{});

        // Read back and verify we get the updated value
        auto read_result = co_await ls.read(*s, dk);
        BOOST_REQUIRE(read_result.has_value());
        auto read_mutation = read_result->to_mutation(s);

        // Should get the second mutation
        assert_that(read_mutation).is_equal_to(m2);
    });
}

// create two identical kv schemas and write a mutation to each with the same key.
// read back and verify we get the correct mutation for each table.
SEASTAR_TEST_CASE(test_same_key_in_different_tables) {
    return do_logstor_test([](logstor& ls) -> future<> {
        // Create two tables with identical schemas
        auto s1 = make_kv_schema("test_cf1");
        auto s2 = make_kv_schema("test_cf2");

        // Create mutation for first table
        auto m1 = make_mutation(s1, "shared_key", "value_in_table1", 1);
        auto dk1 = dht::decorate_key(*s1, m1.key());

        // Create mutation for second table with same key, different value
        auto m2 = make_mutation(s2, "shared_key", "value_in_table2", 1);
        auto dk2 = dht::decorate_key(*s2, m2.key());

        // Write to both tables
        co_await ls.write(m1, group_id{});
        co_await ls.write(m2, group_id{});

        // Read from first table and verify
        auto read_result1 = co_await ls.read(*s1, dk1);
        BOOST_REQUIRE(read_result1.has_value());
        auto read_mutation1 = read_result1->to_mutation(s1);
        assert_that(read_mutation1).is_equal_to(m1);

        // Read from second table and verify
        auto read_result2 = co_await ls.read(*s2, dk2);
        BOOST_REQUIRE(read_result2.has_value());
        auto read_mutation2 = read_result2->to_mutation(s2);
        assert_that(read_mutation2).is_equal_to(m2);
    });
}

// basic tests of the index functions.
SEASTAR_TEST_CASE(test_index) {
    logstor::init_crypto();
    auto free_crypto = defer([] { logstor::free_crypto(); });

    auto indexp = std::make_unique<log_index>();
    auto& index = *indexp;

    auto s = make_kv_schema();

    auto pk1 = dht::decorate_key(*s, make_key(*s, "key1"));
    index_key key1 = logstor::calculate_key(*s, pk1);
    index_entry entry1 = {
        .location = log_location{
            .segment = log_segment_id(0),
            .offset = 0,
            .size = 100
        }
    };
    index_entry entry2 = {
        .location = log_location{
            .segment = log_segment_id(1),
            .offset = 0,
            .size = 200
        }
    };

    {
        auto old = index.exchange(key1, entry1);
        BOOST_REQUIRE_EQUAL(old.has_value(), false);

        auto result = index.get(key1);
        BOOST_REQUIRE_EQUAL(result.has_value(), true);
        BOOST_REQUIRE_EQUAL(*result, entry1);
    }

    {
        auto old = index.exchange(key1, entry2);
        BOOST_REQUIRE_EQUAL(old.has_value(), true);
        BOOST_REQUIRE_EQUAL(*old, entry1);

        auto result = index.get(key1);
        BOOST_REQUIRE_EQUAL(result.has_value(), true);
        BOOST_REQUIRE_EQUAL(*result, entry2);
    }

    {
        auto new_location = log_location{
            .segment = log_segment_id(2),
            .offset = 0,
            .size = 200
        };
        bool updated1 = index.update_record_location(key1, entry2.location, new_location);
        BOOST_REQUIRE_EQUAL(updated1, true);

        auto result = index.get(key1);
        BOOST_REQUIRE_EQUAL(result.has_value(), true);
        BOOST_REQUIRE_EQUAL(result->location, new_location);

        bool updated2 = index.update_record_location(key1, entry2.location, entry1.location);
        BOOST_REQUIRE_EQUAL(updated2, false);
    }

    co_return;
}

SEASTAR_TEST_CASE(test_index_iterator) {
    logstor::init_crypto();
    auto free_crypto = defer([] { logstor::free_crypto(); });

    auto indexp = std::make_unique<log_index>();
    auto& index = *indexp;

    auto s = make_kv_schema();

    // Insert multiple entries into the index
    std::vector<std::pair<index_key, index_entry>> expected_entries;

    for (size_t i = 0; i < 10; i++) {
        auto pk = dht::decorate_key(*s, make_key(*s, format("key_{}", i)));
        index_key key = logstor::calculate_key(*s, pk);
        index_entry entry = {
            .location = log_location{
                .segment = log_segment_id(i / 3),  // Spread across different segments
                .offset = static_cast<uint32_t>(i * 100),
                .size = static_cast<uint32_t>(100 + i)
            }
        };

        (void)index.exchange(key, entry);
        expected_entries.push_back({key, entry});
    }

    // Iterate over all entries and verify we get them all
    size_t count = 0;
    for (const auto& entry : index) {
        count++;

        // Verify this entry exists in our expected set
        bool found = false;
        for (const auto& [expected_key, expected_entry] : expected_entries) {
            if (entry.location == expected_entry.location) {
                found = true;
                break;
            }
        }
        BOOST_REQUIRE(found);
    }

    // Verify we iterated over all entries
    BOOST_REQUIRE_EQUAL(count, expected_entries.size());

    // Test that empty index iteration works
    auto empty_index = std::make_unique<log_index>();
    count = 0;
    for (const auto& entry : *empty_index) {
        (void)entry;
        count++;
    }
    BOOST_REQUIRE_EQUAL(count, 0);

    co_return;
}

static future<> do_segment_manager_test(segment_manager_config cfg, noncopyable_function<future<> (segment_manager&, log_index&)> f) {
    auto indexp = std::make_unique<log_index>();
    auto& index = *indexp;
    auto sm = segment_manager(cfg, index);
    co_await sm.start();
    co_await f(sm, index).finally([&] {
        return sm.stop();
    });
}

// write multiple mutations to multiple segments, then read all records in those segments back using
// for_each_record and verify we get exactly the mutations we wrote.
SEASTAR_TEST_CASE(test_for_each_record) {
    logstor::init_crypto();
    auto free_crypto = defer([] { logstor::free_crypto(); });

    tmpdir tmp;
    segment_manager_config cfg = {
        .base_dir = tmp.path(),
        .segment_size = 8 * 1024,  // 8KB segments
        .file_size = 8 * 8 * 1024,    // 8 segments per file
        .disk_size = 512 * 1024 * 1024 // 512MB
    };

    co_await do_segment_manager_test(cfg, [](segment_manager& sm, log_index&) -> future<> {
        auto s = make_kv_schema();

        // Write several records and track their locations
        struct expected_record {
            mutation mut;
            log_location loc;
        };
        std::vector<expected_record> expected_records;
        std::vector<log_segment_id> segments_used;

        // write 5 buffers, each with multiple records
        for (int i = 0; i < 5; i++) {

            struct write_future {
                future<log_location> fut;
                mutation mut;
            };

            write_buffer wb(sm.get_segment_size());
            std::vector<write_future> write_futures;

            // different number of records in each buffer
            for (int j = 0; j < (i+1) * 2; j++) {
                auto m = make_mutation(s, format("key_{}_{}", i, j),
                                      format("value_{}_{}", i, j) + sstring(j + 1, 'X'),
                                      i*5+j);

                log_record_writer writer(log_record {
                    .key = logstor::calculate_key(*s, dht::decorate_key(*s, m.key())),
                    .mut = canonical_mutation(m)
                });
                write_futures.push_back({
                    .fut = wb.write(std::move(writer)),
                    .mut = m
                });
            }

            co_await sm.write(wb);

            for (auto&& w : write_futures) {
                auto loc = co_await std::move(w.fut);
                expected_records.push_back(expected_record{.mut = w.mut, .loc = loc});

                // Track which segments we used
                if (segments_used.empty() || segments_used.back() != loc.segment) {
                    segments_used.push_back(loc.segment);
                }
            }
        }

        // Now read back all records using for_each_record
        size_t record_index = 0;
        co_await sm.for_each_record(segments_used,
            [&](log_location loc, log_record record) -> future<> {
                BOOST_REQUIRE(record_index < expected_records.size());

                auto& expected = expected_records[record_index];

                // Verify location matches
                BOOST_REQUIRE_EQUAL(loc, expected.loc);

                // Verify mutation matches
                auto read_mutation = record.mut.to_mutation(s);
                assert_that(read_mutation).is_equal_to(expected.mut);

                record_index++;
                co_return;
            });

        // Verify we read exactly the expected number of records
        BOOST_REQUIRE_EQUAL(record_index, expected_records.size());
    });
}

// write many mutations with the same key to fill the disk multiple times, each write overwrites the previous one.
// compaction should be triggered to free the old locations and allow us to continue writing without running out of space.
SEASTAR_TEST_CASE(test_overwrites_with_compaction) {
    logstor::init_crypto();
    auto free_crypto = defer([] { logstor::free_crypto(); });

    tmpdir tmp;
    segment_manager_config sm_cfg = {
        .base_dir = tmp.path(),
        .segment_size = 8 * 1024,
        .file_size = 32 * 4 * 1024,
        .disk_size = 32 * 4 * 1024,
        .compaction_enabled = true,
    };

    co_await do_segment_manager_test(sm_cfg, [&sm_cfg] (segment_manager& sm, log_index& index) -> future<> {
        auto s = make_kv_schema();

        // Use a single key that we'll overwrite many times
        auto pk = make_key(*s, "compaction_key");
        auto key = logstor::calculate_key(*s, dht::decorate_key(*s, pk));

        sm.enable_auto_compaction();

        // Write enough overwrites to fill the disk multiple times
        // Each write is block-aligned
        const size_t total_disk_bytes = sm_cfg.disk_size;
        const size_t writes_to_fill_disk = total_disk_bytes / segment_manager::block_alignment;
        const size_t total_writes = writes_to_fill_disk * 3;

        for (size_t i = 0; i < total_writes; i++) {
            auto m = make_mutation(s, "compaction_key", format("value_{}", i), i + 1);

            write_buffer wb(sm.get_segment_size());
            log_record_writer writer(log_record {
                .key = key,
                .mut = canonical_mutation(m)
            });

            auto loc_fut = wb.write(std::move(writer));
            co_await sm.write(wb);
            auto loc = co_await std::move(loc_fut);

            // Free the previous location after overwriting
            auto prev_entry = index.exchange(key, index_entry{.location = loc});
            if (prev_entry) {
                sm.free_record(prev_entry->location);
            }
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
