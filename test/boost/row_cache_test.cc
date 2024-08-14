/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <seastar/core/sleep.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <seastar/util/closeable.hh>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/key_utils.hh"

#include "schema/schema_builder.hh"
#include "test/lib/simple_schema.hh"
#include "row_cache.hh"
#include <seastar/core/thread.hh>
#include "replica/memtable.hh"
#include "partition_slice_builder.hh"
#include "mutation/mutation_rebuilder.hh"
#include "service/migration_manager.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include "test/lib/log.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_utils.hh"
#include "utils/assert.hh"
#include "utils/throttle.hh"

#include <fmt/ranges.h>
#include <boost/range/algorithm/min_element.hpp>
#include "readers/from_mutations_v2.hh"
#include "readers/delegating_v2.hh"
#include "readers/empty_v2.hh"
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

static schema_ptr make_schema_with_extra_column() {
    return schema_builder(make_schema())
        .with_column("a", bytes_type, column_kind::regular_column)
        .build();
}

static thread_local api::timestamp_type next_timestamp = 1;

static
mutation make_new_mutation(schema_ptr s, partition_key key) {
    mutation m(s, key);
    static thread_local int next_value = 1;
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes(format("v{:d}", next_value++))), next_timestamp++);
    return m;
}

static
partition_key new_key(schema_ptr s) {
    static thread_local int next = 0;
    return partition_key::from_single_value(*s, to_bytes(format("key{:d}", next++)));
}

static
mutation make_new_mutation(schema_ptr s) {
    return make_new_mutation(s, new_key(s));
}

snapshot_source make_decorated_snapshot_source(snapshot_source src, std::function<mutation_source(mutation_source)> decorator) {
    return snapshot_source([src = std::move(src), decorator = std::move(decorator)] () mutable {
        return decorator(src());
    });
}

mutation_source make_source_with(mutation m) {
    return mutation_source([m] (schema_ptr s, reader_permit permit, const dht::partition_range&, const query::partition_slice&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
        SCYLLA_ASSERT(m.schema() == s);
        return make_mutation_reader_from_mutations_v2(s, std::move(permit), m, std::move(fwd));
    });
}

// It is assumed that src won't change.
snapshot_source snapshot_source_from_snapshot(mutation_source src) {
    return snapshot_source([src = std::move(src)] {
        return src;
    });
}

bool has_key(row_cache& cache, const dht::decorated_key& key) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto range = dht::partition_range::make_singular(key);
    auto reader = cache.make_reader(cache.schema(), semaphore.make_permit(), range);
    auto close_reader = deferred_close(reader);
    auto mo = read_mutation_from_mutation_reader(reader).get();
    if (!bool(mo)) {
        return false;
    }
    return !mo->partition().empty();
}

void verify_has(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(has_key(cache, key));
}

void verify_does_not_have(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(!has_key(cache, key));
}

void verify_has(row_cache& cache, const mutation& m) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto range = dht::partition_range::make_singular(m.decorated_key());
    auto reader = cache.make_reader(cache.schema(), semaphore.make_permit(), range);
    assert_that(std::move(reader)).next_mutation().is_equal_to(m);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        tests::reader_concurrency_semaphore_wrapper semaphore;

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(make_source_with(m)), tracker);

        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_works_after_clearing) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(make_source_with(m)), tracker);

        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();

        tracker.clear();

        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

class partition_counting_reader final : public delegating_reader_v2 {
    int& _counter;
    bool _count_fill_buffer = true;
public:
    partition_counting_reader(mutation_reader mr, int& counter)
        : delegating_reader_v2(std::move(mr)), _counter(counter) { }
    virtual future<> fill_buffer() override {
        if (_count_fill_buffer) {
            ++_counter;
            _count_fill_buffer = false;
        }
        return delegating_reader_v2::fill_buffer();
    }
    virtual future<> next_partition() override {
        _count_fill_buffer = false;
        ++_counter;
        return delegating_reader_v2::next_partition();
    }
};

mutation_reader make_counting_reader(mutation_reader mr, int& counter) {
    return make_mutation_reader<partition_counting_reader>(std::move(mr), counter);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_empty_full_range) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        int secondary_calls_count = 0;
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mutation_source([&secondary_calls_count] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice&,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd) {
            return make_counting_reader(make_empty_flat_reader_v2(s, std::move(permit)), secondary_calls_count);
        })), tracker);

        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

dht::partition_range make_single_partition_range(schema_ptr& s, int pkey) {
    auto pk = partition_key::from_exploded(*s, { int32_type->decompose(pkey) });
    auto dk = dht::decorate_key(*s, pk);
    return dht::partition_range::make_singular(dk);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_empty_single_partition_query) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        int secondary_calls_count = 0;
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mutation_source([&secondary_calls_count] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice&,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd) {
            return make_counting_reader(make_empty_flat_reader_v2(s, std::move(permit)), secondary_calls_count);
        })), tracker);
        auto range = make_single_partition_range(s, 100);
        assert_that(cache.make_reader(s, semaphore.make_permit(), range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
        assert_that(cache.make_reader(s, semaphore.make_permit(), range))
            .produces_eos_or_empty_mutation();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

SEASTAR_TEST_CASE(test_cache_uses_continuity_info_for_single_partition_query) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        int secondary_calls_count = 0;
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mutation_source([&secondary_calls_count] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice&,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd) {
            return make_counting_reader(make_empty_flat_reader_v2(s, std::move(permit)), secondary_calls_count);
        })), tracker);

        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
                .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);

        auto range = make_single_partition_range(s, 100);

        assert_that(cache.make_reader(s, semaphore.make_permit(), range))
                .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

void test_cache_delegates_to_underlying_only_once_with_single_partition(schema_ptr s,
                                                                        tests::reader_concurrency_semaphore_wrapper& semaphore,
                                                                        const mutation& m,
                                                                        const dht::partition_range& range,
                                                                        int calls_to_secondary) {
    int secondary_calls_count = 0;
    cache_tracker tracker;
    row_cache cache(s, snapshot_source_from_snapshot(mutation_source([m, &secondary_calls_count] (
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding fwd) {
        SCYLLA_ASSERT(m.schema() == s);
        if (range.contains(dht::ring_position(m.decorated_key()), dht::ring_position_comparator(*s))) {
            return make_counting_reader(make_mutation_reader_from_mutations_v2(s, std::move(permit), m, std::move(fwd)), secondary_calls_count);
        } else {
            return make_counting_reader(make_empty_flat_reader_v2(s, std::move(permit)), secondary_calls_count);
        }
    })), tracker);

    assert_that(cache.make_reader(s, semaphore.make_permit(), range))
        .produces(m)
        .produces_end_of_stream();
    BOOST_REQUIRE_EQUAL(secondary_calls_count, calls_to_secondary);
    assert_that(cache.make_reader(s, semaphore.make_permit(), range))
        .produces(m)
        .produces_end_of_stream();
    BOOST_REQUIRE_EQUAL(secondary_calls_count, calls_to_secondary);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_single_key_range) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto m = make_new_mutation(s);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, semaphore, m,
            dht::partition_range::make_singular(query::ring_position(m.decorated_key())), 1);
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_full_range) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto m = make_new_mutation(s);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, semaphore, m, query::full_partition_range, 2);
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_range_open) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto m = make_new_mutation(s);
        dht::partition_range::bound end = {dht::ring_position(m.decorated_key()), true};
        dht::partition_range range = dht::partition_range::make_ending_with(end);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, semaphore, m, range, 2);
    });
}

// partitions must be sorted by decorated key
static void require_no_token_duplicates(const std::vector<mutation>& partitions) {
    std::optional<dht::token> last_token;
    for (auto&& p : partitions) {
        const dht::decorated_key& key = p.decorated_key();
        if (last_token && key.token() == *last_token) {
            BOOST_FAIL("token duplicate detected");
        }
        last_token = key.token();
    }
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_multiple_mutations) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("key", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type)
            .build();

        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto make_partition_mutation = [s] (bytes key) -> mutation {
            mutation m(s, partition_key::from_single_value(*s, key));
            m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
            return m;
        };

        int partition_count = 5;

        std::vector<mutation> partitions;
        for (int i = 0; i < partition_count; ++i) {
            partitions.emplace_back(
                make_partition_mutation(to_bytes(format("key_{:d}", i))));
        }

        std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());
        require_no_token_duplicates(partitions);

        dht::decorated_key key_before_all = partitions.front().decorated_key();
        partitions.erase(partitions.begin());

        dht::decorated_key key_after_all = partitions.back().decorated_key();
        partitions.pop_back();

        cache_tracker tracker;
        auto mt = make_memtable(s, partitions);

        auto make_cache = [&tracker, &mt](schema_ptr s, int& secondary_calls_count) -> lw_shared_ptr<row_cache> {
            auto secondary = mutation_source([&mt, &secondary_calls_count] (schema_ptr s, reader_permit permit, const dht::partition_range& range,
                    const query::partition_slice& slice, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
                return make_counting_reader(mt->make_flat_reader(s, std::move(permit), range, slice, std::move(trace), std::move(fwd)), secondary_calls_count);
            });

            return make_lw_shared<row_cache>(s, snapshot_source_from_snapshot(secondary), tracker);
        };

        auto make_ds = [&make_cache](schema_ptr s, int& secondary_calls_count) -> mutation_source {
            auto cache = make_cache(s, secondary_calls_count);
            return mutation_source([cache] (schema_ptr s, reader_permit permit, const dht::partition_range& range,
                    const query::partition_slice& slice, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
                return cache->make_reader(s, std::move(permit), range, slice, std::move(trace), std::move(fwd));
            });
        };

        auto do_test = [&s, &semaphore, &partitions] (const mutation_source& ds, const dht::partition_range& range,
                                          int& secondary_calls_count, int expected_calls) {
            assert_that(ds.make_reader_v2(s, semaphore.make_permit(), range))
                .produces(slice(partitions, range))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(expected_calls, secondary_calls_count);
        };

        {
            int secondary_calls_count = 0;
            auto test = [&] (const mutation_source& ds, const dht::partition_range& range, int expected_count) {
                do_test(ds, range, secondary_calls_count, expected_count);
            };

            auto ds = make_ds(s, secondary_calls_count);
            auto expected = partitions.size() + 1;
            test(ds, query::full_partition_range, expected);
            test(ds, query::full_partition_range, expected);
            test(ds, dht::partition_range::make_ending_with({partitions[0].decorated_key(), false}), expected);
            test(ds, dht::partition_range::make_ending_with({partitions[0].decorated_key(), true}), expected);
            test(ds, dht::partition_range::make_starting_with({partitions.back().decorated_key(), false}), expected);
            test(ds, dht::partition_range::make_starting_with({partitions.back().decorated_key(), true}), expected);
            test(ds, dht::partition_range::make_ending_with({partitions[1].decorated_key(), false}), expected);
            test(ds, dht::partition_range::make_ending_with({partitions[1].decorated_key(), true}), expected);
            test(ds, dht::partition_range::make_starting_with({partitions[1].decorated_key(), false}), expected);
            test(ds, dht::partition_range::make_starting_with({partitions[1].decorated_key(), true}), expected);
            test(ds, dht::partition_range::make_ending_with({partitions.back().decorated_key(), false}), expected);
            test(ds, dht::partition_range::make_ending_with({partitions.back().decorated_key(), true}), expected);
            test(ds, dht::partition_range::make_starting_with({partitions[0].decorated_key(), false}), expected);
            test(ds, dht::partition_range::make_starting_with({partitions[0].decorated_key(), true}), expected);
            test(ds, dht::partition_range::make(
                {dht::ring_position::starting_at(key_before_all.token())},
                {dht::ring_position::ending_at(key_after_all.token())}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[1].decorated_key(), true}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), false},
                {partitions[1].decorated_key(), true}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[1].decorated_key(), false}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), false},
                {partitions[1].decorated_key(), false}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[1].decorated_key(), true},
                {partitions[2].decorated_key(), true}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[1].decorated_key(), false},
                {partitions[2].decorated_key(), true}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[1].decorated_key(), true},
                {partitions[2].decorated_key(), false}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[1].decorated_key(), false},
                {partitions[2].decorated_key(), false}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[2].decorated_key(), true}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), false},
                {partitions[2].decorated_key(), true}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[2].decorated_key(), false}),
                expected);
            test(ds, dht::partition_range::make(
                {partitions[0].decorated_key(), false},
                {partitions[2].decorated_key(), false}),
                expected);
        }
        {
            int secondary_calls_count = 0;
            auto ds = make_ds(s, secondary_calls_count);
            auto range = dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[1].decorated_key(), true});
            assert_that(ds.make_reader_v2(s, semaphore.make_permit(), range))
                .produces(slice(partitions, range))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(3, secondary_calls_count);
            assert_that(ds.make_reader_v2(s, semaphore.make_permit(), range))
                .produces(slice(partitions, range))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(3, secondary_calls_count);
            auto range2 = dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[1].decorated_key(), false});
            assert_that(ds.make_reader_v2(s, semaphore.make_permit(), range2))
                .produces(slice(partitions, range2))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(3, secondary_calls_count);
            auto range3 = dht::partition_range::make(
                {dht::ring_position::starting_at(key_before_all.token())},
                {partitions[2].decorated_key(), false});
            assert_that(ds.make_reader_v2(s, semaphore.make_permit(), range3))
                .produces(slice(partitions, range3))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(5, secondary_calls_count);
        }
        {
            int secondary_calls_count = 0;
            auto test = [&] (const mutation_source& ds, const dht::partition_range& range, int expected_count) {
                do_test(ds, range, secondary_calls_count, expected_count);
            };

            auto cache = make_cache(s, secondary_calls_count);
            auto ds = mutation_source([cache] (schema_ptr s, reader_permit permit, const dht::partition_range& range,
                    const query::partition_slice& slice, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
                    return cache->make_reader(s, std::move(permit), range, slice, std::move(trace), std::move(fwd));
            });

            test(ds, query::full_partition_range, partitions.size() + 1);
            test(ds, query::full_partition_range, partitions.size() + 1);

            cache->invalidate(row_cache::external_updater([] {}), key_after_all).get();

            assert_that(ds.make_reader_v2(s, semaphore.make_permit(), query::full_partition_range))
                .produces(slice(partitions, query::full_partition_range))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(partitions.size() + 2, secondary_calls_count);
        }
    });
}

static std::vector<mutation> make_ring(schema_ptr s, int n_mutations) {
    std::vector<mutation> mutations;
    for (int i = 0; i < n_mutations; ++i) {
        mutations.push_back(make_new_mutation(s));
    }
    std::sort(mutations.begin(), mutations.end(), mutation_decorated_key_less_comparator());
    return mutations;
}

SEASTAR_TEST_CASE(test_query_of_incomplete_range_goes_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        std::vector<mutation> mutations = make_ring(s, 3);

        auto mt = make_memtable(s, mutations);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return dht::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        auto key0_range = get_partition_range(mutations[0]);
        auto key2_range = get_partition_range(mutations[2]);

        // Populate cache for first key
        assert_that(cache.make_reader(s, semaphore.make_permit(), key0_range))
            .produces(mutations[0])
            .produces_end_of_stream();

        // Populate cache for last key
        assert_that(cache.make_reader(s, semaphore.make_permit(), key2_range))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test single-key queries
        assert_that(cache.make_reader(s, semaphore.make_permit(), key0_range))
            .produces(mutations[0])
            .produces_end_of_stream();

        assert_that(cache.make_reader(s, semaphore.make_permit(), key2_range))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test range query
        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
            .produces(mutations[0])
            .produces(mutations[1])
            .produces(mutations[2])
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_single_key_queries_after_population_in_reverse_order) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        std::vector<mutation> mutations = make_ring(s, 3);
        auto mt = make_memtable(s, mutations);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return dht::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        auto key0_range = get_partition_range(mutations[0]);
        auto key1_range = get_partition_range(mutations[1]);
        auto key2_range = get_partition_range(mutations[2]);

        for (int i = 0; i < 2; ++i) {
            assert_that(cache.make_reader(s, semaphore.make_permit(), key2_range))
                .produces(mutations[2])
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, semaphore.make_permit(), key1_range))
                .produces(mutations[1])
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, semaphore.make_permit(), key0_range))
                .produces(mutations[0])
                .produces_end_of_stream();
        }
    });
}

// Reproducer for https://github.com/scylladb/scylla/issues/4236
SEASTAR_TEST_CASE(test_partition_range_population_with_concurrent_memtable_flushes) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        std::vector<mutation> mutations = make_ring(s, 3);
        auto mt = make_memtable(s, mutations);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        bool cancel_updater = false;
        auto updater = repeat([&] {
            if (cancel_updater) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            return yield().then([&] {
                auto mt = make_lw_shared<replica::memtable>(s);
                return cache.update(row_cache::external_updater([]{}), *mt).then([mt] {
                    return stop_iteration::no;
                });
            });
        });

        {
            auto pr = dht::partition_range::make_singular(query::ring_position(mutations[1].decorated_key()));
            assert_that(cache.make_reader(s, semaphore.make_permit(), pr))
                .produces(mutations[1])
                .produces_end_of_stream();
        }

        {
            auto pr = dht::partition_range::make_ending_with(
                {query::ring_position(mutations[2].decorated_key()), true});
            assert_that(cache.make_reader(s, semaphore.make_permit(), pr))
                .produces(mutations[0])
                .produces(mutations[1])
                .produces(mutations[2])
                .produces_end_of_stream();
        }

        cache.invalidate(row_cache::external_updater([]{})).get();

        {
            assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
                .produces(mutations[0])
                .produces(mutations[1])
                .produces(mutations[2])
                .produces_end_of_stream();
        }

        cancel_updater = true;
        updater.get();
    });
}

SEASTAR_TEST_CASE(test_row_cache_conforms_to_mutation_source) {
    return seastar::async([] {
        cache_tracker tracker;

        run_mutation_source_tests([&tracker](schema_ptr s, const std::vector<mutation>& mutations) -> mutation_source {
            auto mt = make_memtable(s, mutations);
            auto cache = make_lw_shared<row_cache>(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);
            return mutation_source([cache] (schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return cache->make_reader(s, std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
            });
        });
    });
}

static
mutation make_fully_continuous(const mutation& m) {
    mutation res = m;
    res.partition().make_fully_continuous();
    return res;
}

SEASTAR_TEST_CASE(test_reading_from_random_partial_partition) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        tests::reader_concurrency_semaphore_wrapper semaphore;

        // The test primes the cache with m1, which has random continuity,
        // and then applies m2 on top of it. This should result in some of m2's
        // write information to be dropped. The test then verifies that we still get the
        // proper m1 + m2.

        auto m1 = gen();
        auto m2 = make_fully_continuous(gen());

        memtable_snapshot_source underlying(gen.schema());
        underlying.apply(make_fully_continuous(m1));

        row_cache cache(gen.schema(), snapshot_source([&] { return underlying(); }), tracker);

        cache.populate(m1); // m1 is supposed to have random continuity and populate() should preserve it

        auto rd1 = cache.make_reader(gen.schema(), semaphore.make_permit());
        rd1.fill_buffer().get();

        // Merge m2 into cache
        auto mt = make_lw_shared<replica::memtable>(gen.schema());
        mt->apply(m2);
        cache.update(row_cache::external_updater([&] { underlying.apply(m2); }), *mt).get();

        auto rd2 = cache.make_reader(gen.schema(), semaphore.make_permit());
        rd2.fill_buffer().get();

        assert_that(std::move(rd1)).next_mutation().is_equal_to_compacted(m1);
        assert_that(std::move(rd2)).next_mutation().is_equal_to_compacted(m1 + m2);
    });
}

SEASTAR_TEST_CASE(test_presence_checker_runs_under_right_allocator) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);

        memtable_snapshot_source underlying(gen.schema());

        // Create a snapshot source whose presence checker allocates and stores a managed object.
        // The presence checker may assume that it runs and is destroyed in the context
        // of the standard allocator. If that isn't the case, there will be alloc-dealloc mismatch.
        auto src = snapshot_source([&] {
            auto ms = underlying();
            return mutation_source([ms = std::move(ms)] (schema_ptr s,
                reader_permit permit,
                const dht::partition_range& pr,
                const query::partition_slice& slice,
                tracing::trace_state_ptr tr,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding mr_fwd) {
                return ms.make_reader_v2(s, std::move(permit), pr, slice, std::move(tr), fwd, mr_fwd);
            }, [] {
                return [saved = managed_bytes()] (const dht::decorated_key& key) mutable {
                    // size large enough to defeat the small blob optimization
                    saved = managed_bytes(managed_bytes::initialized_later(), 1024);
                    return partition_presence_checker_result::maybe_exists;
                };
            });
        });

        row_cache cache(gen.schema(), std::move(src), tracker);

        auto m1 = make_fully_continuous(gen());

        auto mt = make_lw_shared<replica::memtable>(gen.schema());
        mt->apply(m1);
        cache.update(row_cache::external_updater([&] { underlying.apply(m1); }), *mt).get();
    });
}

SEASTAR_TEST_CASE(test_random_partition_population) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto m1 = make_fully_continuous(gen());
        auto m2 = make_fully_continuous(gen());

        memtable_snapshot_source underlying(gen.schema());
        underlying.apply(m1);

        row_cache cache(gen.schema(), snapshot_source([&] { return underlying(); }), tracker);

        assert_that(cache.make_reader(gen.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces_end_of_stream();

        cache.invalidate(row_cache::external_updater([&] {
            underlying.apply(m2);
        })).get();

        auto pr = dht::partition_range::make_singular(m2.decorated_key());
        assert_that(cache.make_reader(gen.schema(), semaphore.make_permit(), pr))
            .produces(m1 + m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_eviction) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 100000; i++) {
            auto m = make_new_mutation(s);
            keys.emplace_back(m.decorated_key());
            cache.populate(m);
        }

        auto& random = seastar::testing::local_random_engine;
        std::shuffle(keys.begin(), keys.end(), random);

        for (auto&& key : keys) {
            auto pr = dht::partition_range::make_singular(key);
            auto rd = cache.make_reader(s, semaphore.make_permit(), pr);
            auto close_rd = deferred_close(rd);
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
        }

        while (tracker.partitions() > 0) {
            logalloc::shard_tracker().reclaim(100);
        }

        BOOST_REQUIRE_EQUAL(tracker.get_stats().partition_evictions, keys.size());
    });
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR // Depends on eviction, which is absent with the std allocator

SEASTAR_TEST_CASE(test_eviction_from_invalidated) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        row_cache cache(gen.schema(), snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto prev_evictions = tracker.get_stats().partition_evictions;

        std::vector<dht::decorated_key> keys;
        while (tracker.get_stats().partition_evictions == prev_evictions) {
            auto dk = dht::decorate_key(*gen.schema(), new_key(gen.schema()));
            auto m = mutation(gen.schema(), dk, make_fully_continuous(gen()).partition());
            keys.emplace_back(dk);
            cache.populate(m);
        }

        auto& random = seastar::testing::local_random_engine;
        std::shuffle(keys.begin(), keys.end(), random);

        for (auto&& key : keys) {
            cache.make_reader(s, semaphore.make_permit(), dht::partition_range::make_singular(key)).close().get();
        }

        cache.invalidate(row_cache::external_updater([] {})).get();

        std::vector<sstring> tmp;
        auto alloc_size = logalloc::segment_size * 10;
        /*
         * Now allocate huge chunks on the region until it gives up
         * with bad_alloc. At that point the region must not have more
         * memory than the chunk size, neither it must contain rows
         * or partitions (except for dummy entries)
         */
        try {
            while (true) {
                tmp.push_back(uninitialized_string(alloc_size));
            }
        } catch (const std::bad_alloc&) {
            BOOST_REQUIRE(tracker.region().occupancy().total_space() < alloc_size);
            BOOST_REQUIRE(tracker.get_stats().partitions == 0);
            BOOST_REQUIRE(tracker.get_stats().rows == 0);
        }
    });
}

#endif

SEASTAR_TEST_CASE(test_eviction_after_schema_change) {
    return seastar::async([] {
        auto s = make_schema();
        auto s2 = make_schema_with_extra_column();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto m = make_new_mutation(s);
        cache.populate(m);

        cache.set_schema(s2);

        {
            auto pr = dht::partition_range::make_singular(m.decorated_key());
            auto rd = cache.make_reader(s2, semaphore.make_permit(), pr);
            auto close_rd = deferred_close(rd);
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
        }

        tracker.cleaner().drain().get();
        while (tracker.region().evict_some() == memory::reclaiming_result::reclaimed_something) ;

        // The partition should be evictable after schema change
        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 0);
        BOOST_REQUIRE_EQUAL(tracker.get_stats().partitions, 0);
        BOOST_REQUIRE_EQUAL(tracker.get_stats().partition_evictions, 1);
        verify_does_not_have(cache, m.decorated_key());
    });
}

void test_sliced_read_row_presence(mutation_reader reader, schema_ptr s, std::deque<int> expected)
{
    auto close_reader = deferred_close(reader);
    clustering_key::equality ck_eq(*s);

    auto mfopt = reader().get();
    BOOST_REQUIRE(mfopt->is_partition_start());
    while ((mfopt = reader().get()) && !mfopt->is_end_of_partition()) {
        if (mfopt->is_clustering_row()) {
            BOOST_REQUIRE(!expected.empty());
            auto expected_ck = expected.front();
            auto ck = clustering_key_prefix::from_single_value(*s, int32_type->decompose(expected_ck));
            expected.pop_front();
            auto& cr = mfopt->as_clustering_row();
            if (!ck_eq(cr.key(), ck)) {
                BOOST_FAIL(format("Expected {}, but got {}", ck, cr.key()));
            }
        }
    }
    BOOST_REQUIRE(expected.empty());
    BOOST_REQUIRE(mfopt && mfopt->is_end_of_partition());
    BOOST_REQUIRE(!reader().get());
}

SEASTAR_TEST_CASE(test_single_partition_update) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto pk = partition_key::from_exploded(*s, { int32_type->decompose(100) });
        auto dk = dht::decorate_key(*s, pk);
        auto range = dht::partition_range::make_singular(dk);
        auto make_ck = [&s] (int v) {
            return clustering_key_prefix::from_single_value(*s, int32_type->decompose(v));
        };
        auto ck1 = make_ck(1);
        auto ck2 = make_ck(2);
        auto ck3 = make_ck(3);
        auto ck4 = make_ck(4);
        auto ck7 = make_ck(7);
        memtable_snapshot_source cache_mt(s);
        {
            mutation m(s, pk);
            m.set_clustered_cell(ck1, "v", data_value(101), 1);
            m.set_clustered_cell(ck2, "v", data_value(101), 1);
            m.set_clustered_cell(ck4, "v", data_value(101), 1);
            m.set_clustered_cell(ck7, "v", data_value(101), 1);
            cache_mt.apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, snapshot_source([&] { return cache_mt(); }), tracker);

        {
            auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_ending_with(ck1))
                .with_range(query::clustering_range::make_starting_with(ck4))
                .build();
            auto reader = cache.make_reader(s, semaphore.make_permit(), range, slice);
            test_sliced_read_row_presence(std::move(reader), s, {1, 4, 7});
        }

        auto mt = make_lw_shared<replica::memtable>(s);
        cache.update(row_cache::external_updater([&] {
            mutation m(s, pk);
            m.set_clustered_cell(ck3, "v", data_value(101), 1);
            mt->apply(m);
            cache_mt.apply(m);
        }), *mt).get();

        {
            auto reader = cache.make_reader(s, semaphore.make_permit(), range);
            test_sliced_read_row_presence(std::move(reader), s, {1, 2, 3, 4, 7});
        }

    });
}

SEASTAR_TEST_CASE(test_update) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker, is_continuous::yes);

        testlog.info("Check cache miss with populate");

        int partition_count = 1000;

        // populate cache with some partitions
        std::vector<dht::decorated_key> keys_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_in_cache.push_back(m.decorated_key());
            cache.populate(m);
        }

        // populate memtable with partitions not in cache
        auto mt = make_lw_shared<replica::memtable>(s);
        std::vector<dht::decorated_key> keys_not_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_not_in_cache.push_back(m.decorated_key());
            mt->apply(m);
        }

        cache.update(row_cache::external_updater([] {}), *mt).get();

        for (auto&& key : keys_not_in_cache) {
            verify_has(cache, key);
        }

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }

        std::copy(keys_not_in_cache.begin(), keys_not_in_cache.end(), std::back_inserter(keys_in_cache));
        keys_not_in_cache.clear();

        testlog.info("Check cache miss with drop");

        auto mt2 = make_lw_shared<replica::memtable>(s);

        // populate memtable with partitions not in cache
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_not_in_cache.push_back(m.decorated_key());
            mt2->apply(m);
            cache.invalidate(row_cache::external_updater([] {}), m.decorated_key()).get();
        }

        cache.update(row_cache::external_updater([] {}), *mt2).get();

        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }

        testlog.info("Check cache hit with merge");

        auto mt3 = make_lw_shared<replica::memtable>(s);

        std::vector<mutation> new_mutations;
        for (auto&& key : keys_in_cache) {
            auto m = make_new_mutation(s, key.key());
            new_mutations.push_back(m);
            mt3->apply(m);
        }

        cache.update(row_cache::external_updater([] {}), *mt3).get();

        for (auto&& m : new_mutations) {
            verify_has(cache, m);
        }
    });
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR

static inline
mutation make_new_large_mutation(schema_ptr s, partition_key key) {
    mutation m(s, key);
    static thread_local int next_value = 1;
    static constexpr size_t blob_size = 64 * 1024;
    std::vector<int> data;
    data.reserve(blob_size);
    for (unsigned i = 0; i < blob_size; i++) {
        data.push_back(next_value);
    }
    next_value++;
    bytes b(reinterpret_cast<int8_t*>(data.data()), data.size() * sizeof(int));
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(std::move(b)), next_timestamp++);
    return m;
}

static inline
mutation make_new_large_mutation(schema_ptr s, int key) {
    return make_new_large_mutation(s, partition_key::from_single_value(*s, to_bytes(format("key{:d}", key))));
}

static inline
mutation make_new_mutation(schema_ptr s, int key) {
    return make_new_mutation(s, partition_key::from_single_value(*s, to_bytes(format("key{:d}", key))));
}

SEASTAR_TEST_CASE(test_update_failure) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker, is_continuous::yes);

        int partition_count = 1000;

        // populate cache with some partitions
        using partitions_type = std::map<partition_key, mutation_partition, partition_key::less_compare>;
        auto original_partitions = partitions_type(partition_key::less_compare(*s));
        for (int i = 0; i < partition_count / 2; i++) {
            auto m = make_new_mutation(s, i + partition_count / 2);
            original_partitions.emplace(m.key(), mutation_partition(*s, m.partition()));
            cache.populate(m);
        }

        // populate memtable with more updated partitions
        auto mt = make_lw_shared<replica::memtable>(s);
        auto updated_partitions = partitions_type(partition_key::less_compare(*s));
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_large_mutation(s, i);
            updated_partitions.emplace(m.key(), mutation_partition(*s, m.partition()));
            mt->apply(m);
        }

        // fill all transient memory
        std::vector<bytes> memory_hog;
        {
            logalloc::reclaim_lock _(tracker.region());
            try {
                while (true) {
                    memory_hog.emplace_back(bytes(bytes::initialized_later(), 4 * 1024));
                }
            } catch (const std::bad_alloc&) {
                // expected
            }
        }

        auto ev = tracker.region().evictor();
        int evicitons_left = 10;
        tracker.region().make_evictable([&] () mutable {
            if (evicitons_left == 0) {
                return memory::reclaiming_result::reclaimed_nothing;
            }
            --evicitons_left;
            return ev();
        });

        bool failed = false;
        try {
            cache.update(row_cache::external_updater([] { }), *mt).get();
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        BOOST_REQUIRE(!evicitons_left); // should have happened

        memory_hog.clear();

        auto has_only = [&] (const partitions_type& partitions) {
            auto reader = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range);
            auto close_reader = deferred_close(reader);
            for (int i = 0; i < partition_count; i++) {
                auto mopt = read_mutation_from_mutation_reader(reader).get();
                if (!mopt) {
                    break;
                }
                auto it = partitions.find(mopt->key());
                BOOST_REQUIRE(it != partitions.end());
                BOOST_REQUIRE(it->second.equal(*s, mopt->partition()));
            }
            BOOST_REQUIRE(!reader().get());
        };

        if (failed) {
            has_only(original_partitions);
        } else {
            has_only(updated_partitions);
        }
    });
}
#endif

class throttled_mutation_source {
private:
    class impl : public enable_lw_shared_from_this<impl> {
        mutation_source _underlying;
        utils::throttle& _throttle;
    private:
        class reader : public delegating_reader_v2 {
            utils::throttle& _throttle;
        public:
            reader(utils::throttle& t, mutation_reader r)
                    : delegating_reader_v2(std::move(r))
                    , _throttle(t)
            {}
            virtual future<> fill_buffer() override {
                return delegating_reader_v2::fill_buffer().finally([this] () {
                    return _throttle.enter();
                });
            }
        };
    public:
        impl(utils::throttle& t, mutation_source underlying)
            : _underlying(std::move(underlying))
            , _throttle(t)
        { }

        mutation_reader make_reader(schema_ptr s, reader_permit permit, const dht::partition_range& pr,
                const query::partition_slice& slice, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
            return make_mutation_reader<reader>(_throttle, _underlying.make_reader_v2(s, std::move(permit), pr, slice, std::move(trace), std::move(fwd)));
        }
    };
    lw_shared_ptr<impl> _impl;
public:
    throttled_mutation_source(utils::throttle& t, mutation_source underlying)
        : _impl(make_lw_shared<impl>(t, std::move(underlying)))
    { }

    operator mutation_source() const {
        return mutation_source([impl = _impl] (schema_ptr s, reader_permit permit, const dht::partition_range& pr,
                const query::partition_slice& slice, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
            return impl->make_reader(std::move(s), std::move(permit), pr, slice, std::move(trace), std::move(fwd));
        });
    }
};
static std::vector<mutation> updated_ring(std::vector<mutation>& mutations) {
    std::vector<mutation> result;
    for (auto&& m : mutations) {
        result.push_back(make_new_mutation(m.schema(), m.key()));
    }
    return result;
}

SEASTAR_TEST_CASE(test_continuity_flag_and_invalidate_race) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto ring = make_ring(s, 4);
        auto mt = make_memtable(s, ring);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        // Bring ring[2]and ring[3] to cache.
        auto range = dht::partition_range::make_starting_with({ ring[2].ring_position(), true });
        assert_that(cache.make_reader(s, semaphore.make_permit(), range))
                .produces(ring[2])
                .produces(ring[3])
                .produces_end_of_stream();

        // Start reader with full range.
        auto rd = assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range));
        rd.produces(ring[0]);

        // Invalidate ring[2] and ring[3]
        cache.invalidate(row_cache::external_updater([] {}), dht::partition_range::make_starting_with({ ring[2].ring_position(), true })).get();

        // Continue previous reader.
        rd.produces(ring[1])
          .produces(ring[2])
          .produces(ring[3])
          .produces_end_of_stream();

        // Start another reader with full range.
        rd = assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range));
        rd.produces(ring[0])
          .produces(ring[1])
          .produces(ring[2]);

        // Invalidate whole cache.
        cache.invalidate(row_cache::external_updater([] {})).get();

        rd.produces(ring[3])
          .produces_end_of_stream();

        // Start yet another reader with full range.
        assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range))
                .produces(ring[0])
                .produces(ring[1])
                .produces(ring[2])
                .produces(ring[3])
                .produces_end_of_stream();;
    });
}

SEASTAR_TEST_CASE(test_cache_population_and_update_race) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        memtable_snapshot_source memtables(s);
        utils::throttle thr;
        auto cache_source = make_decorated_snapshot_source(snapshot_source([&] { return memtables(); }), [&] (mutation_source src) {
            return throttled_mutation_source(thr, std::move(src));
        });
        cache_tracker tracker;

        auto ring = make_ring(s, 3);
        auto mt1 = make_memtable(s, ring);
        memtables.apply(*mt1);

        row_cache cache(s, cache_source, tracker);

        auto ring2 = updated_ring(ring);
        auto mt2 = make_memtable(s, ring2);

        auto f = thr.block();

        auto m0_range = dht::partition_range::make_singular(ring[0].ring_position());
        auto rd1 = cache.make_reader(s, semaphore.make_permit(), m0_range);
        rd1.set_max_buffer_size(1);
        auto rd1_fill_buffer = rd1.fill_buffer();

        auto rd2 = cache.make_reader(s, semaphore.make_permit());
        rd2.set_max_buffer_size(1);
        auto rd2_fill_buffer = rd2.fill_buffer();

        f.get();
        sleep(10ms).get();

        // This update should miss on all partitions
        auto mt2_copy = make_lw_shared<replica::memtable>(s);
        mt2_copy->apply(*mt2, semaphore.make_permit()).get();
        auto update_future = cache.update(row_cache::external_updater([&] { memtables.apply(mt2_copy); }), *mt2);

        auto rd3 = cache.make_reader(s, semaphore.make_permit());

        // rd2, which is in progress, should not prevent forward progress of update()
        thr.unblock();
        update_future.get();

        rd1_fill_buffer.get();
        rd2_fill_buffer.get();

        // Reads started before memtable flush should return previous value, otherwise this test
        // doesn't trigger the conditions it is supposed to protect against.
        assert_that(std::move(rd1)).produces(ring[0]);

        assert_that(std::move(rd2)).produces(ring[0])
            .produces(ring2[1])
            .produces(ring2[2])
            .produces_end_of_stream();

        // Reads started after update was started but before previous populations completed
        // should already see the new data
        assert_that(std::move(rd3))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();

        // Reads started after flush should see new data
        assert_that(cache.make_reader(s, semaphore.make_permit()))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_invalidate) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        int partition_count = 1000;

        // populate cache with some partitions
        std::vector<dht::decorated_key> keys_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_in_cache.push_back(m.decorated_key());
            cache.populate(m);
        }

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }

        // remove a single element from cache
        auto some_element = keys_in_cache.begin() + 547;
        std::vector<dht::decorated_key> keys_not_in_cache;
        keys_not_in_cache.push_back(*some_element);
        cache.invalidate(row_cache::external_updater([] {}), *some_element).get();
        keys_in_cache.erase(some_element);

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }
        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }

        // remove a range of elements
        std::sort(keys_in_cache.begin(), keys_in_cache.end(), [s] (auto& dk1, auto& dk2) {
            return dk1.less_compare(*s, dk2);
        });
        auto some_range_begin = keys_in_cache.begin() + 123;
        auto some_range_end = keys_in_cache.begin() + 423;
        auto range = dht::partition_range::make(
            { *some_range_begin, true }, { *some_range_end, false }
        );
        keys_not_in_cache.insert(keys_not_in_cache.end(), some_range_begin, some_range_end);
        cache.invalidate(row_cache::external_updater([] {}), range).get();
        keys_in_cache.erase(some_range_begin, some_range_end);

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }
        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }
    });
}

SEASTAR_TEST_CASE(test_cache_population_and_clear_race) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        memtable_snapshot_source memtables(s);
        utils::throttle thr;
        auto cache_source = make_decorated_snapshot_source(snapshot_source([&] { return memtables(); }), [&] (mutation_source src) {
            return throttled_mutation_source(thr, std::move(src));
        });
        cache_tracker tracker;

        auto ring = make_ring(s, 3);
        auto mt1 = make_memtable(s, ring);
        memtables.apply(*mt1);

        row_cache cache(s, std::move(cache_source), tracker);

        auto ring2 = updated_ring(ring);
        auto mt2 = make_memtable(s, ring2);

        auto f = thr.block();

        auto rd1 = cache.make_reader(s, semaphore.make_permit());
        rd1.set_max_buffer_size(1);
        auto rd1_fill_buffer = rd1.fill_buffer();

        f.get();
        sleep(10ms).get();

        // This update should miss on all partitions
        auto cache_cleared = cache.invalidate(row_cache::external_updater([&] {
            memtables.apply(mt2);
        }));

        auto rd2 = cache.make_reader(s, semaphore.make_permit());

        // rd1, which is in progress, should not prevent forward progress of clear()
        thr.unblock();
        cache_cleared.get();

        // Reads started before memtable flush should return previous value, otherwise this test
        // doesn't trigger the conditions it is supposed to protect against.

        rd1_fill_buffer.get();

        assert_that(std::move(rd1)).produces(ring[0])
            .produces(ring2[1])
            .produces(ring2[2])
            .produces_end_of_stream();

        // Reads started after clear but before previous populations completed
        // should already see the new data
        assert_that(std::move(rd2))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();

        // Reads started after clear should see new data
        assert_that(cache.make_reader(s, semaphore.make_permit()))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_mvcc) {
    return seastar::async([] {
        auto test = [&] (const mutation& m1, const mutation& m2, bool with_active_memtable_reader) {
            auto s = m1.schema();
            tests::reader_concurrency_semaphore_wrapper semaphore;

            memtable_snapshot_source underlying(s);
            partition_key::equality eq(*s);

            underlying.apply(m1);

            cache_tracker tracker;
            row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

            auto pk = m1.key();
            cache.populate(m1);

            auto rd1 = cache.make_reader(s, semaphore.make_permit());
            rd1.fill_buffer().get();

            auto rd2 = cache.make_reader(s, semaphore.make_permit());
            rd2.fill_buffer().get();

            auto mt1 = make_lw_shared<replica::memtable>(s);
            mt1->apply(m2);

            auto m12 = m1 + m2;

            mutation_reader_opt mt1_reader_opt;
            auto close_mt1_reader = defer([&mt1_reader_opt] {
                if (mt1_reader_opt) {
                    mt1_reader_opt->close().get();
                }
            });
            if (with_active_memtable_reader) {
                mt1_reader_opt = mt1->make_flat_reader(s, semaphore.make_permit());
                mt1_reader_opt->set_max_buffer_size(1);
                mt1_reader_opt->fill_buffer().get();
            }

            auto mt1_copy = make_lw_shared<replica::memtable>(s);
            mt1_copy->apply(*mt1, semaphore.make_permit()).get();
            cache.update(row_cache::external_updater([&] { underlying.apply(mt1_copy); }), *mt1).get();

            auto rd3 = cache.make_reader(s, semaphore.make_permit());
            rd3.fill_buffer().get();

            auto rd4 = cache.make_reader(s, semaphore.make_permit());
            rd4.fill_buffer().get();

            auto rd5 = cache.make_reader(s, semaphore.make_permit());
            rd5.fill_buffer().get();

            assert_that(std::move(rd3)).has_monotonic_positions();

            if (with_active_memtable_reader) {
                SCYLLA_ASSERT(mt1_reader_opt);
                auto mt1_reader_mutation = read_mutation_from_mutation_reader(*mt1_reader_opt).get();
                BOOST_REQUIRE(mt1_reader_mutation);
                assert_that(*mt1_reader_mutation).is_equal_to_compacted(m2);
            }

            assert_that(std::move(rd4)).produces(m12);
            assert_that(std::move(rd1)).produces(m1);

            cache.invalidate(row_cache::external_updater([] {})).get();

            assert_that(std::move(rd2)).produces(m1);
            assert_that(std::move(rd5)).produces(m12);
        };

        for_each_mutation_pair([&] (const mutation& m1_, const mutation& m2_, are_equal) {
            if (m1_.schema() != m2_.schema()) {
                return;
            }
            if (m1_.partition().empty() || m2_.partition().empty()) {
                return;
            }
            auto s = m1_.schema();

            auto m1 = m1_;
            m1.partition().make_fully_continuous();

            mutation_application_stats app_stats;
            auto m2 = mutation(m1.schema(), m1.decorated_key());
            m2.partition().apply(*s, m2_.partition(), *s, app_stats);
            m2.partition().make_fully_continuous();

            test(m1, m2, false);
            test(m1, m2, true);
        });
    });
}

SEASTAR_TEST_CASE(test_slicing_mutation_reader) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pk = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        mutation m(s, pk);
        constexpr auto row_count = 8;
        for (auto i = 0; i < row_count; i++) {
            m.set_clustered_cell(clustering_key_prefix::from_single_value(*s, int32_type->decompose(i)),
                                 to_bytes("v"), data_value(i), api::new_timestamp());
        }

        auto mt = make_memtable(s, {m});
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto run_tests = [&] (auto& ps, std::deque<int> expected) {
            cache.invalidate(row_cache::external_updater([] {})).get();

            auto reader = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);

            reader = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);

            auto dk = dht::decorate_key(*s, pk);
            auto singular_range = dht::partition_range::make_singular(dk);

            reader = cache.make_reader(s, semaphore.make_permit(), singular_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);

            cache.invalidate(row_cache::external_updater([] {})).get();

            reader = cache.make_reader(s, semaphore.make_permit(), singular_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);
        };

        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                              { },
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(2)), false },
                          }).with_range(clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)))
                          .with_range(query::clustering_range {
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(7)) },
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(10)) },
                          }).build();
            run_tests(ps, { 0, 1, 5, 7 });
        }

        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)) },
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(2)) },
                          }).with_range(query::clustering_range {
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(4)), false },
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(6)) },
                          }).with_range(query::clustering_range {
                              query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(7)), false },
                              { },
                          }).build();
            run_tests(ps, { 1, 2, 5, 6 });
        }

        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                              { },
                              { },
                          }).build();
            run_tests(ps, { 0, 1, 2, 3, 4, 5, 6, 7 });
        }

        {
            auto ps = partition_slice_builder(*s)
                    .with_range(query::clustering_range::make_singular(clustering_key_prefix::from_single_value(*s, int32_type->decompose(4))))
                    .build();
            run_tests(ps, { 4 });
        }
    });
}

static void evict_one_partition(cache_tracker& tracker) {
    auto initial = tracker.partitions();
    SCYLLA_ASSERT(initial > 0);
    while (tracker.partitions() == initial) {
        auto ret = tracker.region().evict_some();
        BOOST_REQUIRE(ret == memory::reclaiming_result::reclaimed_something);
    }
}

static void evict_one_row(cache_tracker& tracker) {
    auto initial = tracker.get_stats().rows;
    SCYLLA_ASSERT(initial > 0);
    while (tracker.get_stats().rows == initial) {
        auto ret = tracker.region().evict_some();
        BOOST_REQUIRE(ret == memory::reclaiming_result::reclaimed_something);
    }
}

SEASTAR_TEST_CASE(test_lru) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        int partition_count = 10;

        std::vector<mutation> partitions = make_ring(s, partition_count);
        for (auto&& m : partitions) {
            cache.populate(m);
        }

        auto pr = dht::partition_range::make_ending_with(dht::ring_position(partitions[2].decorated_key()));
        auto rd = cache.make_reader(s, semaphore.make_permit(), pr);
        assert_that(std::move(rd))
                .produces(partitions[0])
                .produces(partitions[1])
                .produces(partitions[2])
                .produces_end_of_stream();

        evict_one_partition(tracker);

        pr = dht::partition_range::make_ending_with(dht::ring_position(partitions[4].decorated_key()));
        rd = cache.make_reader(s, semaphore.make_permit(), pr);
        assert_that(std::move(rd))
                .produces(partitions[0])
                .produces(partitions[1])
                .produces(partitions[2])
                .produces(partitions[4])
                .produces_end_of_stream();

        pr = dht::partition_range::make_singular(dht::ring_position(partitions[5].decorated_key()));
        rd = cache.make_reader(s, semaphore.make_permit(), pr);
        assert_that(std::move(rd))
                .produces(partitions[5])
                .produces_end_of_stream();

        evict_one_partition(tracker);

        rd = cache.make_reader(s, semaphore.make_permit());
        assert_that(std::move(rd))
                .produces(partitions[0])
                .produces(partitions[1])
                .produces(partitions[2])
                .produces(partitions[4])
                .produces(partitions[5])
                .produces(partitions[7])
                .produces(partitions[8])
                .produces(partitions[9])
                .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_update_invalidating) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto mutation_for_key = [&] (dht::decorated_key key) {
            mutation m(s.schema(), key);
            s.add_row(m, s.make_ckey(0), "val");
            return m;
        };

        auto keys = s.make_pkeys(4);

        auto m1 = mutation_for_key(keys[1]);
        underlying.apply(m1);

        auto m2 = mutation_for_key(keys[3]);
        underlying.apply(m2);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces(m2)
            .produces_end_of_stream();

        auto mt = make_lw_shared<replica::memtable>(s.schema());

        auto m3 = mutation_for_key(m1.decorated_key());
        m3.partition().apply(s.new_tombstone());
        auto m4 = mutation_for_key(keys[2]);
        auto m5 = mutation_for_key(keys[0]);
        mt->apply(m3);
        mt->apply(m4);
        mt->apply(m5);

        auto mt_copy = make_lw_shared<replica::memtable>(s.schema());
        mt_copy->apply(*mt, semaphore.make_permit()).get();
        cache.update_invalidating(row_cache::external_updater([&] { underlying.apply(mt_copy); }), *mt).get();

        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m5)
            .produces(m1 + m3)
            .produces(m4)
            .produces(m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_scan_with_partial_partitions) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

        auto pkeys = s.make_pkeys(3);

        mutation m1(s.schema(), pkeys[0]);
        s.add_row(m1, s.make_ckey(0), "v1");
        s.add_row(m1, s.make_ckey(1), "v2");
        s.add_row(m1, s.make_ckey(2), "v3");
        s.add_row(m1, s.make_ckey(3), "v4");
        cache_mt->apply(m1);

        mutation m2(s.schema(), pkeys[1]);
        s.add_row(m2, s.make_ckey(0), "v5");
        s.add_row(m2, s.make_ckey(1), "v6");
        s.add_row(m2, s.make_ckey(2), "v7");
        cache_mt->apply(m2);

        mutation m3(s.schema(), pkeys[2]);
        s.add_row(m3, s.make_ckey(0), "v8");
        s.add_row(m3, s.make_ckey(1), "v9");
        s.add_row(m3, s.make_ckey(2), "v10");
        cache_mt->apply(m3);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        // partially populate all up to middle of m1
        {
            auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_ending_with(s.make_ckey(1)))
                .build();
            auto prange = dht::partition_range::make_ending_with(dht::ring_position(m1.decorated_key()));
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), prange, slice))
                .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
                .produces_end_of_stream();
        }

        // partially populate m3
        {
            auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_ending_with(s.make_ckey(1)))
                .build();
            auto prange = dht::partition_range::make_singular(m3.decorated_key());
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), prange, slice))
                .produces(m3, slice.row_ranges(*s.schema(), m3.key()))
                .produces_end_of_stream();
        }

        // full scan
        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();

        // full scan after full scan
        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_populates_partition_tombstone) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

        auto pkeys = s.make_pkeys(2);

        mutation m1(s.schema(), pkeys[0]);
        s.add_static_row(m1, "val");
        m1.partition().apply(tombstone(s.new_timestamp(), gc_clock::now()));
        cache_mt->apply(m1);

        mutation m2(s.schema(), pkeys[1]);
        s.add_static_row(m2, "val");
        m2.partition().apply(tombstone(s.new_timestamp(), gc_clock::now()));
        cache_mt->apply(m2);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        // singular range case
        {
            auto prange = dht::partition_range::make_singular(dht::ring_position(m1.decorated_key()));
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), prange))
                .produces(m1)
                .produces_end_of_stream();

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), prange)) // over populated
                .produces(m1)
                .produces_end_of_stream();
        }

        // range scan case
        {
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
                .produces(m1)
                .produces(m2)
                .produces_end_of_stream();

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit())) // over populated
                .produces(m1)
                .produces(m2)
                .produces_end_of_stream();
        }
    });
}

// Returns a range tombstone which represents the same writes as this one but governed by a schema
// with reversed clustering key order.
static range_tombstone reversed(range_tombstone rt) {
    rt.reverse();
    return rt;
}

static range_tombstone_change start_change(const range_tombstone& rt) {
    return range_tombstone_change(rt.position(), rt.tomb);
}

static range_tombstone_change end_change(const range_tombstone& rt) {
    return range_tombstone_change(rt.end_position(), {});
}

SEASTAR_TEST_CASE(test_scan_with_partial_partitions_reversed) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

        auto pkeys = s.make_pkeys(3);
        auto rev_schema = s.schema()->make_reversed();

        mutation m1(s.schema(), pkeys[0]);
        s.add_row(m1, s.make_ckey(0), "v0");
        s.add_row(m1, s.make_ckey(1), "v1");
        s.add_row(m1, s.make_ckey(2), "v2");
        s.add_row(m1, s.make_ckey(3), "v3");
        s.add_row(m1, s.make_ckey(4), "v4");
        cache_mt->apply(m1);

        mutation m2(s.schema(), pkeys[1]);
        s.add_row(m2, s.make_ckey(3), "v5");
        s.add_row(m2, s.make_ckey(4), "v6");
        s.add_row(m2, s.make_ckey(5), "v7");
        cache_mt->apply(m2);

        mutation m3(s.schema(), pkeys[2]);
        auto rt1 = s.make_range_tombstone(s.make_ckey_range(0, 3));
        auto rt2 = s.make_range_tombstone(s.make_ckey_range(3, 4));
        m3.partition().apply_delete(*s.schema(), rt1);
        m3.partition().apply_delete(*s.schema(), rt2);
        s.add_row(m3, s.make_ckey(2), "v10");
        cache_mt->apply(m3);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        // partially populate middle of m1, clustering range: [-inf, 1]
        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make_ending_with(s.make_ckey(1)))
                    .build();
            auto prange = dht::partition_range::make_ending_with(dht::ring_position(m1.decorated_key()));
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), prange, slice))
                    .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
                    .produces_end_of_stream();
        }

        // partially populate m3, clustering range: [-inf, 1]
        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make_ending_with(s.make_ckey(1)))
                    .build();
            auto prange = dht::partition_range::make_singular(m3.decorated_key());
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), prange, slice))
                    .produces(m3, slice.row_ranges(*s.schema(), m3.key()))
                    .produces_end_of_stream();
        }

        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(s.make_ckey_range(1, 4))
                    .build();


            auto rev_slice = query::reverse_slice(*s.schema(), slice);
            assert_that(cache.make_reader(rev_schema, semaphore.make_permit(), query::full_partition_range, rev_slice))
                    .produces_partition_start(m1.decorated_key())
                    .produces_row_with_key(s.make_ckey(4))
                    .produces_row_with_key(s.make_ckey(3))
                    .produces_row_with_key(s.make_ckey(2))
                    .produces_row_with_key(s.make_ckey(1))
                    .produces_partition_end()
                    .produces_partition_start(m2.decorated_key())
                    .produces_row_with_key(s.make_ckey(4))
                    .produces_row_with_key(s.make_ckey(3))
                    .produces_partition_end()
                    .produces_partition_start(m3.decorated_key())
                    .produces_range_tombstone_change(start_change(reversed(rt2)))
                    .produces_range_tombstone_change(range_tombstone_change(reversed(rt2).end_position(), rt1.tomb))
                    .produces_row_with_key(s.make_ckey(2))
                    .produces_range_tombstone_change(range_tombstone_change(position_in_partition::after_key(*s.schema(), s.make_ckey(1)), {}))
                    .produces_partition_end()
                    .produces_end_of_stream();
        }

        // Test query slice which has no rows
        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(s.make_ckey_range(0, 1))
                    .build();

            auto rev_slice = query::reverse_slice(*s.schema(), slice);
            auto pr = dht::partition_range::make_singular(m2.decorated_key());

            assert_that(cache.make_reader(rev_schema, semaphore.make_permit(), pr, rev_slice))
                    .produces_eos_or_empty_mutation();

        }

        // full scan to validate cache state
        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
                .produces(m1)
                .produces(m2)
                .produces(m3)
                .produces_end_of_stream();

        // Test full range population on empty cache
        {
            cache.evict();

            auto slice = s.schema()->full_slice();
            auto rev_slice = query::reverse_slice(*s.schema(), slice);
            auto pr = dht::partition_range::make_singular(m2.decorated_key());

            assert_that(cache.make_reader(rev_schema, semaphore.make_permit(), pr, rev_slice))
                    .produces_partition_start(m2.decorated_key())
                    .produces_row_with_key(s.make_ckey(5))
                    .produces_row_with_key(s.make_ckey(4))
                    .produces_row_with_key(s.make_ckey(3))
                    .produces_partition_end()
                    .produces_end_of_stream();
        }
    });
}

// Tests the case of cache reader having to reconcile a range tombstone
// from the underlying mutation source which overlaps with previously emitted
// tombstones.
SEASTAR_TEST_CASE(test_tombstone_merging_in_partial_partition) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        tombstone t0{s.new_timestamp(), gc_clock::now()};
        tombstone t1{s.new_timestamp(), gc_clock::now()};

        mutation m1(s.schema(), pk);
        m1.partition().apply_delete(*s.schema(),
            s.make_range_tombstone(query::clustering_range::make(s.make_ckey(0), s.make_ckey(10)), t0));
        underlying.apply(m1);

        mutation m2(s.schema(), pk);
        m2.partition().apply_delete(*s.schema(),
            s.make_range_tombstone(query::clustering_range::make(s.make_ckey(3), s.make_ckey(6)), t1));
        m2.partition().apply_delete(*s.schema(),
            s.make_range_tombstone(query::clustering_range::make(s.make_ckey(7), s.make_ckey(12)), t1));
        s.add_row(m2, s.make_ckey(4), "val");
        s.add_row(m2, s.make_ckey(8), "val");
        underlying.apply(m2);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        {
            auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_singular(s.make_ckey(4)))
                .build();

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                .produces(m1 + m2, slice.row_ranges(*s.schema(), pk.key()))
                .produces_end_of_stream();
        }

        {
            auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_starting_with(s.make_ckey(4)))
                .build();

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                .produces(m1 + m2, slice.row_ranges(*s.schema(), pk.key()))
                .produces_end_of_stream();

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice)).has_monotonic_positions();
        }
    });
}

static void consume_all(mutation_reader& rd) {
    while (auto mfopt = rd().get()) {}
}

static void populate_range(row_cache& cache, const dht::partition_range& pr = query::full_partition_range,
    const query::clustering_range& r = query::full_clustering_range)
{
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto slice = partition_slice_builder(*cache.schema()).with_range(r).build();
    auto rd = cache.make_reader(cache.schema(), semaphore.make_permit(), pr, slice);
    auto close_rd = deferred_close(rd);
    consume_all(rd);
}

static void apply(row_cache& cache, memtable_snapshot_source& underlying, const mutation& m) {
    auto mt = make_lw_shared<replica::memtable>(m.schema());
    mt->apply(m);
    cache.update(row_cache::external_updater([&] { underlying.apply(m); }), *mt).get();
}

static void apply(row_cache& cache, memtable_snapshot_source& underlying, replica::memtable& m) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto mt1 = make_lw_shared<replica::memtable>(m.schema());
    mt1->apply(m, semaphore.make_permit()).get();
    cache.update(row_cache::external_updater([&] { underlying.apply(std::move(mt1)); }), m).get();
}

SEASTAR_TEST_CASE(test_readers_get_all_data_after_eviction) {
    return seastar::async([] {
        simple_schema table;
        schema_ptr s = table.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        memtable_snapshot_source underlying(s);

        auto m1 = table.new_mutation("pk");
        table.add_row(m1, table.make_ckey(3), "v3");

        auto m2 = table.new_mutation("pk");
        table.add_row(m2, table.make_ckey(1), "v1");
        table.add_row(m2, table.make_ckey(2), "v2");

        underlying.apply(m1);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);
        cache.populate(m1);

        auto apply = [&] (mutation m) {
            ::apply(cache, underlying, m);
        };

        auto make_reader = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, slice);
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return assert_that(std::move(rd));
        };

        auto rd1 = make_reader(s->full_slice());

        apply(m2);

        auto rd2 = make_reader(s->full_slice());

        auto slice_with_key2 = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(table.make_ckey(2)))
            .build();
        auto rd3 = make_reader(slice_with_key2);

        cache.evict();

        rd3.produces_partition_start(m1.decorated_key())
            .produces_row_with_key(table.make_ckey(2))
            .produces_partition_end()
            .produces_end_of_stream();

        rd1.produces(m1);
        rd2.produces(m1 + m2);
    });
}

// Reproduces #3139
SEASTAR_TEST_CASE(test_single_tombstone_with_small_buffer) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        auto rt1 = s.make_range_tombstone(query::clustering_range::make(s.make_ckey(1), s.make_ckey(2)),
            s.new_tombstone());
        m1.partition().apply_delete(*s.schema(), rt1);

        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        populate_range(cache);

        auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
        rd.set_max_buffer_size(1);

        assert_that(std::move(rd)).produces_partition_start(pk)
            .produces_range_tombstone_change(start_change(rt1))
            .produces_range_tombstone_change(end_change(rt1))
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

// Reproduces #3139
SEASTAR_TEST_CASE(test_tombstone_and_row_with_small_buffer) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        auto rt1 = s.make_range_tombstone(query::clustering_range::make(s.make_ckey(1), s.make_ckey(2)),
                                          s.new_tombstone());
        m1.partition().apply_delete(*s.schema(), rt1);
        s.add_row(m1, s.make_ckey(1), "v1");

        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        populate_range(cache);

        auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
        rd.set_max_buffer_size(1);

        assert_that(std::move(rd)).produces_partition_start(pk)
            .produces_range_tombstone_change(start_change(rt1))
            .produces_row_with_key(s.make_ckey(1))
            .produces_range_tombstone_change(end_change(rt1));
    });
}

// Reproducer for https://github.com/scylladb/scylladb/issues/12462
SEASTAR_THREAD_TEST_CASE(test_range_tombstone_adjacent_to_slice_is_closed) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    cache_tracker tracker;
    memtable_snapshot_source underlying(s.schema());

    auto pk = s.make_pkey(0);
    auto pr = dht::partition_range::make_singular(pk);

    mutation m1(s.schema(), pk);
    auto rt0 = s.make_range_tombstone(*position_range_to_clustering_range(position_range(
            position_in_partition::before_key(s.make_ckey(1)),
            position_in_partition::before_key(s.make_ckey(3))), *s.schema()));
    m1.partition().apply_delete(*s.schema(), rt0);
    s.add_row(m1, s.make_ckey(0), "v1");

    underlying.apply(m1);

    row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);
    populate_range(cache);

    // Create a reader to pin the MVCC version
    auto rd0 = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
    auto close_rd0 = deferred_close(rd0);
    rd0.set_max_buffer_size(1);
    rd0.fill_buffer().get();

    mutation m2(s.schema(), pk);
    auto rt1 = s.make_range_tombstone(*position_range_to_clustering_range(position_range(
            position_in_partition::before_key(s.make_ckey(1)),
            position_in_partition::before_key(s.make_ckey(2))), *s.schema()));
    m2.partition().apply_delete(*s.schema(), rt1);
    apply(cache, underlying, m2);

    // State of cache:
    //  v2: ROW(0), RT(before(1), before(2))@t1
    //  v1: RT(before(1), before(3))@t0

    // range_tombstone_change_generator will work with the stream: RT(1, before(2))@t1, RT(before(2), before(3))@t0
    // It's important that there is an RT which starts exactly at the slice upper bound to trigger
    // the problem, and the RT will be in the stream only because it is a residual from RT(before(1), before(3)),
    // which overlaps with the slice in the older version. That's why we need two MVCC versions.

    auto slice = partition_slice_builder(*s.schema())
        .with_range(*position_range_to_clustering_range(position_range(
                position_in_partition::before_key(s.make_ckey(0)),
                position_in_partition::before_key(s.make_ckey(2))), *s.schema()))
        .build();

    assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
        .produces_partition_start(pk)
        .produces_row_with_key(s.make_ckey(0))
        .produces_range_tombstone_change(start_change(rt1))
        .produces_range_tombstone_change(end_change(rt1))
        .produces_partition_end()
        .produces_end_of_stream();
}

//
// Tests the following case of eviction and re-population:
//
// (Before)  <ROW key=0> <RT1> <RT2> <RT3> <ROW key=8, cont=1>
//                       ^--- lower bound  ^---- next row
//
// (After)   <ROW key=0>    <ROW key=8, cont=0> <ROW key=8, cont=1>
//                       ^--- lower bound       ^---- next row
SEASTAR_TEST_CASE(test_tombstones_are_not_missed_when_range_is_invalidated) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        s.add_row(m1, s.make_ckey(0), "v0");
        auto rt1 = s.make_range_tombstone(query::clustering_range::make(s.make_ckey(1), s.make_ckey(2)),
            s.new_tombstone());
        auto rt2 = s.make_range_tombstone(query::clustering_range::make(s.make_ckey(3), s.make_ckey(4)),
            s.new_tombstone());
        auto rt3 = s.make_range_tombstone(query::clustering_range::make(s.make_ckey(5), s.make_ckey(6)),
            s.new_tombstone());
        m1.partition().apply_delete(*s.schema(), rt1);
        m1.partition().apply_delete(*s.schema(), rt2);
        m1.partition().apply_delete(*s.schema(), rt3);
        s.add_row(m1, s.make_ckey(8), "v8");

        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_reader = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return assert_that(std::move(rd));
        };

        // populate using reader in same snapshot
        {
            populate_range(cache);

            auto slice_after_7 = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_starting_with(s.make_ckey(7)))
                .build();

            auto rd2 = make_reader(slice_after_7);

            auto rd = make_reader(s.schema()->full_slice());
            rd.produces_partition_start(pk);
            rd.produces_row_with_key(s.make_ckey(0));
            rd.produces_range_tombstone_change(start_change(rt1));
            rd.produces_range_tombstone_change(end_change(rt1));

            cache.evict();

            rd2.produces_partition_start(pk);
            rd2.produces_row_with_key(s.make_ckey(8));
            rd2.produces_partition_end();
            rd2.produces_end_of_stream();

            rd.produces_range_tombstone_change(start_change(rt2));
            rd.produces_range_tombstone_change(end_change(rt2));
            rd.produces_range_tombstone_change(start_change(rt3));
            rd.produces_range_tombstone_change(end_change(rt3));
            rd.produces_row_with_key(s.make_ckey(8));
            rd.produces_partition_end();
            rd.produces_end_of_stream();
        }

        // populate using reader created after invalidation
        {
            populate_range(cache);

            auto rd = make_reader(s.schema()->full_slice());
            rd.produces_partition_start(pk);
            rd.produces_row_with_key(s.make_ckey(0));
            rd.produces_range_tombstone_change(start_change(rt1));
            rd.produces_range_tombstone_change(end_change(rt1));

            mutation m2(s.schema(), pk);
            s.add_row(m2, s.make_ckey(7), "v7");

            cache.invalidate(row_cache::external_updater([&] {
                underlying.apply(m2);
            })).get();

            populate_range(cache, pr, query::clustering_range::make_starting_with(s.make_ckey(5)));

            rd.produces_range_tombstone_change(start_change(rt2));
            rd.produces_range_tombstone_change(end_change(rt2));
            rd.produces_range_tombstone_change(start_change(rt3));
            rd.produces_range_tombstone_change(end_change(rt3));
            rd.produces_row_with_key(s.make_ckey(8));
            rd.produces_partition_end();
            rd.produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_update_from_memtable) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;

        // keys[0] - in underlying, in cache
        // keys[1] - not in underlying, continuous in cache
        // keys[2] - not in underlying, continuous in cache, with snapshot in source
        // keys[3] - population upper bound
        // keys[4] - in underlying, not in cache
        auto pkeys = s.make_pkeys(5);
        auto population_range = dht::partition_range::make_ending_with({pkeys[3]});

        std::vector<mutation> muts;
        std::vector<mutation> muts2;

        for (auto&& pk : pkeys) {
            mutation mut(s.schema(), pk);
            s.add_row(mut, s.make_ckey(1), "v");
            muts.push_back(mut);
            s.add_row(mut, s.make_ckey(1), "v2");
            muts2.push_back(mut);
        }

        std::vector<mutation> orig;
        orig.push_back(muts[0]);
        orig.push_back(muts[3]);
        orig.push_back(muts[4]);

        bool succeeded = false;

        memtable_snapshot_source underlying(s.schema());
        memory::with_allocation_failures([&] {
            if (succeeded) {
                return;
            }

            for (auto&& m : orig) {
                memory::scoped_critical_alloc_section dfg;
                underlying.apply(m);
            }

            row_cache cache(s.schema(), snapshot_source([&] {
                memory::scoped_critical_alloc_section dfg;
                return underlying();
            }), tracker);

            auto make_reader = [&] (const dht::partition_range& pr) {
                auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
                rd.set_max_buffer_size(1);
                rd.fill_buffer().get();
                return rd;
            };

            populate_range(cache, population_range);
            auto rd1_v1 = assert_that(make_reader(population_range));
            mutation_reader_opt snap;
            auto close_snap = defer([&snap] {
                if (snap) {
                    snap->close().get();
                }
            });

            auto d = defer([&] {
                memory::scoped_critical_alloc_section dfg;
                assert_that(cache.make_reader(cache.schema(), semaphore.make_permit()))
                    .produces(orig)
                    .produces_end_of_stream();

                rd1_v1.produces(muts[0])
                    .produces(muts[3])
                    .produces_end_of_stream();
            });

            auto mt = make_memtable(cache.schema(), muts2);

            // Make snapshot on pkeys[2]
            auto pr = dht::partition_range::make_singular(pkeys[2]);
            snap = mt->make_flat_reader(s.schema(), semaphore.make_permit(), pr);
            snap->set_max_buffer_size(1);
            snap->fill_buffer().get();

            cache.update(row_cache::external_updater([&] {
                memory::scoped_critical_alloc_section dfg;
                auto mt2 = make_memtable(cache.schema(), muts2);
                underlying.apply(std::move(mt2));
            }), *mt).get();

            d.cancel();

            memory::scoped_critical_alloc_section dfg;
            succeeded = true;

            assert_that(cache.make_reader(cache.schema(), semaphore.make_permit()))
                .produces(muts2)
                .produces_end_of_stream();

            rd1_v1.produces(muts[0])
                .produces(muts2[1])
                .produces(muts2[2])
                .produces(muts2[3])
                .produces_end_of_stream();
        });
        tracker.cleaner().drain().get();
        BOOST_REQUIRE_EQUAL(0, tracker.get_stats().rows);
        BOOST_REQUIRE_EQUAL(0, tracker.get_stats().partitions);
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_reads) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        memtable_snapshot_source underlying(s);

        auto mut = make_fully_continuous(gen());
        underlying.apply(mut);

        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto run_queries = [&] {
            auto singular_pr = dht::partition_range::make_singular(mut.decorated_key());
            auto slice = partition_slice_builder(*s).with_ranges(gen.make_random_ranges(3)).build();
            auto&& ranges = slice.row_ranges(*s, mut.key());

            memory::with_allocation_failures([&] {
                auto rd = cache.make_reader(s, semaphore.make_permit(), singular_pr, slice);
                auto close_rd = deferred_close(rd);
                auto got_opt = read_mutation_from_mutation_reader(rd).get();
                BOOST_REQUIRE(got_opt);
                BOOST_REQUIRE(!read_mutation_from_mutation_reader(rd).get());

                assert_that(*got_opt).is_equal_to_compacted(mut, ranges);
                assert_that(cache.make_reader(s, semaphore.make_permit(), singular_pr, slice))
                    .produces(mut, ranges);
            });

            memory::with_allocation_failures([&] {
                auto rd = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, slice);
                auto close_rd = deferred_close(rd);
                auto got_opt = read_mutation_from_mutation_reader(rd).get();
                BOOST_REQUIRE(got_opt);
                BOOST_REQUIRE(!read_mutation_from_mutation_reader(rd).get());

                assert_that(*got_opt).is_equal_to_compacted(mut, ranges);
                assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, slice))
                    .produces(mut, ranges);
            });
        };

        auto run_query = [&] {
            auto slice = partition_slice_builder(*s).with_ranges(gen.make_random_ranges(3)).build();
            auto&& ranges = slice.row_ranges(*s, mut.key());
            memory::with_allocation_failures([&] {
                assert_that(cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, slice))
                    .produces(mut, ranges);
            });
        };

        run_queries();

        auto&& injector = memory::local_failure_injector();
        injector.run_with_callback([&] {
            if (tracker.region().reclaiming_enabled()) {
                tracker.region().full_compaction();
            }
        }, run_query);

        injector.run_with_callback([&] {
            if (tracker.region().reclaiming_enabled()) {
                cache.evict();
            }
        }, run_queries);
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_transitioning_from_underlying_read_to_read_from_cache) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation mut(s.schema(), pk);
        s.add_row(mut, s.make_ckey(6), "v");
        auto rt = s.make_range_tombstone(s.make_ckey_range(3, 4));
        mut.partition().apply_row_tombstone(*s.schema(), rt);
        underlying.apply(mut);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto slice = partition_slice_builder(*s.schema())
            .with_range(s.make_ckey_range(0, 1))
            .with_range(s.make_ckey_range(3, 6))
            .build();

        memory::with_allocation_failures([&] {
            {
                memory::scoped_critical_alloc_section dfg;
                cache.evict();
                populate_range(cache, pr, s.make_ckey_range(6, 10));
            }

            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
            auto close_rd = deferred_close(rd);
            auto got_opt = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got_opt);
            auto mfopt = rd().get();
            BOOST_REQUIRE(!mfopt);

            assert_that(*got_opt).is_equal_to(mut);
        });
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_partition_scan) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pkeys = s.make_pkeys(7);
        std::vector<mutation> muts;

        for (auto&& pk : pkeys) {
            mutation mut(s.schema(), pk);
            s.add_row(mut, s.make_ckey(1), "v");
            muts.push_back(mut);
            underlying.apply(mut);
        }

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        memory::with_allocation_failures([&] {
            {
                memory::scoped_critical_alloc_section dfg;
                cache.evict();
                populate_range(cache, dht::partition_range::make_singular(pkeys[1]));
                populate_range(cache, dht::partition_range::make({pkeys[3]}, {pkeys[5]}));
            }

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
                .produces(muts)
                .produces_end_of_stream();
        });
    });
}

SEASTAR_TEST_CASE(test_concurrent_population_before_latest_version_iterator) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        s.add_row(m1, s.make_ckey(0), "v");
        s.add_row(m1, s.make_ckey(1), "v");
        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_reader = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return assert_that(std::move(rd));
        };

        {
            populate_range(cache, pr, s.make_ckey_range(0, 1));
            auto rd = make_reader(s.schema()->full_slice()); // to keep current version alive

            mutation m2(s.schema(), pk);
            s.add_row(m2, s.make_ckey(2), "v");
            s.add_row(m2, s.make_ckey(3), "v");
            s.add_row(m2, s.make_ckey(4), "v");
            apply(cache, underlying, m2);

            auto slice1 = partition_slice_builder(*s.schema())
                .with_range(s.make_ckey_range(0, 5))
                .build();

            auto rd1 = make_reader(slice1);
            rd1.produces_partition_start(pk);
            rd1.produces_row_with_key(s.make_ckey(0));

            populate_range(cache, pr, s.make_ckey_range(3, 3));

            auto rd2 = make_reader(slice1);

            rd2.produces_partition_start(pk);
            rd2.produces_row_with_key(s.make_ckey(0));

            populate_range(cache, pr, s.make_ckey_range(2, 3));

            rd2.produces_row_with_key(s.make_ckey(1));
            rd2.produces_row_with_key(s.make_ckey(2));
            rd2.produces_row_with_key(s.make_ckey(3));
            rd2.produces_row_with_key(s.make_ckey(4));
            rd2.produces_partition_end();
            rd2.produces_end_of_stream();

            rd1.produces_row_with_key(s.make_ckey(1));
            rd1.produces_row_with_key(s.make_ckey(2));
            rd1.produces_row_with_key(s.make_ckey(3));
            rd1.produces_row_with_key(s.make_ckey(4));
            rd1.produces_partition_end();
            rd1.produces_end_of_stream();
        }

        {
            cache.evict();
            populate_range(cache, pr, s.make_ckey_range(4, 4));

            auto slice1 = partition_slice_builder(*s.schema())
                .with_range(s.make_ckey_range(0, 1))
                .with_range(s.make_ckey_range(3, 3))
                .build();
            auto rd1 = make_reader(slice1);

            rd1.produces_partition_start(pk);
            rd1.produces_row_with_key(s.make_ckey(0));

            populate_range(cache, pr, s.make_ckey_range(2, 4));

            rd1.produces_row_with_key(s.make_ckey(1));
            rd1.produces_row_with_key(s.make_ckey(3));
            rd1.produces_partition_end();
            rd1.produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_concurrent_populating_partition_range_reads) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto keys = s.make_pkeys(10);
        std::vector<mutation> muts;

        for (auto&& k : keys) {
            mutation m(s.schema(), k);
            m.partition().apply(s.new_tombstone());
            muts.push_back(m);
            underlying.apply(m);
        }

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        // Check the case when one reader inserts entries after the other reader's range but before
        // that readers upper bound at the time the read started.

        auto range1 = dht::partition_range::make({keys[0]}, {keys[3]});
        auto range2 = dht::partition_range::make({keys[4]}, {keys[8]});

        populate_range(cache, dht::partition_range::make_singular({keys[0]}));
        populate_range(cache, dht::partition_range::make_singular({keys[1]}));
        populate_range(cache, dht::partition_range::make_singular({keys[6]}));

        // FIXME: When readers have buffering across partitions, limit buffering to 1
        auto rd1 = assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), range1));
        rd1.produces(muts[0]);

        auto rd2 = assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), range2));
        rd2.produces(muts[4]);

        rd1.produces(muts[1]);
        rd1.produces(muts[2]);
        rd1.produces(muts[3]);
        rd1.produces_end_of_stream();

        rd2.produces(muts[5]);
        rd2.produces(muts[6]);
        rd2.produces(muts[7]);
        rd2.produces(muts[8]);
        rd1.produces_end_of_stream();
    });
}

static void populate_range(row_cache& cache, const dht::partition_range& pr, const query::clustering_row_ranges& ranges) {
    for (auto&& r : ranges) {
        populate_range(cache, pr, r);
    }
}

static void check_continuous(row_cache& cache, const dht::partition_range& pr, const query::clustering_range& r = query::full_clustering_range) {
    auto s0 = cache.get_cache_tracker().get_stats();
    populate_range(cache, pr, r);
    auto s1 = cache.get_cache_tracker().get_stats();
    if (s0.reads_with_misses != s1.reads_with_misses) {
        std::cerr << cache << "\n";
        BOOST_FAIL(format("Got cache miss while reading range {}", r));
    }
}

static void check_continuous(row_cache& cache, dht::partition_range& pr, const query::clustering_row_ranges& ranges) {
    for (auto&& r : ranges) {
        check_continuous(cache, pr, r);
    }
}

SEASTAR_TEST_CASE(test_random_row_population) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        s.add_row(m1, s.make_ckey(0), "v0");
        s.add_row(m1, s.make_ckey(2), "v2");
        s.add_row(m1, s.make_ckey(4), "v4");
        s.add_row(m1, s.make_ckey(6), "v6");
        s.add_row(m1, s.make_ckey(8), "v8");
        unsigned max_key = 9;
        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_reader = [&] (const query::partition_slice* slice = nullptr) {
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice ? *slice : s.schema()->full_slice());
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return rd;
        };

        std::vector<query::clustering_range> ranges;

        ranges.push_back(query::clustering_range::make_ending_with({s.make_ckey(5)}));
        ranges.push_back(query::clustering_range::make_starting_with({s.make_ckey(5)}));
        for (unsigned i = 0; i <= max_key; ++i) {
            for (unsigned j = i; j <= max_key; ++j) {
                ranges.push_back(query::clustering_range::make({s.make_ckey(i)}, {s.make_ckey(j)}));
            }
        }

        auto& rng = seastar::testing::local_random_engine;
        std::shuffle(ranges.begin(), ranges.end(), rng);

        struct read {
            std::unique_ptr<query::partition_slice> slice;
            mutation_reader reader;
            mutation_rebuilder_v2 result_builder;

            read() = delete;
            read(std::unique_ptr<query::partition_slice> slice_, mutation_reader reader_, mutation_rebuilder_v2 result_builder_) noexcept
                    : slice(std::move(slice_))
                    , reader(std::move(reader_))
                    , result_builder(std::move(result_builder_))
            { }
            read(read&& o) = default;
            ~read() {
                reader.close().get();
            }
        };

        std::vector<read> readers;
        for (auto&& r : ranges) {
            auto slice = std::make_unique<query::partition_slice>(partition_slice_builder(*s.schema()).with_range(r).build());
            auto rd = make_reader(slice.get());
            auto rb = mutation_rebuilder_v2(s.schema());
            readers.push_back(read{std::move(slice), std::move(rd), std::move(rb)});
        }

        while (!readers.empty()) {
            std::vector<read> remaining_readers;
            for (auto i = readers.begin(); i != readers.end(); i++) {
                auto mfo = i->reader().get();
                if (!mfo) {
                    auto&& ranges = i->slice->row_ranges(*s.schema(), pk.key());
                    auto result = *i->result_builder.consume_end_of_stream();
                    assert_that(result).is_equal_to(m1, ranges);
                } else {
                    i->result_builder.consume(std::move(*mfo));
                    remaining_readers.emplace_back(std::move(*i));
                }
            }
            readers = std::move(remaining_readers);
        }

        check_continuous(cache, pr, query::clustering_range::make({s.make_ckey(0)}, {s.make_ckey(9)}));
    });
}

SEASTAR_TEST_CASE(test_no_misses_when_read_is_repeated) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        memtable_snapshot_source underlying(gen.schema());

        auto m1 = gen();
        underlying.apply(m1);
        auto pr = dht::partition_range::make_singular(m1.decorated_key());

        cache_tracker tracker;
        row_cache cache(gen.schema(), snapshot_source([&] { return underlying(); }), tracker);

        for (auto n_ranges : {1, 2, 4}) {
            auto ranges = gen.make_random_ranges(n_ranges);
            testlog.info("Reading {{{}}}", ranges);

            populate_range(cache, pr, ranges);
            check_continuous(cache, pr, ranges);
            auto s1 = tracker.get_stats();
            populate_range(cache, pr, ranges);
            auto s2 = tracker.get_stats();

            if (s1.reads_with_misses != s2.reads_with_misses) {
                BOOST_FAIL(format("Got cache miss when repeating read of {} on {}", ranges, m1));
            }
        }
    });
}

SEASTAR_TEST_CASE(test_continuity_is_populated_when_read_overlaps_with_older_version) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        s.add_row(m1, s.make_ckey(2), "v2");
        s.add_row(m1, s.make_ckey(4), "v4");
        underlying.apply(m1);

        mutation m2(s.schema(), pk);
        s.add_row(m2, s.make_ckey(6), "v6");
        s.add_row(m2, s.make_ckey(8), "v8");

        mutation m3(s.schema(), pk);
        s.add_row(m3, s.make_ckey(10), "v");
        s.add_row(m3, s.make_ckey(12), "v");

        mutation m4(s.schema(), pk);
        s.add_row(m4, s.make_ckey(14), "v");

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto apply = [&] (mutation m) {
            auto mt = make_lw_shared<replica::memtable>(m.schema());
            mt->apply(m);
            cache.update(row_cache::external_updater([&] { underlying.apply(m); }), *mt).get();
        };

        auto make_reader = [&] {
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return rd;
        };

        {
            auto rd1 = make_reader(); // to keep the old version around
            auto close_rd1 = deferred_close(rd1);

            populate_range(cache, pr, query::clustering_range::make({s.make_ckey(2)}, {s.make_ckey(4)}));

            apply(m2);

            populate_range(cache, pr, s.make_ckey_range(3, 5));
            check_continuous(cache, pr, s.make_ckey_range(2, 5));

            populate_range(cache, pr, s.make_ckey_range(3, 7));
            check_continuous(cache, pr, s.make_ckey_range(2, 7));

            populate_range(cache, pr, s.make_ckey_range(3, 8));
            check_continuous(cache, pr, s.make_ckey_range(2, 8));

            populate_range(cache, pr, s.make_ckey_range(3, 9));
            check_continuous(cache, pr, s.make_ckey_range(2, 9));

            populate_range(cache, pr, s.make_ckey_range(0, 1));
            check_continuous(cache, pr, s.make_ckey_range(0, 1));
            check_continuous(cache, pr, s.make_ckey_range(2, 9));

            populate_range(cache, pr, s.make_ckey_range(1, 2));
            check_continuous(cache, pr, s.make_ckey_range(0, 9));

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                .produces(m1 + m2)
                .produces_end_of_stream();
        }

        cache.evict();

        {
            populate_range(cache, pr, s.make_ckey_range(2, 2));
            populate_range(cache, pr, s.make_ckey_range(5, 5));
            populate_range(cache, pr, s.make_ckey_range(8, 8));

            auto rd1 = make_reader(); // to keep the old version around
            auto close_rd1 = deferred_close(rd1);

            apply(m3);

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                .produces(m1 + m2 + m3)
                .produces_end_of_stream();
        }

        cache.evict();

        { // singular range case
            populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(4)));
            populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(7)));

            auto rd1 = make_reader(); // to keep the old version around
            auto close_rd1 = deferred_close(rd1);

            apply(m4);

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                .produces_compacted(m1 + m2 + m3 + m4, gc_clock::now())
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_continuity_population_with_multicolumn_clustering_key) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck1", int32_type, column_kind::clustering_key)
            .with_column("ck2", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        cache_tracker tracker;
        memtable_snapshot_source underlying(s);

        auto pk = dht::decorate_key(*s,
            partition_key::from_single_value(*s, serialized(3)));
        auto pr = dht::partition_range::make_singular(pk);

        auto ck1 = clustering_key::from_deeply_exploded(*s, {data_value(1), data_value(1)});
        auto ck2 = clustering_key::from_deeply_exploded(*s, {data_value(1), data_value(2)});
        auto ck3 = clustering_key::from_deeply_exploded(*s, {data_value(2), data_value(1)});
        auto ck4 = clustering_key::from_deeply_exploded(*s, {data_value(2), data_value(2)});
        auto ck_3_4 = clustering_key_prefix::from_deeply_exploded(*s, {data_value(2)});
        auto ck5 = clustering_key::from_deeply_exploded(*s, {data_value(3), data_value(1)});
        auto ck6 = clustering_key::from_deeply_exploded(*s, {data_value(3), data_value(2)});

        auto new_tombstone = [] {
            return tombstone(api::new_timestamp(), gc_clock::now());
        };

        mutation m34(s, pk);
        m34.partition().clustered_row(*s, ck3).apply(new_tombstone());
        m34.partition().clustered_row(*s, ck4).apply(new_tombstone());

        mutation m1(s, pk);
        m1.partition().clustered_row(*s, ck2).apply(new_tombstone());
        m1.apply(m34);
        underlying.apply(m1);

        mutation m2(s, pk);
        m2.partition().clustered_row(*s, ck6).apply(new_tombstone());

        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto apply = [&] (mutation m) {
            auto mt = make_lw_shared<replica::memtable>(m.schema());
            mt->apply(m);
            cache.update(row_cache::external_updater([&] { underlying.apply(m); }), *mt).get();
        };

        auto make_reader = [&] (const query::partition_slice* slice = nullptr) {
            auto rd = cache.make_reader(s, semaphore.make_permit(), pr, slice ? *slice : s->full_slice());
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return rd;
        };

        {
            auto range_3_4 = query::clustering_range::make_singular(ck_3_4);
            populate_range(cache, pr, range_3_4);
            check_continuous(cache, pr, range_3_4);

            auto slice1 = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_singular(ck2))
                .build();
            auto rd1 = make_reader(&slice1);
            auto close_rd1 = deferred_close(rd1);

            apply(m2);

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that(std::move(rd1))
                .produces_partition_start(pk)
                .produces_row_with_key(ck2)
                .produces_partition_end()
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, semaphore.make_permit(), pr))
                .produces_compacted(m1 + m2, gc_clock::now())
                .produces_end_of_stream();

            auto slice34 = partition_slice_builder(*s)
                .with_range(range_3_4)
                .build();
            assert_that(cache.make_reader(s, semaphore.make_permit(), pr, slice34))
                .produces_compacted(m34, gc_clock::now())
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_continuity_is_populated_for_single_row_reads) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        s.add_row(m1, s.make_ckey(2), "v2");
        s.add_row(m1, s.make_ckey(4), "v4");
        s.add_row(m1, s.make_ckey(6), "v6");
        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(2)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(2)));

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(6)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(6)));

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(3)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(3)));

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(4)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(4)));

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(1)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(1)));

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(5)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(5)));

        populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(7)));
        check_continuous(cache, pr, query::clustering_range::make_singular(s.make_ckey(7)));

        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces_compacted(m1, gc_clock::now())
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_concurrent_setting_of_continuity_on_read_upper_bound) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        s.add_row(m1, s.make_ckey(0), "v1");
        s.add_row(m1, s.make_ckey(1), "v1");
        s.add_row(m1, s.make_ckey(2), "v1");
        s.add_row(m1, s.make_ckey(3), "v1");
        underlying.apply(m1);

        mutation m2(s.schema(), pk);
        s.add_row(m2, s.make_ckey(4), "v2");

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_rd = [&] (const query::partition_slice* slice = nullptr) {
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice ? *slice : s.schema()->full_slice());
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return rd;
        };

        {
            auto rd1 = make_rd(); // to keep the old version around
            auto close_rd1 = deferred_close(rd1);

            populate_range(cache, pr, s.make_ckey_range(0, 0));
            populate_range(cache, pr, s.make_ckey_range(3, 3));

            apply(cache, underlying, m2);

            auto slice1 = partition_slice_builder(*s.schema())
                .with_range(s.make_ckey_range(0, 4))
                .build();
            auto rd2 = assert_that(make_rd(&slice1));

            rd2.produces_partition_start(pk);
            rd2.produces_row_with_key(s.make_ckey(0));
            rd2.produces_row_with_key(s.make_ckey(1));

            populate_range(cache, pr, s.make_ckey_range(2, 4));

            rd2.produces_row_with_key(s.make_ckey(2));
            rd2.produces_row_with_key(s.make_ckey(3));
            rd2.produces_row_with_key(s.make_ckey(4));
            rd2.produces_partition_end();
            rd2.produces_end_of_stream();

            // FIXME: [1, 2] will not be continuous due to concurrent population.
            // check_continuous(cache, pr, s.make_ckey_range(0, 4));

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                .produces(m1 + m2)
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_tombstone_merging_of_overlapping_tombstones_in_many_versions) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(s.schema(), pk);
        m1.partition().apply_delete(*s.schema(),
            s.make_range_tombstone(s.make_ckey_range(2, 107), s.new_tombstone()));
        s.add_row(m1, s.make_ckey(5), "val");

        // What is important here is that it contains a newer range tombstone
        // which trims [2, 107] from m1 into (100, 107], which starts after ck=5.
        mutation m2(s.schema(), pk);
        m2.partition().apply_delete(*s.schema(),
            s.make_range_tombstone(s.make_ckey_range(1, 100), s.new_tombstone()));

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_reader = [&] {
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit());
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return rd;
        };

        apply(cache, underlying, m1);
        populate_range(cache, pr, s.make_ckey_range(0, 3));

        auto rd1 = make_reader();
        auto close_rd1 = deferred_close(rd1);

        apply(cache, underlying, m2);

        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1 + m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_concurrent_reads_and_eviction) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        gen.set_key_cardinality(16);
        memtable_snapshot_source underlying(gen.schema());
        schema_ptr s = gen.schema();
        schema_ptr rev_s = s->make_reversed();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto m0 = gen();
        m0.partition().make_fully_continuous();
        circular_buffer<mutation> versions;
        size_t last_generation = 0;
        size_t cache_generation = 0; // cache contains only versions >= than this
        underlying.apply(m0);
        versions.emplace_back(m0);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto pr = dht::partition_range::make_singular(m0.decorated_key());
        auto make_reader = [&] (const query::partition_slice& slice) {
            auto reversed = slice.is_reversed();
            auto rd = cache.make_reader(reversed ? rev_s : s, semaphore.make_permit(), pr, slice);
            rd.set_max_buffer_size(3);
            rd.fill_buffer().get();
            return rd;
        };

        const int n_readers = 3;
        std::vector<size_t> generations(n_readers);
        auto gc_versions = [&] {
            auto n_live = last_generation - *boost::min_element(generations) + 1;
            while (versions.size() > n_live) {
                versions.pop_front();
            }
        };

        bool done = false;
        auto readers = parallel_for_each(boost::irange(0, n_readers), [&] (auto id) {
            generations[id] = last_generation;
            return seastar::async([&, id] {
                while (!done) {
                    auto oldest_generation = cache_generation;
                    generations[id] = oldest_generation;
                    gc_versions();

                    bool reversed = tests::random::get_bool();

                    auto fwd_ranges = gen.make_random_ranges(1);
                    auto slice = partition_slice_builder(*s)
                        .with_ranges(fwd_ranges)
                        .build();

                    if (reversed) {
                        slice = query::reverse_slice(*s, std::move(slice));
                    }

                    auto rd = make_reader(slice);
                    auto close_rd = deferred_close(rd);
                    auto actual_opt = read_mutation_from_mutation_reader(rd).get();
                    BOOST_REQUIRE(actual_opt);
                    auto actual = *actual_opt;

                    auto&& ranges = slice.row_ranges(*rd.schema(), actual.key());
                    actual.partition().mutable_row_tombstones().trim(*rd.schema(), ranges);
                    actual = std::move(actual).compacted();

                    auto n_to_consider = last_generation - oldest_generation + 1;
                    auto possible_versions = boost::make_iterator_range(versions.end() - n_to_consider, versions.end());
                    if (!boost::algorithm::any_of(possible_versions, [&] (const mutation& m) {
                        auto m2 = m.sliced(fwd_ranges);
                        if (reversed) {
                            m2 = reverse(std::move(m2));
                        }
                        m2 = std::move(m2).compacted();
                        if (n_to_consider == 1) {
                            assert_that(actual).is_equal_to(m2);
                        }
                        return m2 == actual;
                    })) {
                        BOOST_FAIL(format("Mutation read doesn't match any expected version, slice: {}, read: {}\nexpected: [{}]",
                            slice, actual, fmt::join(possible_versions, ",\n")));
                    }
                }
            }).finally([&] {
                done = true;
            });
        });

        int n_updates = 100;
        while (!done && n_updates--) {
            auto m2 = gen();
            m2.partition().make_fully_continuous();

            bool upgrade_schema = tests::random::get_bool();
            if (upgrade_schema) {
                schema_ptr new_schema = schema_builder(s)
                    .with_column(to_bytes("_phantom"), byte_type)
                    .remove_column("_phantom")
                    .build();
                m2.upgrade(new_schema);
                cache.set_schema(new_schema);
            }

            auto mt = make_lw_shared<replica::memtable>(m2.schema());
            mt->apply(m2);
            cache.update(row_cache::external_updater([&] () noexcept {
                auto snap = underlying();
                underlying.apply(m2);
                auto new_version = versions.back() + m2;
                versions.emplace_back(std::move(new_version));
                ++last_generation;
            }), *mt).get();
            cache_generation = last_generation;

            yield().get();
            tracker.region().evict_some();

            // Don't allow backlog to grow too much to avoid bad_alloc
            const auto max_active_versions = 7;
            while (!done && versions.size() > max_active_versions) {
                yield().get();
            }
        }

        done = true;
        readers.get();

        assert_that(cache.make_reader(s, semaphore.make_permit()))
            .produces(versions.back());
    });
}

SEASTAR_TEST_CASE(test_alter_then_preempted_update_then_memtable_read) {
    return seastar::async([] {
        simple_schema ss;
        memtable_snapshot_source underlying(ss.schema());
        schema_ptr s = ss.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pk = ss.make_pkey("pk");
        mutation m(s, pk);
        mutation m2(s, pk);
        const int c_keys = 10000; // enough for update to be preempted
        for (auto ck : ss.make_ckeys(c_keys)) {
            ss.add_row(m, ck, "tag1");
            ss.add_row(m2, ck, "tag2");
        }

        underlying.apply(m);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto pr = dht::partition_range::make_singular(m.decorated_key());

        // Populate the cache so that update has an entry to update.
        assert_that(cache.make_reader(s, semaphore.make_permit(), pr)).produces(m);

        auto mt2 = make_lw_shared<replica::memtable>(s);
        mt2->apply(m2);

        // Alter the schema
        auto s2 = schema_builder(s)
            .with_column(to_bytes("_a"), byte_type)
            .build();
        cache.set_schema(s2);
        mt2->set_schema(s2);

        auto update_f = cache.update(row_cache::external_updater([&] () noexcept {
            underlying.apply(m2);
        }), *mt2);
        auto wait_for_update = defer([&] { update_f.get(); });

        // Wait for cache update to enter the partition
        while (tracker.get_stats().partition_merges == 0) {
            yield().get();
        }

        auto mt2_reader = mt2->make_flat_reader(s, semaphore.make_permit(), pr, s->full_slice(),
            nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
        auto cache_reader = cache.make_reader(s, semaphore.make_permit(), pr, s->full_slice(),
            nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);

        assert_that(std::move(mt2_reader)).produces(m2);
        assert_that(std::move(cache_reader)).produces(m);

        wait_for_update.cancel();
        update_f.get();

        assert_that(cache.make_reader(s, semaphore.make_permit())).produces(m + m2);
    });
}

SEASTAR_TEST_CASE(test_cache_update_and_eviction_preserves_monotonicity_of_memtable_readers) {
    // Verifies that memtable readers created before memtable is moved to cache
    // are not affected by eviction in cache after their partition entries were moved to cache.
    // Reproduces https://github.com/scylladb/scylla/issues/3186
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        mutation m1 = gen();
        mutation m2 = gen();
        m1.partition().make_fully_continuous();
        m2.partition().make_fully_continuous();

        cache_tracker tracker;
        memtable_snapshot_source underlying(s);
        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker, is_continuous::yes);

        lw_shared_ptr<replica::memtable> mt = make_lw_shared<replica::memtable>(s);

        mt->apply(m1);

        auto mt_rd1 = mt->make_flat_reader(s, semaphore.make_permit());
        mt_rd1.set_max_buffer_size(1);
        mt_rd1.fill_buffer().get();
        BOOST_REQUIRE(mt_rd1.is_buffer_full()); // If fails, increase n_rows

        auto mt_rd2 = mt->make_flat_reader(s, semaphore.make_permit());
        mt_rd2.set_max_buffer_size(1);
        mt_rd2.fill_buffer().get();

        apply(cache, underlying, *mt);

        assert_that(std::move(mt_rd1))
            .produces(m1);

        auto c_rd1 = cache.make_reader(s, semaphore.make_permit());
        c_rd1.set_max_buffer_size(1);
        c_rd1.fill_buffer().get();

        apply(cache, underlying, m2);

        auto c_rd2 = cache.make_reader(s, semaphore.make_permit());
        c_rd2.set_max_buffer_size(1);
        c_rd2.fill_buffer().get();

        cache.evict();

        assert_that(std::move(mt_rd2)).produces(m1);
        assert_that(std::move(c_rd1)).produces(m1);
        assert_that(std::move(c_rd2)).produces(m1 + m2);
    });
}

SEASTAR_TEST_CASE(test_hash_is_cached) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);

        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto mut = make_new_mutation(s);
        memtable_snapshot_source underlying(s);
        underlying.apply(mut);

        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        {
            auto rd = cache.make_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(!row.cells().cell_hash_for(0));
        }

        {
            auto slice = s->full_slice();
            slice.options.set<query::partition_slice::option::with_digest>();
            auto rd = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, slice);
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        {
            auto rd = cache.make_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        auto mt = make_lw_shared<replica::memtable>(s);
        mt->apply(make_new_mutation(s, mut.key()));
        cache.update(row_cache::external_updater([&] { }), *mt).get();

        {
            auto rd = cache.make_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(!row.cells().cell_hash_for(0));
        }

        {
            auto slice = s->full_slice();
            slice.options.set<query::partition_slice::option::with_digest>();
            auto rd = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, slice);
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        {
            auto rd = cache.make_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }
    });
}

SEASTAR_TEST_CASE(test_random_population_with_many_versions) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        memtable_snapshot_source underlying(gen.schema());
        schema_ptr s = gen.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto m1 = gen();
        auto m2 = gen();
        auto m3 = gen();

        m1.partition().make_fully_continuous();
        m2.partition().make_fully_continuous();
        m3.partition().make_fully_continuous();

        cache_tracker tracker;
        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto make_reader = [&] () {
            auto rd = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, s->full_slice());
            rd.set_max_buffer_size(1);
            rd.fill_buffer().get();
            return assert_that(std::move(rd));
        };

        {
            apply(cache, underlying, m1);
            populate_range(cache, query::full_partition_range, gen.make_random_ranges(1));

            auto snap1 = make_reader();

            apply(cache, underlying, m2);
            populate_range(cache, query::full_partition_range, gen.make_random_ranges(1));

            auto snap2 = make_reader();

            apply(cache, underlying, m3);
            populate_range(cache, query::full_partition_range, gen.make_random_ranges(1));

            auto snap3 = make_reader();

            populate_range(cache, query::full_partition_range, gen.make_random_ranges(1));
            populate_range(cache, query::full_partition_range, gen.make_random_ranges(2));
            populate_range(cache, query::full_partition_range, gen.make_random_ranges(3));

            auto snap4 = make_reader();

            snap1.produces(m1);
            snap2.produces(m1 + m2);
            snap3.produces(m1 + m2 + m3);
            snap4.produces(m1 + m2 + m3);
        }

        // After all readers are gone
        make_reader().produces(m1 + m2 + m3);
    });
}

SEASTAR_TEST_CASE(test_static_row_is_kept_alive_by_reads_with_no_clustering_ranges) {
    return seastar::async([] {
        simple_schema table;
        auto s = table.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto mt = make_lw_shared<replica::memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto keys = table.make_pkeys(3);

        mutation m1(s, keys[0]);
        table.add_static_row(m1, "v1");

        mutation m2(s, keys[1]);
        table.add_static_row(m2, "v2");

        mutation m3(s, keys[2]);
        table.add_static_row(m3, "v3");

        cache.populate(m1);
        cache.populate(m2);
        cache.populate(m3);

        {
            auto slice = partition_slice_builder(*s)
                .with_ranges({})
                .build();
            assert_that(cache.make_reader(s, semaphore.make_permit(), dht::partition_range::make_singular(keys[0]), slice))
                .produces(m1);
        }

        evict_one_partition(tracker); // should evict keys[1], not keys[0]

        verify_does_not_have(cache, keys[1]);
        verify_has(cache, keys[0]);
        verify_has(cache, keys[2]);
    });
}

SEASTAR_TEST_CASE(test_eviction_after_old_snapshot_touches_overriden_rows_keeps_later_snapshot_consistent) {
    return seastar::async([] {
        simple_schema table;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto s = table.schema();

        {
            memtable_snapshot_source underlying(s);
            cache_tracker tracker;
            row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

            auto pk = table.make_pkey();

            mutation m1(s, pk);
            table.add_row(m1, table.make_ckey(0), "1");
            table.add_row(m1, table.make_ckey(1), "2");
            table.add_row(m1, table.make_ckey(2), "3");

            mutation m2(s, pk);
            table.add_row(m2, table.make_ckey(0), "1'");
            table.add_row(m2, table.make_ckey(1), "2'");
            table.add_row(m2, table.make_ckey(2), "3'");

            apply(cache, underlying, m1);

            populate_range(cache);

            auto pr1 = dht::partition_range::make_singular(pk);
            auto rd1 = cache.make_reader(s, semaphore.make_permit(), pr1);
            rd1.set_max_buffer_size(1);
            rd1.fill_buffer().get();

            apply(cache, underlying, m2);

            auto pr2 = dht::partition_range::make_singular(pk);
            auto rd2 = cache.make_reader(s, semaphore.make_permit(), pr2);
            rd2.set_max_buffer_size(1);

            auto rd1_a = assert_that(std::move(rd1));
            rd1_a.produces(m1);

            evict_one_row(tracker);
            evict_one_row(tracker);
            evict_one_row(tracker);

            assert_that(std::move(rd2)).produces(m1 + m2);
        }
        {
            memtable_snapshot_source underlying(s);
            cache_tracker tracker;
            row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

            auto pk = table.make_pkey();
            auto pr = dht::partition_range::make_singular(pk);

            mutation m1(s, pk);
            table.add_row(m1, table.make_ckey(0), "1");
            table.add_row(m1, table.make_ckey(1), "2");
            table.add_row(m1, table.make_ckey(2), "3");

            mutation m2(s, pk);
            table.add_row(m2, table.make_ckey(2), "3'");

            apply(cache, underlying, m1);

            populate_range(cache, pr, query::clustering_range::make_singular(table.make_ckey(0)));
            populate_range(cache, pr, query::clustering_range::make_singular(table.make_ckey(1)));
            populate_range(cache, pr, query::clustering_range::make_singular(table.make_ckey(2)));

            auto rd1 = cache.make_reader(s, semaphore.make_permit(), pr);
            rd1.set_max_buffer_size(1);
            rd1.fill_buffer().get();

            apply(cache, underlying, m2);

            auto rd2 = cache.make_reader(s, semaphore.make_permit(), pr);
            rd2.set_max_buffer_size(1);

            auto rd1_a = assert_that(std::move(rd1));
            rd1_a.produces(m1);

            evict_one_row(tracker);
            evict_one_row(tracker);
            evict_one_row(tracker);

            assert_that(std::move(rd2)).produces(m1 + m2);
        }
    });
}

SEASTAR_TEST_CASE(test_reading_progress_with_small_buffer_and_invalidation) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto m1 = s.new_mutation("pk");
        auto result_builder = mutation_rebuilder_v2(s.schema());

        s.delete_range(m1, s.make_ckey_range(3, 10));
        s.delete_range(m1, s.make_ckey_range(4, 10));
        s.add_row(m1, s.make_ckey(6), "v");

        apply(cache, underlying, m1);
        cache.evict();

        auto pkr = dht::partition_range::make_singular(m1.decorated_key());

        populate_range(cache, pkr, s.make_ckey_range(5, 7));
        populate_range(cache, pkr, s.make_ckey_range(4, 7));
        populate_range(cache, pkr, s.make_ckey_range(3, 7));

        auto rd3 = cache.make_reader(s.schema(), semaphore.make_permit(), pkr);
        auto close_rd3 = deferred_close(rd3);
        rd3.set_max_buffer_size(1);

        while (!rd3.is_end_of_stream()) {
            tracker.allocator().invalidate_references();
            rd3.fill_buffer().get();
            while (!rd3.is_buffer_empty()) {
                result_builder.consume(rd3.pop_mutation_fragment());
            }
        }

        auto result = *result_builder.consume_end_of_stream();
        assert_that(result).is_equal_to(m1);
    });
}

SEASTAR_TEST_CASE(test_scans_erase_dummies) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

        auto pkey = s.make_pkey("pk");

        // underlying should not be empty, otherwise cache will make the whole range continuous
        mutation m1(s.schema(), pkey);
        s.add_row(m1, s.make_ckey(0), "v1");
        cache_mt->apply(m1);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        auto pr = dht::partition_range::make_singular(pkey);

        auto populate_range = [&] (int start, int end) {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make(s.make_ckey(start), s.make_ckey(end)))
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .produces_partition_start(pkey)
                    .produces_partition_end()
                    .produces_end_of_stream();
        };

        populate_range(10, 20);

        // Expect 3 dummies, 2 for the last query's bounds and 1 for the last dummy.
        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 3);

        populate_range(5, 15);

        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 3);

        populate_range(16, 21);

        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 3);

        populate_range(30, 31);

        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 5);

        populate_range(2, 40);

        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 3);

        // full scan
        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces_end_of_stream();

        BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 2);
    });
}

SEASTAR_TEST_CASE(test_range_tombstone_adjacent_with_population_bound) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pkey = s.make_pkey("pk");

        mutation m1(s.schema(), pkey);
        auto k1 = s.make_ckey(7);
        s.add_row(m1, k1, "v1");

        memtable_snapshot_source underlying(s.schema());
        underlying.apply(m1);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto pr = dht::partition_range::make_singular(pkey);

        // Force k1 into cache without dummy entries before k1.
        // Needed for later range population to end at k1.
        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make_singular(k1))
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .has_monotonic_positions();
        }

        auto r1 = *position_range_to_clustering_range(position_range(
                position_in_partition::before_all_clustered_rows(), position_in_partition::before_key(k1)), *s.schema());
        s.delete_range(m1, r1);

        auto mt2 = make_lw_shared<replica::memtable>(s.schema());
        mt2->apply(m1);
        cache.update(row_cache::external_updater([&] {
            underlying.apply(m1);
        }), *mt2).get();

        {
            auto slice = partition_slice_builder(*s.schema()).build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .produces(m1)
                    .produces_end_of_stream();
        }

        // full scan
        assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_single_row_query_with_range_tombstone_is_cached) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pkey = s.make_pkey("pk");

        mutation m1(s.schema(), pkey);
        s.delete_range(m1, query::full_clustering_range);
        auto k1 = s.make_ckey(7);
        s.add_row(m1, k1, "v1");

        memtable_snapshot_source underlying(s.schema());
        underlying.apply(m1);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto pr = dht::partition_range::make_singular(pkey);

        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make_singular(k1))
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .produces(m1, slice.row_ranges(*s.schema(), pkey.key()));
        }

        auto misses_before = tracker.get_stats().row_misses;

        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make_singular(k1))
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .produces(m1, slice.row_ranges(*s.schema(), pkey.key()));
        }

        BOOST_REQUIRE_EQUAL(misses_before, tracker.get_stats().row_misses);
    });
}

// Tests the following scenario:
//
// Initial state:
//
//   v2: ==== <7> [entry2] ==== <9> === <13> ==== <last dummy>
//   v1: ======================================== <last dummy> [entry1]
//
// After two eviction events which evict entry1 and entry2, we should end up with:
//
//   v2: ---------------------- <9> === <13> ==== <last dummy>
//   v1: ---------------------------------------- <last dummy>
//
// last dummy entries are treated in a special way in rows_entry::on_evicted(), and there
// was a bug which didn't clear the continuity on last dummy when it was selected for eviction.
// As a result, the view was this:
//
//   v2: ---------------------- <9> === <13> ==== <last dummy>
//   v1: ======================================== <last dummy>
//
// This would violate the "older versions are evicted first" rule, which implies
// that when entry2 is evicted in v2, the range in which entry2 falls into in all older versions
// must be discontinuous. This won't hold if we don't clear continuity on last dummy in v1.
// As a result, the range into which entry2 falls into from the perspective of v2 snapshot
// would appear as continuous and <7> would be missing from the read result, because
// continuity of a snapshot is a union of continuous ranges in all versions.
//
// Reproduces https://github.com/scylladb/scylladb/issues/12451
SEASTAR_TEST_CASE(test_version_merging_with_range_tombstones_over_rowless_version) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pkey = s.make_pkey("pk");
        auto pr = dht::partition_range::make_singular(pkey);

        memtable_snapshot_source underlying(s.schema());

        mutation m1(s.schema(), pkey);
        m1.partition().apply(s.new_tombstone());
        underlying.apply(m1);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        // Populate cache
        assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                .produces(m1);

        mutation m2(s.schema(), pkey);
        s.delete_range(m2, s.make_ckey_range(7, 13));
        s.add_row(m2, s.make_ckey(7), "v");
        s.delete_range(m2, s.make_ckey_range(9, 17));
        s.add_row(m2, s.make_ckey(9), "v");
        s.add_row(m2, s.make_ckey(17), "v");

        {
            auto rd1 = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
            auto close_rd1 = deferred_close(rd1);
            rd1.set_max_buffer_size(1); // To hold the snapshot
            rd1.fill_buffer().get();

            apply(cache, underlying, m2);

            evict_one_row(tracker); // hits last dummy in oldest version.

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                    .produces(m1 + m2);

            evict_one_row(tracker); // hits entry in the latest version, row (v1) or rtc (v2)

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                    .produces(m1 + m2);

            evict_one_row(tracker); // hits entry in the latest version, row (both v1 and v2)

            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                    .produces(m1 + m2);
        }

        tracker.cleaner().drain().get();

        assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
                .produces(m1 + m2);
    });
}

SEASTAR_TEST_CASE(row_cache_is_populated_using_compacting_sstable_reader) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        replica::database& db = env.local_db();
        service::migration_manager& mm = env.migration_manager().local();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        sstring ks_name = "ks";
        sstring table_name = "table_name";

        schema_ptr s = schema_builder(ks_name, table_name)
            .with_column(to_bytes("pk"), int32_type, column_kind::partition_key)
            .with_column(to_bytes("ck"), int32_type, column_kind::clustering_key)
            .with_column(to_bytes("id"), int32_type)
            .build();
        mm.announce(
            service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s, api::new_timestamp()).get(),
            mm.start_group0_operation().get(),
            ""
        ).get();

        replica::table& t = db.find_column_family(ks_name, table_name);

        dht::decorated_key pk = dht::decorate_key(*s, partition_key::from_single_value(*s, serialized(1)));
        clustering_key ck = clustering_key::from_single_value(*s, serialized(2));

        // Create two separate sstables that will contain only a tombstone after compaction
        mutation m_insert = mutation(s, pk);
        m_insert.set_clustered_cell(ck, to_bytes("id"), data_value(3), api::new_timestamp());
        t.apply(m_insert);
        t.flush().get();

        mutation m_delete = mutation(s, pk);
        m_delete.partition().apply(tombstone{api::new_timestamp(), gc_clock::now()});
        t.apply(m_delete);
        t.flush().get();

        // Clear the cache and repopulate it by reading sstables
        t.get_row_cache().evict();

        auto reader = t.get_row_cache().make_reader(s, semaphore.make_permit());
        deferred_close dc{reader};
        reader.consume_pausable([s](mutation_fragment_v2&& mf) {
            return stop_iteration::no;
        }).get();

        // We should have the cache entry, but it can only contain the dummy
        // row, necessary to denote the tombstone. If we have more than one
        // row, then cache contains both the original row and the tombstone
        // meaning the input from sstables wasn't compacted and we put
        // unnecessary pressure on the cache.
        cache_entry& entry = t.get_row_cache().lookup(pk);
        const utils::immutable_collection<mutation_partition::rows_type>& rt = entry.partition().version()->partition().clustered_rows();
        BOOST_ASSERT(rt.calculate_size() == 1);
    });
}

SEASTAR_TEST_CASE(test_eviction_of_upper_bound_of_population_range) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

        auto pkey = s.make_pkey("pk");

        mutation m1(s.schema(), pkey);
        s.add_row(m1, s.make_ckey(1), "v1");
        s.add_row(m1, s.make_ckey(2), "v2");
        cache_mt->apply(m1);

        cache_tracker tracker;
        utils::throttle thr(true);
        auto cache_source = make_decorated_snapshot_source(snapshot_source([&] { return cache_mt->as_data_source(); }),
                                                           [&] (mutation_source src) {
            return throttled_mutation_source(thr, std::move(src));
        });
        row_cache cache(s.schema(), cache_source, tracker);

        auto pr = dht::partition_range::make_singular(pkey);

        auto read = [&] (int start, int end) {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make(s.make_ckey(start), s.make_ckey(end)))
                    .build();
            auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
            auto close_rd = deferred_close(rd);
            auto m_cache = read_mutation_from_mutation_reader(rd).get();
            close_rd.close_now();
            rd = cache_mt->make_flat_reader(s.schema(), semaphore.make_permit(), pr, slice);
            auto close_rd2 = deferred_close(rd);
            auto m_mt = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(m_mt);
            assert_that(m_cache).has_mutation().is_equal_to(*m_mt);
        };

        // populate [2]
        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make_singular(s.make_ckey(2)))
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .has_monotonic_positions();
        }

        auto arrived = thr.block();

        // Read [0, 2]
        auto f = seastar::async([&] {
            read(0, 2);
        });

        arrived.get();

        // populate (2, 3]
        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(query::clustering_range::make(query::clustering_range::bound(s.make_ckey(2), false),
                                                              query::clustering_range::bound(s.make_ckey(3), true)))
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .has_monotonic_positions();
        }

        testlog.trace("Evicting");
        evict_one_row(tracker); // Evicts before(0)
        evict_one_row(tracker); // Evicts ck(2)
        testlog.trace("Unblocking");

        thr.unblock();
        f.get();

        read(0, 3);
    });
}

// Checks that merging rows from different partition versions preserves the LRU link of the entry
// from the newer version. We need this in case we're merging two last dummy entries where the older
// dummy is already unlinked from the LRU. We need to preserve the fact that the last dummy in the
// newer version is still linked, which may be the last entry which is still holding the partition
// entry. Otherwise, we may end up with the partition entry not having any entries linked in the LRU,
// and we'll end up with an unevictable empty partition entry.
// If we preserve the LRU link from the newer version, we'll be able to evict the partition entry
// due to the "older versions are evicted first" rule.
SEASTAR_TEST_CASE(test_partition_entry_evicted_with_dummy_rows_unlinked_in_oldest_mvcc_version) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        memtable_snapshot_source underlying(s.schema());

        auto pkey = s.make_pkey("pk");

        mutation m1(s.schema(), pkey);
        m1.partition().apply(s.new_tombstone());
        underlying.apply(m1);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto pr = dht::partition_range::make_singular(pkey);

        {
            auto rd1 = cache.make_reader(s.schema(), semaphore.make_permit(), pr);
            auto close_rd = deferred_close(rd1);
            rd1.set_max_buffer_size(1);
            rd1.fill_buffer().get();

            mutation m2(s.schema(), pkey);
            m2.partition().apply(s.new_tombstone());
            apply(cache, underlying, m2);

            BOOST_REQUIRE_EQUAL(tracker.get_stats().partitions, 1);
            BOOST_REQUIRE_EQUAL(tracker.get_stats().rows, 2); // 2 dummy rows, one in each version.

            tracker.evict_from_lru_shallow();
        }

        cache.evict();

        tracker.cleaner().drain().get();
        tracker.memtable_cleaner().drain().get();

        BOOST_REQUIRE_EQUAL(tracker.get_stats().partitions, 0);
    });
}

SEASTAR_TEST_CASE(test_reading_of_nonfull_keys) {
        return seastar::async([] {
            schema_ptr s = schema_builder("ks", "cf")
                    .with_column("pk", utf8_type, column_kind::partition_key)
                    .with_column("ck1", utf8_type, column_kind::clustering_key)
                    .with_column("ck2", utf8_type, column_kind::clustering_key)
                    .with_column("v", utf8_type)
                    .build();

            auto pkey = dht::decorate_key(*s, partition_key::from_single_value(*s, serialized("pk1")));

            auto make_ck = [&] (sstring ck1, std::optional<sstring> ck2 = {}) {
                if (ck2) {
                    return clustering_key::from_exploded(*s, {serialized(ck1), serialized(*ck2)});
                }
                return clustering_key::from_exploded(*s, {serialized(ck1)});
            };

            auto prefix_a = make_ck("a");
            auto full_a = prefix_a;
            clustering_key::make_full(*s, full_a);
            auto full_a_a = make_ck("a", "a");

            tests::reader_concurrency_semaphore_wrapper semaphore;

            auto pr = dht::partition_range::make_singular(pkey);

            memtable_snapshot_source underlying(s);

            auto t1 = tombstone(api::new_timestamp(), gc_clock::now());
            mutation m1(s, pkey);
            m1.partition().clustered_row(*s, prefix_a).apply(t1);
            m1.partition().clustered_row(*s, full_a).apply(t1);
            m1.partition().clustered_row(*s, full_a_a).apply(t1);
            underlying.apply(m1);

            cache_tracker tracker;
            row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

            // populating read
            assert_that(cache.make_reader(s, semaphore.make_permit(), pr))
                    .produces(m1);

            // non-populating read
            assert_that(cache.make_reader(s, semaphore.make_permit(), pr))
                    .produces(m1);
        });
}

SEASTAR_TEST_CASE(test_populating_cache_with_expired_and_nonexpired_tombstones) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        sstring ks_name = "ks";
        sstring table_name = "test_pop_cache_tomb_table";

        env.execute_cql(format(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = "
            "{{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}};", ks_name)).get();
        env.execute_cql(format(
            "CREATE TABLE {}.{} (pk int, ck int, PRIMARY KEY(pk, ck));", ks_name, table_name)).get();

        BOOST_REQUIRE(env.local_db().has_schema(ks_name, table_name));

        replica::table& t = env.local_db().find_column_family(ks_name, table_name);
        schema_ptr s = t.schema();

        dht::decorated_key dk = tests::generate_partition_key(s);

        auto ck1 = clustering_key::from_deeply_exploded(*s, {1});
        auto ck1_prefix = clustering_key_prefix::from_deeply_exploded(*s, {1});
        auto ck2 = clustering_key::from_deeply_exploded(*s, {2});
        auto ck2_prefix = clustering_key_prefix::from_deeply_exploded(*s, {2});

        auto dt_noexp = gc_clock::now();
        auto dt_exp = gc_clock::now() - std::chrono::seconds(s->gc_grace_seconds().count() + 1);

        mutation m(s, dk);
        m.partition().apply_delete(*s, ck1_prefix, tombstone(1, dt_noexp)); // create non-expired tombstone
        m.partition().apply_delete(*s, ck2_prefix, tombstone(2, dt_exp)); // create expired tombstone
        t.apply(m);
        t.flush().get();

        // Clear the cache and repopulate it by reading sstables
        t.get_row_cache().evict();

        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto reader = t.get_row_cache().make_reader(s, semaphore.make_permit());
        deferred_close dc{reader};
        reader.consume_pausable([s](mutation_fragment_v2&& mf) {
            return stop_iteration::no;
        }).get();

        cache_entry& entry = t.get_row_cache().lookup(dk);
        auto& cp = entry.partition().version()->partition();

        BOOST_REQUIRE_EQUAL(cp.clustered_row(*s, ck1).deleted_at(), row_tombstone(tombstone(1, dt_noexp))); // non-expired tombstone is in cache
        BOOST_REQUIRE(cp.find_row(*s, ck2) == nullptr); // expired tombstone isn't in cache

        const auto rows = cp.non_dummy_rows();
        BOOST_REQUIRE(std::distance(rows.begin(), rows.end()) == 1); // cache contains non-expired row only
    });
}

SEASTAR_THREAD_TEST_CASE(test_population_of_subrange_of_expired_partition) {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pkey = s.make_pkey("pk");
        auto pr = dht::partition_range::make_singular(pkey);

        mutation m1(s.schema(), pkey);
        s.delete_range(m1, s.make_ckey_range(5, 10));
        auto k1 = s.make_ckey(7);
        s.add_row(m1, k1, "v1");

        memtable_snapshot_source underlying(s.schema());
        underlying.apply(m1);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        {
            auto slice = partition_slice_builder(*s.schema())
                    .with_range(s.make_ckey_range(5, 10)) // Should cover all tombstones so that the reader produces m1.
                    .build();
            assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice))
                    .has_monotonic_positions();
        }

        BOOST_REQUIRE(tracker.get_stats().rows > 0);

        // Simulate compaction removing the partition.
        // Shouldn't affect what's already in cache.
        underlying.clear();
        cache.refresh_snapshot();

        assert_that(cache.make_reader(s.schema(), semaphore.make_permit(), pr))
        .produces(m1)
        .produces_end_of_stream();
}

// Reproducer for #14110.
// Forces a scenario where digest is calculated for rows in old MVCC
// versions, incompatible with the current schema.
// In the original issue, this crashed the node with an SCYLLA_ASSERT failure,
// because the digest calculation was passed the current schema,
// instead of the row's actual old schema.
SEASTAR_THREAD_TEST_CASE(test_digest_read_during_schema_upgrade) {
    // The test will insert a row into the cache,
    // then drop a column, and read the old row with the new schema.
    // If the old row was processed with the new schema,
    // the test would fail because one of the row's columns would
    // have no definition.
    auto s1 = schema_builder("ks", "cf")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("v1", utf8_type, column_kind::regular_column)
        .build();
    auto s2 = schema_builder(s1)
        .remove_column("v1")
        .build();

    // Create a mutation with one row, with inconsequential keys and values.
    auto pk = partition_key::from_single_value(*s1, serialized(0));
    auto m1 = std::invoke([s1, pk] {
        auto x = mutation(s1, pk);
        auto ck = clustering_key::from_single_value(*s1, serialized(0));
        x.set_clustered_cell(ck, "v1", "v1_value", api::new_timestamp());
        return x;
    });

    // Populate the cache with m1.
    memtable_snapshot_source underlying(s1);
    underlying.apply(m1);
    cache_tracker tracker;
    row_cache cache(s1, snapshot_source([&] { return underlying(); }), tracker);
    populate_range(cache);

    // A schema upgrade of a MVCC version happens by adding an empty version
    // with the new schema next to it, and merging the old-schema version into
    // the new-schema version.
    //
    // We want to test a read of rows which are still in the old-schema
    // version. To ensure that, we have to prevent mutation_cleaner from
    // merging the versions until the test is done.
    auto pause_background_merges = tracker.cleaner().pause();

    // Upgrade the cache
    cache.set_schema(s2);

    // Create a digest-requesting reader for the tested partition.
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto pr = dht::partition_range::make_singular(dht::decorate_key(*s1, pk));
    auto slice = partition_slice_builder(*s2)
        .with_option<query::partition_slice::option::with_digest>()
        .build();
    auto rd = cache.make_reader(s2, semaphore.make_permit(), pr, slice);
    auto close_rd = deferred_close(rd);

    // In the original issue reproduced by this test, the read would crash
    // on an SCYLLA_ASSERT.
    // So what we are really testing below is that the read doesn't crash.
    // The comparison with m2 is just a sanity check.
    auto m2 = m1;
    m2.upgrade(s2);
    assert_that(std::move(rd)).produces(m2);
}

SEASTAR_TEST_CASE(test_cache_compacts_expired_tombstones_on_read) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();

        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto pkey = tests::generate_partition_key(s);

        auto make_ck = [&s] (int v) {
            return clustering_key::from_deeply_exploded(*s, {data_value{v}});
        };

        auto make_prefix = [&s] (int v) {
            return clustering_key_prefix::from_deeply_exploded(*s, {data_value{v}});
        };

        auto ck1 = make_ck(1);
        auto ck2 = make_ck(2);
        auto ck3 = make_ck(3);
        auto dt_noexp = gc_clock::now();
        auto dt_exp = gc_clock::now() - std::chrono::seconds(s->gc_grace_seconds().count() + 1);

        auto mt = make_lw_shared<replica::memtable>(s);
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        {
            mutation m(s, pkey);
            m.set_clustered_cell(ck1, "v", data_value(101), 1);
            m.partition().apply_delete(*s, make_prefix(2), tombstone(1, dt_noexp)); // create non-expired tombstone
            m.partition().apply_delete(*s, make_prefix(3), tombstone(2, dt_exp)); // create expired tombstone
            cache.populate(m);
        }

        tombstone_gc_state gc_state(nullptr);
        auto rd1 = cache.make_reader(s, semaphore.make_permit(), query::full_partition_range, &gc_state);
        auto close_rd = deferred_close(rd1);
        rd1.fill_buffer().get(); // cache_mutation_reader compacts cache on fill buffer

        cache_entry& entry = cache.lookup(pkey);
        auto& cp = entry.partition().version()->partition();

        BOOST_REQUIRE(cp.find_row(*s, ck1) != nullptr); // live row is in cache
        BOOST_REQUIRE_EQUAL(cp.clustered_row(*s, ck2).deleted_at(), row_tombstone(tombstone(1, dt_noexp))); // non-expired tombstone is in cache
        BOOST_REQUIRE(cp.find_row(*s, ck3) == nullptr); // expired tombstone isn't in cache

        // check tracker stats
        auto &tracker_stats = tracker.get_stats();
        BOOST_REQUIRE(tracker_stats.rows_compacted == 1);
        BOOST_REQUIRE(tracker_stats.rows_compacted_away == 1);
    });
}

SEASTAR_TEST_CASE(test_compact_range_tombstones_on_read) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        auto ck0 = s.make_ckey(0);
        auto ck1 = s.make_ckey(1);
        auto ck2 = s.make_ckey(2);
        auto ck3 = s.make_ckey(3);

        auto dt_noexp = gc_clock::now();
        auto dt_exp = gc_clock::now() - std::chrono::seconds(s.schema()->gc_grace_seconds().count() + 1);

        mutation m(s.schema(), pk);
        auto rt1 = s.make_range_tombstone(s.make_ckey_range(2, 3), dt_noexp);
        auto rt2 = s.make_range_tombstone(s.make_ckey_range(1, 2), dt_exp);
        m.partition().apply_delete(*s.schema(), rt1);
        m.partition().apply_delete(*s.schema(), rt2);
        s.add_row(m, ck0, "v0");
        s.add_row(m, ck1, "v1");
        s.add_row(m, ck2, "v2");
        s.add_row(m, ck3, "v3");
        cache.populate(m);

        tombstone_gc_state gc_state(nullptr);

        cache_entry& entry = cache.lookup(pk);
        auto& cp = entry.partition().version()->partition();

        // check all rows are in cache
        BOOST_REQUIRE(cp.find_row(*s.schema(), ck0) != nullptr);
        BOOST_REQUIRE(cp.find_row(*s.schema(), ck1) != nullptr);
        BOOST_REQUIRE(cp.find_row(*s.schema(), ck2) != nullptr);
        BOOST_REQUIRE(cp.find_row(*s.schema(), ck3) != nullptr);

        // workaround: make row cells to be compacted during next read
        auto set_cells_timestamp_to_min = [&](deletable_row& row) {
            row.cells().for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
                const column_definition& def = s.schema()->column_at(column_kind::clustering_key, id);

                auto cell_view = cell.as_mutable_atomic_cell(def);
                cell_view.set_timestamp(api::min_timestamp);
            });
        };

        set_cells_timestamp_to_min(cp.clustered_row(*s.schema(), ck0));
        set_cells_timestamp_to_min(cp.clustered_row(*s.schema(), ck1));
        set_cells_timestamp_to_min(cp.clustered_row(*s.schema(), ck2));
        set_cells_timestamp_to_min(cp.clustered_row(*s.schema(), ck3));

        {
            auto rd1 = cache.make_reader(s.schema(), semaphore.make_permit(), pr, &gc_state);
            auto close_rd1 = deferred_close(rd1);
            rd1.fill_buffer().get();

            // check some rows are compacted on read from cache
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck0) != nullptr);
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck1) == nullptr);
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck2) == nullptr);
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck3) != nullptr);
        }

        {
            auto rd2 = cache.make_reader(s.schema(), semaphore.make_permit(), pr, &gc_state);
            auto close_rd2 = deferred_close(rd2);
            rd2.fill_buffer().get();

            // check compacted rows weren't resurrected on second read from cache
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck0) != nullptr);
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck1) == nullptr);
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck2) == nullptr);
            BOOST_REQUIRE(cp.find_row(*s.schema(), ck3) != nullptr);
        }

        // check tracker stats
        auto &tracker_stats = tracker.get_stats();
        BOOST_REQUIRE(tracker_stats.rows_compacted == 2);
        BOOST_REQUIRE(tracker_stats.rows_compacted_away == 2);
    });
}

// Reproduces #15278
// Check that the semaphore's OOM kill doesn't send LSA allocating sections
// into a tailspin, retrying the failing code, with increase reserves, which
// of course doesn't necessarily help release pressure on the semaphore.
SEASTAR_THREAD_TEST_CASE(test_cache_reader_semaphore_oom_kill) {
    simple_schema s;
    reader_concurrency_semaphore semaphore(100, 1, get_name(), std::numeric_limits<size_t>::max(), utils::updateable_value<uint32_t>(1),
            utils::updateable_value<uint32_t>(1), reader_concurrency_semaphore::register_metrics::no);
    auto stop_semaphore = deferred_stop(semaphore);

    cache_tracker tracker;
    auto cache_mt = make_lw_shared<replica::memtable>(s.schema());
    row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

    auto pk = s.make_pkey(0);

    mutation m(s.schema(), pk);
    s.add_row(m, s.make_ckey(0), sstring(1024, '0'));
    cache.populate(m);

    auto pr = dht::partition_range::make_singular(pk);
    tombstone_gc_state gc_state(nullptr);

    BOOST_REQUIRE_EQUAL(semaphore.get_stats().total_reads_killed_due_to_kill_limit, 0);
    auto kill_limit_before = 0;

    // Check different amounts of memory consumed before the read, so the OOM kill is triggered in different places.
    for (unsigned memory = 1; memory <= 512; memory *= 2) {
        semaphore.set_resources({1, memory});
        auto permit = semaphore.obtain_permit(s.schema(), "read", 0, db::no_timeout, {}).get();
        auto create_reader_and_read_all = [&] {
            auto rd = cache.make_reader(s.schema(), permit, pr, &gc_state);
            auto close_rd = deferred_close(rd);
            while (rd().get());
        };
        BOOST_REQUIRE_THROW(create_reader_and_read_all(), utils::memory_limit_reached);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().total_reads_killed_due_to_kill_limit, ++kill_limit_before);
    }
}

// Reproducer for #16759.
//
#ifdef SCYLLA_ENABLE_PREEMPTION_SOURCE
SEASTAR_THREAD_TEST_CASE(test_preempt_cache_update) {
    // Starts requesting preemption after the given number of checks.
    // On the first yield, blocks on a semaphore until it's later
    // unblocked by the test.
    struct preempter : public custom_preemption_source::impl {
        semaphore wait_for_block{0};
        semaphore wait_for_unblock{0};
        uint64_t yields = 0;
        int64_t until_preempt;
        preempter(uint64_t count) : until_preempt(count) {}
        bool should_preempt() override {
            return (--until_preempt == 0);
        }
        void thread_yield() override {
            if (yields++ == 0) {
                wait_for_block.signal();
                wait_for_unblock.wait().get();
            }
        }
    };

    // Create a few mutations with multiple rows.
    simple_schema s;
    auto keys = s.make_pkeys(3);
    std::vector<mutation> mutations;
    for (const auto& pk : keys) {
        mutation m(s.schema(), pk);
        for (int j = 0; j < 3; ++j) {
            s.add_row(m, s.make_ckey(j), "example_value");
        }
        mutations.push_back(std::move(m));
    }

    // Test all possible preemption points.
    for (uint64_t preempt_after = 1; true; ++preempt_after) {
        testlog.trace("preempt after {}", preempt_after);
        auto preempt_src = custom_preemption_source{std::make_unique<preempter>(preempt_after)};
        auto& p = dynamic_cast<preempter&>(*preempt_src._impl);

        // Set up the cache and populate it with the second mutation,
        // so that the update can be preempted in the middle of a partition.
        // It's a condition for reproducing #16759.
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());
        underlying.apply(mutations[1]);
        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);
        cache.populate(mutations[1]);

        tests::reader_concurrency_semaphore_wrapper semaphore;

        // Update the cache (and the underlying source) with the mutations.
        auto mt = make_lw_shared<replica::memtable>(s.schema());
        for (const auto& m : mutations) {
            mt->apply(m);
        }
        auto mt_copy = make_lw_shared<replica::memtable>(s.schema());
        mt_copy->apply(*mt, semaphore.make_permit()).get();
        auto update_fut = cache.update(row_cache::external_updater([&] { underlying.apply(mt_copy); }), *mt, preempt_src).then([&] {
            // The update does not have to yield.
            // We don't want the test to break if it doesn't yield.
            // So if the test wasn't unblocked by a yield, we have to
            // do it here.
            p.wait_for_block.signal();
        });

        // Wait for the update thread to yield.
        p.wait_for_block.wait().get();

        {
            // Read combined cache and memtables, and check that it produces
            // the inserted data.
            std::vector<mutation_reader> readers;
            readers.push_back(cache.make_reader(s.schema(), semaphore.make_permit()));
            readers.push_back(mt->make_flat_reader(s.schema(), semaphore.make_permit()));
            auto at = assert_that(make_combined_reader(s.schema(), semaphore.make_permit(), std::move(readers)));
            for (const auto& m : mutations) {
                at.produces(m);
            }
            at.produces_end_of_stream();
        }

        // Unblock the update and wait for it to finish.
        p.wait_for_unblock.signal();
        update_fut.get();

        {
            // Read the cache after the update is over and check that it produces the inserted
            // data.
            auto at = assert_that(cache.make_reader(s.schema(), semaphore.make_permit()));
            for (const auto& m : mutations) {
                at.produces(m);
            }
            at.produces_end_of_stream();
        }

        // If a preemption request wasn't triggered in this loop, this means
        // we have tested all preemption points and we are done.
        if (p.until_preempt > 0) {
            break;
        }
    }
}
#endif

// Reproducer for scylladb/scylladb#18045.
SEASTAR_THREAD_TEST_CASE(test_reproduce_18045) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v", int32_type)
        .build();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto make_ck = [&s] (int v) {
        return clustering_key::from_deeply_exploded(*s, {data_value{v}});
    };

    auto pk = tests::generate_partition_key(s);
    auto ck1 = make_ck(1);
    auto ck2 = make_ck(2);
    auto ck3 = make_ck(3);

    // In the blocks below, we set up the following state:
    // 1. Underlying row at ck1, live.
    // 2. Cache entry at ck2, expired, discontinuous.
    // 3. Cache entry at ck3, live, continuous.

    auto dt_exp = gc_clock::now() - std::chrono::seconds(s->gc_grace_seconds().count() + 1);
    mutation m(s, pk);
    m.set_clustered_cell(ck1, "v", data_value(0), 1);
    m.partition().apply_delete(*s, ck2, tombstone(1, dt_exp));
    m.set_clustered_cell(ck3, "v", data_value(0), 1);

    memtable_snapshot_source underlying(s);
    underlying.apply(m);

    cache_tracker tracker;
    row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);
    cache.populate(m);

    with_allocator(tracker.allocator(), [&] {
        auto& e = *cache.lookup(pk).partition().version()->partition().clustered_rows().begin();
        tracker.get_lru().remove(e);
        e.on_evicted(tracker);
    });

    // We have set up the desired state.
    // Now we do a reverse query over the partition.
    // This query will remove the expired entry at ck2, leaving cursor's _latest_it dangling.
    // Then, it will populate the cache with ck3.
    // Before the fix for issue #18045, this caused a (ASAN-triggering) use-after-free,
    // because _latest_it was deferenced during the population.

    tombstone_gc_state gc_state(nullptr);
    auto slice = query::reverse_slice(*s, s->full_slice());
    auto rd = cache.make_reader(
        s->make_reversed(),
        semaphore.make_permit(),
        dht::partition_range::make_singular(pk),
        slice,
        nullptr,
        streamed_mutation::forwarding::no,
        mutation_reader::forwarding::no,
        &gc_state);
    auto close_rd = deferred_close(rd);
    read_mutation_from_mutation_reader(rd).get();
}
