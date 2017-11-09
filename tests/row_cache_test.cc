/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <boost/test/unit_test.hpp>
#include <seastar/core/sleep.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/mutation_source_test.hh"

#include "schema_builder.hh"
#include "simple_schema.hh"
#include "row_cache.hh"
#include "core/thread.hh"
#include "memtable.hh"
#include "partition_slice_builder.hh"
#include "tests/memtable_snapshot_source.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::chrono_literals;

static seastar::logger test_log("test");

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

static thread_local api::timestamp_type next_timestamp = 1;

static
mutation make_new_mutation(schema_ptr s, partition_key key) {
    mutation m(key, s);
    static thread_local int next_value = 1;
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes(sprint("v%d", next_value++))), next_timestamp++);
    return m;
}

static inline
mutation make_new_large_mutation(schema_ptr s, partition_key key) {
    mutation m(key, s);
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

static
partition_key new_key(schema_ptr s) {
    static thread_local int next = 0;
    return partition_key::from_single_value(*s, to_bytes(sprint("key%d", next++)));
}

static
mutation make_new_mutation(schema_ptr s) {
    return make_new_mutation(s, new_key(s));
}

static inline
mutation make_new_large_mutation(schema_ptr s, int key) {
    return make_new_large_mutation(s, partition_key::from_single_value(*s, to_bytes(sprint("key%d", key))));
}

static inline
mutation make_new_mutation(schema_ptr s, int key) {
    return make_new_mutation(s, partition_key::from_single_value(*s, to_bytes(sprint("key%d", key))));
}

snapshot_source make_decorated_snapshot_source(snapshot_source src, std::function<mutation_source(mutation_source)> decorator) {
    return snapshot_source([src = std::move(src), decorator = std::move(decorator)] () mutable {
        return decorator(src());
    });
}

mutation_source make_source_with(mutation m) {
    return mutation_source([m] (schema_ptr s, const dht::partition_range&, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
        assert(m.schema() == s);
        return make_reader_returning(m, std::move(fwd));
    });
}

// It is assumed that src won't change.
snapshot_source snapshot_source_from_snapshot(mutation_source src) {
    return snapshot_source([src = std::move(src)] {
        return src;
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(make_source_with(m)), tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_works_after_clearing) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(make_source_with(m)), tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();

        tracker.clear();

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

class partition_counting_reader final : public mutation_reader::impl {
    mutation_reader _reader;
    int& _counter;
public:
    partition_counting_reader(mutation_reader mr, int& counter)
        : _reader(std::move(mr)), _counter(counter) { }

    virtual future<streamed_mutation_opt> operator()() override {
        _counter++;
        return _reader();
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        return _reader.fast_forward_to(pr);
    }
};

mutation_reader make_counting_reader(mutation_reader mr, int& counter) {
    return make_mutation_reader<partition_counting_reader>(std::move(mr), counter);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_empty_full_range) {
    return seastar::async([] {
        auto s = make_schema();
        int secondary_calls_count = 0;
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mutation_source([&secondary_calls_count] (schema_ptr s, const dht::partition_range& range, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
            return make_counting_reader(make_empty_reader(), secondary_calls_count);
        })), tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

dht::partition_range make_single_partition_range(schema_ptr& s, int pkey) {
    auto pk = partition_key::from_exploded(*s, { int32_type->decompose(pkey) });
    auto dk = dht::global_partitioner().decorate_key(*s, pk);
    return dht::partition_range::make_singular(dk);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_empty_single_partition_query) {
    return seastar::async([] {
        auto s = make_schema();
        int secondary_calls_count = 0;
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mutation_source([&secondary_calls_count] (schema_ptr s, const dht::partition_range& range, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
            return make_counting_reader(make_empty_reader(), secondary_calls_count);
        })), tracker);
        auto range = make_single_partition_range(s, 100);
        assert_that(cache.make_reader(s, range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
        assert_that(cache.make_reader(s, range))
            .produces_eos_or_empty_mutation();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

SEASTAR_TEST_CASE(test_cache_uses_continuity_info_for_single_partition_query) {
    return seastar::async([] {
        auto s = make_schema();
        int secondary_calls_count = 0;
        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mutation_source([&secondary_calls_count] (schema_ptr s, const dht::partition_range& range, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
            return make_counting_reader(make_empty_reader(), secondary_calls_count);
        })), tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
                .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);

        auto range = make_single_partition_range(s, 100);

        assert_that(cache.make_reader(s, range))
                .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

void test_cache_delegates_to_underlying_only_once_with_single_partition(schema_ptr s,
                                                                        const mutation& m,
                                                                        const dht::partition_range& range,
                                                                        int calls_to_secondary) {
    int secondary_calls_count = 0;
    cache_tracker tracker;
    row_cache cache(s, snapshot_source_from_snapshot(mutation_source([m, &secondary_calls_count] (schema_ptr s, const dht::partition_range& range, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
        assert(m.schema() == s);
        if (range.contains(dht::ring_position(m.decorated_key()), dht::ring_position_comparator(*s))) {
            return make_counting_reader(make_reader_returning(m, std::move(fwd)), secondary_calls_count);
        } else {
            return make_counting_reader(make_empty_reader(), secondary_calls_count);
        }
    })), tracker);

    assert_that(cache.make_reader(s, range))
        .produces(m)
        .produces_end_of_stream();
    BOOST_REQUIRE_EQUAL(secondary_calls_count, calls_to_secondary);
    assert_that(cache.make_reader(s, range))
        .produces(m)
        .produces_end_of_stream();
    BOOST_REQUIRE_EQUAL(secondary_calls_count, calls_to_secondary);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_single_key_range) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, m,
            dht::partition_range::make_singular(query::ring_position(m.decorated_key())), 1);
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_full_range) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, m, query::full_partition_range, 2);
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_range_open) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);
        dht::partition_range::bound end = {dht::ring_position(m.decorated_key()), true};
        dht::partition_range range = dht::partition_range::make_ending_with(end);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, m, range, 2);
    });
}

// partitions must be sorted by decorated key
static void require_no_token_duplicates(const std::vector<mutation>& partitions) {
    std::experimental::optional<dht::token> last_token;
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

        auto make_partition_mutation = [s] (bytes key) -> mutation {
            mutation m(partition_key::from_single_value(*s, key), s);
            m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
            return m;
        };

        int partition_count = 5;

        std::vector<mutation> partitions;
        for (int i = 0; i < partition_count; ++i) {
            partitions.emplace_back(
                make_partition_mutation(to_bytes(sprint("key_%d", i))));
        }

        std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());
        require_no_token_duplicates(partitions);

        dht::decorated_key key_before_all = partitions.front().decorated_key();
        partitions.erase(partitions.begin());

        dht::decorated_key key_after_all = partitions.back().decorated_key();
        partitions.pop_back();

        cache_tracker tracker;
        auto mt = make_lw_shared<memtable>(s);

        for (auto&& m : partitions) {
            mt->apply(m);
        }

        auto make_cache = [&tracker, &mt](schema_ptr s, int& secondary_calls_count) -> lw_shared_ptr<row_cache> {
            auto secondary = mutation_source([&mt, &secondary_calls_count] (schema_ptr s, const dht::partition_range& range,
                    const query::partition_slice& slice, const io_priority_class& pc, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
                return make_counting_reader(mt->as_data_source()(s, range, slice, pc, std::move(trace), std::move(fwd)), secondary_calls_count);
            });

            return make_lw_shared<row_cache>(s, snapshot_source_from_snapshot(secondary), tracker);
        };

        auto make_ds = [&make_cache](schema_ptr s, int& secondary_calls_count) -> mutation_source {
            auto cache = make_cache(s, secondary_calls_count);
            return mutation_source([cache] (schema_ptr s, const dht::partition_range& range,
                    const query::partition_slice& slice, const io_priority_class& pc, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
                return cache->make_reader(s, range, slice, pc, std::move(trace), std::move(fwd));
            });
        };

        auto do_test = [&s, &partitions] (const mutation_source& ds, const dht::partition_range& range,
                                          int& secondary_calls_count, int expected_calls) {
            assert_that(ds(s, range))
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
            assert_that(ds(s, range))
                .produces(slice(partitions, range))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(3, secondary_calls_count);
            assert_that(ds(s, range))
                .produces(slice(partitions, range))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(3, secondary_calls_count);
            auto range2 = dht::partition_range::make(
                {partitions[0].decorated_key(), true},
                {partitions[1].decorated_key(), false});
            assert_that(ds(s, range2))
                .produces(slice(partitions, range2))
                .produces_end_of_stream();
            BOOST_CHECK_EQUAL(3, secondary_calls_count);
            auto range3 = dht::partition_range::make(
                {dht::ring_position::starting_at(key_before_all.token())},
                {partitions[2].decorated_key(), false});
            assert_that(ds(s, range3))
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
            auto ds = mutation_source([cache] (schema_ptr s, const dht::partition_range& range,
                    const query::partition_slice& slice, const io_priority_class& pc, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
                    return cache->make_reader(s, range, slice, pc, std::move(trace), std::move(fwd));
            });

            test(ds, query::full_partition_range, partitions.size() + 1);
            test(ds, query::full_partition_range, partitions.size() + 1);

            cache->invalidate([] {}, key_after_all);

            assert_that(ds(s, query::full_partition_range))
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

        std::vector<mutation> mutations = make_ring(s, 3);

        auto mt = make_lw_shared<memtable>(s);
        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return dht::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        auto key0_range = get_partition_range(mutations[0]);
        auto key2_range = get_partition_range(mutations[2]);

        // Populate cache for first key
        assert_that(cache.make_reader(s, key0_range))
            .produces(mutations[0])
            .produces_end_of_stream();

        // Populate cache for last key
        assert_that(cache.make_reader(s, key2_range))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test single-key queries
        assert_that(cache.make_reader(s, key0_range))
            .produces(mutations[0])
            .produces_end_of_stream();

        assert_that(cache.make_reader(s, key2_range))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test range query
        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces(mutations[0])
            .produces(mutations[1])
            .produces(mutations[2])
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_single_key_queries_after_population_in_reverse_order) {
    return seastar::async([] {
        auto s = make_schema();

        auto mt = make_lw_shared<memtable>(s);

        std::vector<mutation> mutations = make_ring(s, 3);

        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return dht::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        auto key0_range = get_partition_range(mutations[0]);
        auto key1_range = get_partition_range(mutations[1]);
        auto key2_range = get_partition_range(mutations[2]);

        for (int i = 0; i < 2; ++i) {
            assert_that(cache.make_reader(s, key2_range))
                .produces(mutations[2])
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, key1_range))
                .produces(mutations[1])
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, key0_range))
                .produces(mutations[0])
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_row_cache_conforms_to_mutation_source) {
    return seastar::async([] {
        cache_tracker tracker;

        run_mutation_source_tests([&tracker](schema_ptr s, const std::vector<mutation>& mutations) -> mutation_source {
            auto mt = make_lw_shared<memtable>(s);

            for (auto&& m : mutations) {
                mt->apply(m);
            }

            auto cache = make_lw_shared<row_cache>(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);
            return mutation_source([cache] (schema_ptr s,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return cache->make_reader(s, range, slice, pc, std::move(trace_state), fwd, fwd_mr);
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

        auto rd1 = cache.make_reader(gen.schema());
        auto sm1 = rd1().get0();

        // Merge m2 into cache
        auto mt = make_lw_shared<memtable>(gen.schema());
        mt->apply(m2);
        cache.update([&] { underlying.apply(m2); }, *mt).get();

        auto rd2 = cache.make_reader(gen.schema());
        auto sm2 = rd2().get0();

        assert_that(std::move(sm1)).has_mutation().is_equal_to(m1);
        assert_that(std::move(sm2)).has_mutation().is_equal_to(m1 + m2);
    });
}

SEASTAR_TEST_CASE(test_random_partition_population) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);

        auto m1 = make_fully_continuous(gen());
        auto m2 = make_fully_continuous(gen());

        memtable_snapshot_source underlying(gen.schema());
        underlying.apply(m1);

        row_cache cache(gen.schema(), snapshot_source([&] { return underlying(); }), tracker);

        assert_that(cache.make_reader(gen.schema()))
            .produces(m1)
            .produces_end_of_stream();

        cache.invalidate([&] {
            underlying.apply(m2);
        }).get();

        auto pr = dht::partition_range::make_singular(m2.decorated_key());
        assert_that(cache.make_reader(gen.schema(), pr))
            .produces(m1 + m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_eviction) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 100000; i++) {
            auto m = make_new_mutation(s);
            keys.emplace_back(m.decorated_key());
            cache.populate(m);
        }

        std::random_device random;
        std::shuffle(keys.begin(), keys.end(), std::default_random_engine(random()));

        for (auto&& key : keys) {
            cache.make_reader(s, dht::partition_range::make_singular(key));
        }

        while (tracker.partitions() > 0) {
            logalloc::shard_tracker().reclaim(100);
        }
    });
}

bool has_key(row_cache& cache, const dht::decorated_key& key) {
    auto range = dht::partition_range::make_singular(key);
    auto reader = cache.make_reader(cache.schema(), range);
    auto mo = reader().get0();
    if (!bool(mo)) {
        return false;
    }
    auto m = mutation_from_streamed_mutation(*mo).get0();
    return !m.partition().empty();
}

void verify_has(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(has_key(cache, key));
}

void verify_does_not_have(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(!has_key(cache, key));
}

void verify_has(row_cache& cache, const mutation& m) {
    auto range = dht::partition_range::make_singular(m.decorated_key());
    auto reader = cache.make_reader(cache.schema(), range);
    assert_that(reader().get0()).has_mutation().is_equal_to(m);
}

void test_sliced_read_row_presence(mutation_reader reader, schema_ptr s, std::deque<int> expected)
{
    clustering_key::equality ck_eq(*s);

    auto smopt = reader().get0();
    BOOST_REQUIRE(smopt);
    auto mfopt = (*smopt)().get0();
    while (mfopt) {
        if (mfopt->is_clustering_row()) {
            BOOST_REQUIRE(!expected.empty());
            auto expected_ck = expected.front();
            auto ck = clustering_key_prefix::from_single_value(*s, int32_type->decompose(expected_ck));
            expected.pop_front();
            auto& cr = mfopt->as_clustering_row();
            if (!ck_eq(cr.key(), ck)) {
                BOOST_FAIL(sprint("Expected %s, but got %s", ck, cr.key()));
            }
        }
        mfopt = (*smopt)().get0();
    }
    BOOST_REQUIRE(expected.empty());
    BOOST_REQUIRE(!reader().get0());
}

SEASTAR_TEST_CASE(test_single_partition_update) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
        auto pk = partition_key::from_exploded(*s, { int32_type->decompose(100) });
        auto dk = dht::global_partitioner().decorate_key(*s, pk);
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
            mutation m(pk, s);
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
            auto reader = cache.make_reader(s, range, slice);
            test_sliced_read_row_presence(std::move(reader), s, {1, 4, 7});
        }

        auto mt = make_lw_shared<memtable>(s);
        cache.update([&] {
            mutation m(pk, s);
            m.set_clustered_cell(ck3, "v", data_value(101), 1);
            mt->apply(m);
            cache_mt.apply(m);
        }, *mt).get();

        {
            auto reader = cache.make_reader(s, range);
            test_sliced_read_row_presence(std::move(reader), s, {1, 2, 3, 4, 7});
        }

    });
}

SEASTAR_TEST_CASE(test_update) {
    return seastar::async([] {
        auto s = make_schema();
        auto cache_mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker, is_continuous::yes);

        BOOST_TEST_MESSAGE("Check cache miss with populate");

        int partition_count = 1000;

        // populate cache with some partitions
        std::vector<dht::decorated_key> keys_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_in_cache.push_back(m.decorated_key());
            cache.populate(m);
        }

        // populate memtable with partitions not in cache
        auto mt = make_lw_shared<memtable>(s);
        std::vector<dht::decorated_key> keys_not_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_not_in_cache.push_back(m.decorated_key());
            mt->apply(m);
        }

        cache.update([] {}, *mt).get();

        for (auto&& key : keys_not_in_cache) {
            verify_has(cache, key);
        }

        for (auto&& key : keys_in_cache) {
            verify_has(cache, key);
        }

        std::copy(keys_not_in_cache.begin(), keys_not_in_cache.end(), std::back_inserter(keys_in_cache));
        keys_not_in_cache.clear();

        BOOST_TEST_MESSAGE("Check cache miss with drop");

        auto mt2 = make_lw_shared<memtable>(s);

        // populate memtable with partitions not in cache
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_not_in_cache.push_back(m.decorated_key());
            mt2->apply(m);
            cache.invalidate([] {}, m.decorated_key()).get();
        }

        cache.update([] {}, *mt2).get();

        for (auto&& key : keys_not_in_cache) {
            verify_does_not_have(cache, key);
        }

        BOOST_TEST_MESSAGE("Check cache hit with merge");

        auto mt3 = make_lw_shared<memtable>(s);

        std::vector<mutation> new_mutations;
        for (auto&& key : keys_in_cache) {
            auto m = make_new_mutation(s, key.key());
            new_mutations.push_back(m);
            mt3->apply(m);
        }

        cache.update([] {}, *mt3).get();

        for (auto&& m : new_mutations) {
            verify_has(cache, m);
        }
    });
}

#ifndef DEFAULT_ALLOCATOR
SEASTAR_TEST_CASE(test_update_failure) {
    return seastar::async([] {
        auto s = make_schema();
        auto cache_mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker, is_continuous::yes);

        int partition_count = 1000;

        // populate cache with some partitions
        using partitions_type = std::map<partition_key, mutation_partition, partition_key::less_compare>;
        auto original_partitions = partitions_type(partition_key::less_compare(*s));
        for (int i = 0; i < partition_count / 2; i++) {
            auto m = make_new_mutation(s, i + partition_count / 2);
            original_partitions.emplace(m.key(), m.partition());
            cache.populate(m);
        }

        // populate memtable with more updated partitions
        auto mt = make_lw_shared<memtable>(s);
        auto updated_partitions = partitions_type(partition_key::less_compare(*s));
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_large_mutation(s, i);
            updated_partitions.emplace(m.key(), m.partition());
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
            cache.update([] { }, *mt).get();
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        BOOST_REQUIRE(!evicitons_left); // should have happened

        memory_hog.clear();

        auto has_only = [&] (const partitions_type& partitions) {
            auto reader = cache.make_reader(s, query::full_partition_range);
            for (int i = 0; i < partition_count; i++) {
                auto mopt = mutation_from_streamed_mutation(reader().get0()).get0();
                if (!mopt) {
                    break;
                }
                auto it = partitions.find(mopt->key());
                BOOST_REQUIRE(it != partitions.end());
                BOOST_REQUIRE(it->second.equal(*s, mopt->partition()));
            }
            BOOST_REQUIRE(!reader().get0());
        };

        if (failed) {
            has_only(original_partitions);
        } else {
            has_only(updated_partitions);
        }
    });
}
#endif

class throttle {
    unsigned _block_counter = 0;
    promise<> _p; // valid when _block_counter != 0, resolves when goes down to 0
public:
    future<> enter() {
        if (_block_counter) {
            promise<> p1;
            promise<> p2;

            auto f1 = p1.get_future();

            p2.get_future().then([p1 = std::move(p1), p3 = std::move(_p)] () mutable {
                p1.set_value();
                p3.set_value();
            });
            _p = std::move(p2);

            return f1;
        } else {
            return make_ready_future<>();
        }
    }

    void block() {
        ++_block_counter;
        _p = promise<>();
    }

    void unblock() {
        assert(_block_counter);
        if (--_block_counter == 0) {
            _p.set_value();
        }
    }
};

class throttled_mutation_source {
private:
    class impl : public enable_lw_shared_from_this<impl> {
        mutation_source _underlying;
        ::throttle& _throttle;
    private:
        class reader : public mutation_reader::impl {
            throttle& _throttle;
            mutation_reader _reader;
        public:
            reader(throttle& t, mutation_reader r)
                    : _throttle(t)
                    , _reader(std::move(r))
            {}

            virtual future<streamed_mutation_opt> operator()() override {
                return _reader().finally([this] () {
                    return _throttle.enter();
                });
            }

            virtual future<> fast_forward_to(const dht::partition_range& pr) override {
                return _reader.fast_forward_to(pr);
            }
        };
    public:
        impl(::throttle& t, mutation_source underlying)
            : _underlying(std::move(underlying))
            , _throttle(t)
        { }

        mutation_reader make_reader(schema_ptr s, const dht::partition_range& pr,
                const query::partition_slice& slice, const io_priority_class& pc, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
            return make_mutation_reader<reader>(_throttle, _underlying(s, pr, slice, pc, std::move(trace), std::move(fwd)));
        }
    };
    lw_shared_ptr<impl> _impl;
public:
    throttled_mutation_source(throttle& t, mutation_source underlying)
        : _impl(make_lw_shared<impl>(t, std::move(underlying)))
    { }

    operator mutation_source() const {
        return mutation_source([impl = _impl] (schema_ptr s, const dht::partition_range& pr,
                const query::partition_slice& slice, const io_priority_class& pc, tracing::trace_state_ptr trace, streamed_mutation::forwarding fwd) {
            return impl->make_reader(std::move(s), pr, slice, pc, std::move(trace), std::move(fwd));
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
        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        auto ring = make_ring(s, 4);
        for (auto&& m : ring) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        // Bring ring[2]and ring[3] to cache.
        auto range = dht::partition_range::make_starting_with({ ring[2].ring_position(), true });
        assert_that(cache.make_reader(s, range))
                .produces(ring[2])
                .produces(ring[3])
                .produces_end_of_stream();

        // Start reader with full range.
        auto rd = assert_that(cache.make_reader(s, query::full_partition_range));
        rd.produces(ring[0]);

        // Invalidate ring[2] and ring[3]
        cache.invalidate([] {}, dht::partition_range::make_starting_with({ ring[2].ring_position(), true })).get();

        // Continue previous reader.
        rd.produces(ring[1])
          .produces(ring[2])
          .produces(ring[3])
          .produces_end_of_stream();

        // Start another reader with full range.
        rd = assert_that(cache.make_reader(s, query::full_partition_range));
        rd.produces(ring[0])
          .produces(ring[1])
          .produces(ring[2]);

        // Invalidate whole cache.
        cache.invalidate([] {}).get();

        rd.produces(ring[3])
          .produces_end_of_stream();

        // Start yet another reader with full range.
        assert_that(cache.make_reader(s, query::full_partition_range))
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
        memtable_snapshot_source memtables(s);
        throttle thr;
        auto cache_source = make_decorated_snapshot_source(snapshot_source([&] { return memtables(); }), [&] (mutation_source src) {
            return throttled_mutation_source(thr, std::move(src));
        });
        cache_tracker tracker;

        auto mt1 = make_lw_shared<memtable>(s);
        auto ring = make_ring(s, 3);
        for (auto&& m : ring) {
            mt1->apply(m);
        }
        memtables.apply(*mt1);

        row_cache cache(s, cache_source, tracker);

        auto mt2 = make_lw_shared<memtable>(s);
        auto ring2 = updated_ring(ring);
        for (auto&& m : ring2) {
            mt2->apply(m);
        }

        thr.block();

        auto m0_range = dht::partition_range::make_singular(ring[0].ring_position());
        auto rd1 = cache.make_reader(s, m0_range);
        auto rd1_result = rd1();

        auto rd2 = cache.make_reader(s);
        auto rd2_result = rd2();

        sleep(10ms).get();

        // This update should miss on all partitions
        auto mt2_copy = make_lw_shared<memtable>(s);
        mt2_copy->apply(*mt2).get();
        auto update_future = cache.update([&] { memtables.apply(mt2_copy); }, *mt2);

        auto rd3 = cache.make_reader(s);

        // rd2, which is in progress, should not prevent forward progress of update()
        thr.unblock();
        update_future.get();

        // Reads started before memtable flush should return previous value, otherwise this test
        // doesn't trigger the conditions it is supposed to protect against.
        assert_that(rd1_result.get0()).has_mutation().is_equal_to(ring[0]);

        assert_that(rd2_result.get0()).has_mutation().is_equal_to(ring[0]);
        assert_that(rd2().get0()).has_mutation().is_equal_to(ring2[1]);
        assert_that(rd2().get0()).has_mutation().is_equal_to(ring2[2]);
        assert_that(rd2().get0()).has_no_mutation();

        // Reads started after update was started but before previous populations completed
        // should already see the new data
        assert_that(std::move(rd3))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();

        // Reads started after flush should see new data
        assert_that(cache.make_reader(s))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_invalidate) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

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
        cache.invalidate([] {}, *some_element).get();
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
        cache.invalidate([] {}, range).get();
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
        memtable_snapshot_source memtables(s);
        throttle thr;
        auto cache_source = make_decorated_snapshot_source(snapshot_source([&] { return memtables(); }), [&] (mutation_source src) {
            return throttled_mutation_source(thr, std::move(src));
        });
        cache_tracker tracker;

        auto mt1 = make_lw_shared<memtable>(s);
        auto ring = make_ring(s, 3);
        for (auto&& m : ring) {
            mt1->apply(m);
        }
        memtables.apply(*mt1);

        row_cache cache(s, std::move(cache_source), tracker);

        auto mt2 = make_lw_shared<memtable>(s);
        auto ring2 = updated_ring(ring);
        for (auto&& m : ring2) {
            mt2->apply(m);
        }

        thr.block();

        auto rd1 = cache.make_reader(s);
        auto rd1_result = rd1();

        sleep(10ms).get();

        // This update should miss on all partitions
        auto cache_cleared = cache.invalidate([&] {
            memtables.apply(mt2);
        });

        auto rd2 = cache.make_reader(s);

        // rd1, which is in progress, should not prevent forward progress of clear()
        thr.unblock();
        cache_cleared.get();

        // Reads started before memtable flush should return previous value, otherwise this test
        // doesn't trigger the conditions it is supposed to protect against.

        assert_that(rd1_result.get0()).has_mutation().is_equal_to(ring[0]);
        assert_that(rd1().get0()).has_mutation().is_equal_to(ring2[1]);
        assert_that(rd1().get0()).has_mutation().is_equal_to(ring2[2]);
        assert_that(rd1().get0()).has_no_mutation();

        // Reads started after clear but before previous populations completed
        // should already see the new data
        assert_that(std::move(rd2))
                .produces(ring2[0])
                .produces(ring2[1])
                .produces(ring2[2])
                .produces_end_of_stream();

        // Reads started after clear should see new data
        assert_that(cache.make_reader(s))
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

            memtable_snapshot_source underlying(s);
            partition_key::equality eq(*s);

            underlying.apply(m1);

            cache_tracker tracker;
            row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

            auto pk = m1.key();
            cache.populate(m1);

            auto sm1 = cache.make_reader(s)().get0();
            BOOST_REQUIRE(sm1);
            BOOST_REQUIRE(eq(sm1->key(), pk));

            auto sm2 = cache.make_reader(s)().get0();
            BOOST_REQUIRE(sm2);
            BOOST_REQUIRE(eq(sm2->key(), pk));

            auto mt1 = make_lw_shared<memtable>(s);
            mt1->apply(m2);

            auto m12 = m1 + m2;

            stdx::optional<mutation_reader> mt1_reader_opt;
            stdx::optional<streamed_mutation_opt> mt1_reader_sm_opt;
            if (with_active_memtable_reader) {
                mt1_reader_opt = mt1->make_reader(s);
                mt1_reader_sm_opt = (*mt1_reader_opt)().get0();
                BOOST_REQUIRE(*mt1_reader_sm_opt);
            }

            auto mt1_copy = make_lw_shared<memtable>(s);
            mt1_copy->apply(*mt1).get();
            cache.update([&] { underlying.apply(mt1_copy); }, *mt1).get();

            auto sm3 = cache.make_reader(s)().get0();
            BOOST_REQUIRE(sm3);
            BOOST_REQUIRE(eq(sm3->key(), pk));

            auto sm4 = cache.make_reader(s)().get0();
            BOOST_REQUIRE(sm4);
            BOOST_REQUIRE(eq(sm4->key(), pk));

            auto sm5 = cache.make_reader(s)().get0();
            BOOST_REQUIRE(sm5);
            BOOST_REQUIRE(eq(sm5->key(), pk));

            assert_that_stream(std::move(*sm3)).has_monotonic_positions();

            if (with_active_memtable_reader) {
                assert(mt1_reader_sm_opt);
                auto mt1_reader_mutation = mutation_from_streamed_mutation(std::move(*mt1_reader_sm_opt)).get0();
                BOOST_REQUIRE(mt1_reader_mutation);
                assert_that(*mt1_reader_mutation).is_equal_to(m2);
            }

            auto m_4 = mutation_from_streamed_mutation(std::move(sm4)).get0();
            assert_that(*m_4).is_equal_to(m12);

            auto m_1 = mutation_from_streamed_mutation(std::move(sm1)).get0();
            assert_that(*m_1).is_equal_to(m1);

            cache.invalidate([] {}).get0();

            auto m_2 = mutation_from_streamed_mutation(std::move(sm2)).get0();
            assert_that(*m_2).is_equal_to(m1);

            auto m_5 = mutation_from_streamed_mutation(std::move(sm5)).get0();
            assert_that(*m_5).is_equal_to(m12);
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

            auto m2 = mutation(m1.decorated_key(), m1.schema());
            m2.partition().apply(*s, m2_.partition(), *s);
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

        auto pk = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        mutation m(pk, s);
        constexpr auto row_count = 8;
        for (auto i = 0; i < row_count; i++) {
            m.set_clustered_cell(clustering_key_prefix::from_single_value(*s, int32_type->decompose(i)),
                                 to_bytes("v"), data_value(i), api::new_timestamp());
        }

        auto mt = make_lw_shared<memtable>(s);
        mt->apply(m);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(mt->as_data_source()), tracker);

        auto run_tests = [&] (auto& ps, std::deque<int> expected) {
            cache.invalidate([] {}).get0();

            auto reader = cache.make_reader(s, query::full_partition_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);

            reader = cache.make_reader(s, query::full_partition_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);

            auto dk = dht::global_partitioner().decorate_key(*s, pk);
            auto singular_range = dht::partition_range::make_singular(dk);

            reader = cache.make_reader(s, singular_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, expected);

            cache.invalidate([] {}).get0();

            reader = cache.make_reader(s, singular_range, ps);
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

SEASTAR_TEST_CASE(test_lru) {
    return seastar::async([] {
        auto s = make_schema();
        auto cache_mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        int partition_count = 10;

        std::vector<mutation> partitions = make_ring(s, partition_count);
        for (auto&& m : partitions) {
            cache.populate(m);
        }

        auto pr = dht::partition_range::make_ending_with(dht::ring_position(partitions[2].decorated_key()));
        auto rd = cache.make_reader(s, pr);
        assert_that(std::move(rd))
                .produces(partitions[0])
                .produces(partitions[1])
                .produces(partitions[2])
                .produces_end_of_stream();

        auto ret = tracker.region().evict_some();
        BOOST_REQUIRE(ret == memory::reclaiming_result::reclaimed_something);

        pr = dht::partition_range::make_ending_with(dht::ring_position(partitions[4].decorated_key()));
        rd = cache.make_reader(s, pr);
        assert_that(std::move(rd))
                .produces(partitions[0])
                .produces(partitions[1])
                .produces(partitions[2])
                .produces(partitions[4])
                .produces_end_of_stream();

        pr = dht::partition_range::make_singular(dht::ring_position(partitions[5].decorated_key()));
        rd = cache.make_reader(s, pr);
        assert_that(std::move(rd))
                .produces(partitions[5])
                .produces_end_of_stream();

        ret = tracker.region().evict_some();
        BOOST_REQUIRE(ret == memory::reclaiming_result::reclaimed_something);

        rd = cache.make_reader(s);
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
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto mutation_for_key = [&] (dht::decorated_key key) {
            mutation m(key, s.schema());
            s.add_row(m, s.make_ckey(0), "val");
            return m;
        };

        auto keys = s.make_pkeys(4);

        auto m1 = mutation_for_key(keys[1]);
        underlying.apply(m1);

        auto m2 = mutation_for_key(keys[3]);
        underlying.apply(m2);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        assert_that(cache.make_reader(s.schema()))
            .produces(m1)
            .produces(m2)
            .produces_end_of_stream();

        auto mt = make_lw_shared<memtable>(s.schema());

        auto m3 = mutation_for_key(m1.decorated_key());
        m3.partition().apply(s.new_tombstone());
        auto m4 = mutation_for_key(keys[2]);
        auto m5 = mutation_for_key(keys[0]);
        mt->apply(m3);
        mt->apply(m4);
        mt->apply(m5);

        auto mt_copy = make_lw_shared<memtable>(s.schema());
        mt_copy->apply(*mt).get();
        cache.update_invalidating([&] { underlying.apply(mt_copy); }, *mt).get();

        assert_that(cache.make_reader(s.schema()))
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
        auto cache_mt = make_lw_shared<memtable>(s.schema());

        auto pkeys = s.make_pkeys(3);

        mutation m1(pkeys[0], s.schema());
        s.add_row(m1, s.make_ckey(0), "v1");
        s.add_row(m1, s.make_ckey(1), "v2");
        s.add_row(m1, s.make_ckey(2), "v3");
        s.add_row(m1, s.make_ckey(3), "v4");
        cache_mt->apply(m1);

        mutation m2(pkeys[1], s.schema());
        s.add_row(m2, s.make_ckey(0), "v5");
        s.add_row(m2, s.make_ckey(1), "v6");
        s.add_row(m2, s.make_ckey(2), "v7");
        cache_mt->apply(m2);

        mutation m3(pkeys[2], s.schema());
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
            assert_that(cache.make_reader(s.schema(), prange, slice))
                .produces(m1, slice.row_ranges(*s.schema(), m1.key()))
                .produces_end_of_stream();
        }

        // partially populate m3
        {
            auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_ending_with(s.make_ckey(1)))
                .build();
            auto prange = dht::partition_range::make_singular(m3.decorated_key());
            assert_that(cache.make_reader(s.schema(), prange, slice))
                .produces(m3, slice.row_ranges(*s.schema(), m3.key()))
                .produces_end_of_stream();
        }

        // full scan
        assert_that(cache.make_reader(s.schema()))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();

        // full scan after full scan
        assert_that(cache.make_reader(s.schema()))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_populates_partition_tombstone) {
    return seastar::async([] {
        simple_schema s;
        auto cache_mt = make_lw_shared<memtable>(s.schema());

        auto pkeys = s.make_pkeys(2);

        mutation m1(pkeys[0], s.schema());
        s.add_static_row(m1, "val");
        m1.partition().apply(tombstone(s.new_timestamp(), gc_clock::now()));
        cache_mt->apply(m1);

        mutation m2(pkeys[1], s.schema());
        s.add_static_row(m2, "val");
        m2.partition().apply(tombstone(s.new_timestamp(), gc_clock::now()));
        cache_mt->apply(m2);

        cache_tracker tracker;
        row_cache cache(s.schema(), snapshot_source_from_snapshot(cache_mt->as_data_source()), tracker);

        // singular range case
        {
            auto prange = dht::partition_range::make_singular(dht::ring_position(m1.decorated_key()));
            assert_that(cache.make_reader(s.schema(), prange))
                .produces(m1)
                .produces_end_of_stream();

            assert_that(cache.make_reader(s.schema(), prange)) // over populated
                .produces(m1)
                .produces_end_of_stream();
        }

        // range scan case
        {
            assert_that(cache.make_reader(s.schema()))
                .produces(m1)
                .produces(m2)
                .produces_end_of_stream();

            assert_that(cache.make_reader(s.schema())) // over populated
                .produces(m1)
                .produces(m2)
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
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        tombstone t0{s.new_timestamp(), gc_clock::now()};
        tombstone t1{s.new_timestamp(), gc_clock::now()};

        mutation m1(pk, s.schema());
        m1.partition().apply_delete(*s.schema(),
            s.make_range_tombstone(query::clustering_range::make(s.make_ckey(0), s.make_ckey(10)), t0));
        underlying.apply(m1);

        mutation m2(pk, s.schema());
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

            assert_that(cache.make_reader(s.schema(), pr, slice))
                .produces(m1 + m2, slice.row_ranges(*s.schema(), pk.key()))
                .produces_end_of_stream();
        }

        {
            auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_starting_with(s.make_ckey(4)))
                .build();

            assert_that(cache.make_reader(s.schema(), pr, slice))
                .produces(m1 + m2, slice.row_ranges(*s.schema(), pk.key()))
                .produces_end_of_stream();

            auto rd = cache.make_reader(s.schema(), pr, slice);
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            assert_that_stream(std::move(*smo)).has_monotonic_positions();
        }
    });
}

static void consume_all(mutation_reader& rd) {
    while (streamed_mutation_opt smo = rd().get0()) {
        auto&& sm = *smo;
        while (sm().get0()) ;
    }
}

static void populate_range(row_cache& cache,
    const dht::partition_range& pr = query::full_partition_range,
    const query::clustering_range& r = query::full_clustering_range)
{
    auto slice = partition_slice_builder(*cache.schema()).with_range(r).build();
    auto rd = cache.make_reader(cache.schema(), pr, slice);
    consume_all(rd);
}

static void apply(row_cache& cache, memtable_snapshot_source& underlying, const mutation& m) {
    auto mt = make_lw_shared<memtable>(m.schema());
    mt->apply(m);
    cache.update([&] { underlying.apply(m); }, *mt).get();
}

SEASTAR_TEST_CASE(test_readers_get_all_data_after_eviction) {
    return seastar::async([] {
        simple_schema table;
        schema_ptr s = table.schema();
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

        auto make_sm = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s, query::full_partition_range, slice);
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return assert_that_stream(std::move(sm));
        };

        auto sm1 = make_sm(s->full_slice());

        apply(m2);

        auto sm2 = make_sm(s->full_slice());

        auto slice_with_key2 = partition_slice_builder(*s)
            .with_range(query::clustering_range::make_singular(table.make_ckey(2)))
            .build();
        auto sm3 = make_sm(slice_with_key2);

        cache.evict();

        sm3.produces_row_with_key(table.make_ckey(2))
            .produces_end_of_stream();

        sm1.produces(m1);
        sm2.produces(m1 + m2);
    });
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
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(pk, s.schema());
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

        auto make_sm = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s.schema(), pr, slice);
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return assert_that_stream(std::move(sm));
        };

        // populate using reader in same snapshot
        {
            populate_range(cache);

            auto slice_after_7 = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_starting_with(s.make_ckey(7)))
                .build();

            auto sma2 = make_sm(slice_after_7);

            auto sma = make_sm(s.schema()->full_slice());
            sma.produces_row_with_key(s.make_ckey(0));
            sma.produces_range_tombstone(rt1);

            cache.evict();

            sma2.produces_row_with_key(s.make_ckey(8));
            sma2.produces_end_of_stream();

            sma.produces_range_tombstone(rt2);
            sma.produces_range_tombstone(rt3);
            sma.produces_row_with_key(s.make_ckey(8));
            sma.produces_end_of_stream();
        }

        // populate using reader created after invalidation
        {
            populate_range(cache);

            auto sma = make_sm(s.schema()->full_slice());
            sma.produces_row_with_key(s.make_ckey(0));
            sma.produces_range_tombstone(rt1);

            mutation m2(pk, s.schema());
            s.add_row(m2, s.make_ckey(7), "v7");

            cache.invalidate([&] {
                underlying.apply(m2);
            }).get();

            populate_range(cache, pr, query::clustering_range::make_starting_with(s.make_ckey(5)));

            sma.produces_range_tombstone(rt2);
            sma.produces_range_tombstone(rt3);
            sma.produces_row_with_key(s.make_ckey(8));
            sma.produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_reads) {
    return seastar::async([] {
        cache_tracker tracker;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        memtable_snapshot_source underlying(s);

        auto mut = make_fully_continuous(gen());
        underlying.apply(mut);

        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);
        auto&& injector = memory::local_failure_injector();

        auto run_queries = [&] {
            auto slice = partition_slice_builder(*s).with_ranges(gen.make_random_ranges(3)).build();
            auto&& ranges = slice.row_ranges(*s, mut.key());
            uint64_t i = 0;
            while (true) {
                try {
                    injector.fail_after(i++);
                    auto rd = cache.make_reader(s, query::full_partition_range, slice);
                    auto smo = rd().get0();
                    BOOST_REQUIRE(smo);
                    auto got = mutation_from_streamed_mutation(*smo).get0();
                    smo = rd().get0();
                    BOOST_REQUIRE(!smo);
                    injector.cancel();

                    assert_that(got).is_equal_to(mut, ranges);
                    assert_that(cache.make_reader(s, query::full_partition_range, slice))
                        .produces(mut, ranges);

                    if (!injector.failed()) {
                        break;
                    }
                } catch (const std::bad_alloc&) {
                    // expected
                }
            }
        };

        auto run_query = [&] {
            auto slice = partition_slice_builder(*s).with_ranges(gen.make_random_ranges(3)).build();
            auto&& ranges = slice.row_ranges(*s, mut.key());
            injector.fail_after(0);
            assert_that(cache.make_reader(s, query::full_partition_range, slice))
                .produces(mut, ranges);
            injector.cancel();
        };

        run_queries();

        injector.run_with_callback([&] {
            if (tracker.region().reclaiming_enabled()) {
                tracker.region().full_compaction();
            }
            injector.fail_after(0);
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
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation mut(pk, s.schema());
        s.add_row(mut, s.make_ckey(6), "v");
        auto rt = s.make_range_tombstone(s.make_ckey_range(3, 4));
        mut.partition().apply_row_tombstone(*s.schema(), rt);
        underlying.apply(mut);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto&& injector = memory::local_failure_injector();

        auto slice = partition_slice_builder(*s.schema())
            .with_range(s.make_ckey_range(0, 1))
            .with_range(s.make_ckey_range(3, 6))
            .build();

        uint64_t i = 0;
        while (true) {
            try {
                cache.evict();
                populate_range(cache, pr, s.make_ckey_range(6, 10));

                injector.fail_after(i++);
                auto rd = cache.make_reader(s.schema(), pr, slice);
                auto smo = rd().get0();
                BOOST_REQUIRE(smo);
                auto got = mutation_from_streamed_mutation(*smo).get0();
                smo = rd().get0();
                BOOST_REQUIRE(!smo);
                injector.cancel();

                assert_that(got).is_equal_to(mut);

                if (!injector.failed()) {
                    break;
                }
            } catch (const std::bad_alloc&) {
                // expected
            }
        }
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_partition_scan) {
    return seastar::async([] {
        simple_schema s;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pkeys = s.make_pkeys(7);
        std::vector<mutation> muts;

        for (auto&& pk : pkeys) {
            mutation mut(pk, s.schema());
            s.add_row(mut, s.make_ckey(1), "v");
            muts.push_back(mut);
            underlying.apply(mut);
        }

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto&& injector = memory::local_failure_injector();

        uint64_t i = 0;
        do {
            try {
                cache.evict();
                populate_range(cache, dht::partition_range::make_singular(pkeys[1]));
                populate_range(cache, dht::partition_range::make({pkeys[3]}, {pkeys[5]}));

                injector.fail_after(i++);
                assert_that(cache.make_reader(s.schema()))
                    .produces(muts)
                    .produces_end_of_stream();
                injector.cancel();
            } catch (const std::bad_alloc&) {
                // expected
            }
        } while (injector.failed());
    });
}

SEASTAR_TEST_CASE(test_concurrent_population_before_latest_version_iterator) {
    return seastar::async([] {
        simple_schema s;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(pk, s.schema());
        s.add_row(m1, s.make_ckey(0), "v");
        s.add_row(m1, s.make_ckey(1), "v");
        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_sm = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s.schema(), pr, slice);
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return assert_that_stream(std::move(sm));
        };

        {
            populate_range(cache, pr, s.make_ckey_range(0, 1));
            auto rd = make_sm(s.schema()->full_slice()); // to keep current version alive

            mutation m2(pk, s.schema());
            s.add_row(m2, s.make_ckey(2), "v");
            s.add_row(m2, s.make_ckey(3), "v");
            s.add_row(m2, s.make_ckey(4), "v");
            apply(cache, underlying, m2);

            auto slice1 = partition_slice_builder(*s.schema())
                .with_range(s.make_ckey_range(0, 5))
                .build();

            auto sma1 = make_sm(slice1);
            sma1.produces_row_with_key(s.make_ckey(0));

            populate_range(cache, pr, s.make_ckey_range(3, 3));

            auto sma2 = make_sm(slice1);

            sma2.produces_row_with_key(s.make_ckey(0));

            populate_range(cache, pr, s.make_ckey_range(2, 3));

            sma2.produces_row_with_key(s.make_ckey(1));
            sma2.produces_row_with_key(s.make_ckey(2));
            sma2.produces_row_with_key(s.make_ckey(3));
            sma2.produces_row_with_key(s.make_ckey(4));
            sma2.produces_end_of_stream();

            sma1.produces_row_with_key(s.make_ckey(1));
            sma1.produces_row_with_key(s.make_ckey(2));
            sma1.produces_row_with_key(s.make_ckey(3));
            sma1.produces_row_with_key(s.make_ckey(4));
            sma1.produces_end_of_stream();
        }

        {
            cache.evict();
            populate_range(cache, pr, s.make_ckey_range(4, 4));

            auto slice1 = partition_slice_builder(*s.schema())
                .with_range(s.make_ckey_range(0, 1))
                .with_range(s.make_ckey_range(3, 3))
                .build();
            auto sma1 = make_sm(slice1);

            sma1.produces_row_with_key(s.make_ckey(0));

            populate_range(cache, pr, s.make_ckey_range(2, 4));

            sma1.produces_row_with_key(s.make_ckey(1));
            sma1.produces_row_with_key(s.make_ckey(3));
            sma1.produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_concurrent_populating_partition_range_reads) {
    return seastar::async([] {
        simple_schema s;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto keys = s.make_pkeys(10);
        std::vector<mutation> muts;

        for (auto&& k : keys) {
            mutation m(k, s.schema());
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
        auto rd1 = assert_that(cache.make_reader(s.schema(), range1));
        rd1.produces(muts[0]);

        auto rd2 = assert_that(cache.make_reader(s.schema(), range2));
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

static void populate_range(row_cache& cache, dht::partition_range& pr, const query::clustering_row_ranges& ranges) {
    for (auto&& r : ranges) {
        populate_range(cache, pr, r);
    }
}

static void check_continuous(row_cache& cache, dht::partition_range& pr, query::clustering_range r = query::full_clustering_range) {
    auto s0 = cache.get_cache_tracker().get_stats();
    populate_range(cache, pr, r);
    auto s1 = cache.get_cache_tracker().get_stats();
    if (s0.reads_with_misses != s1.reads_with_misses) {
        std::cerr << cache << "\n";
        BOOST_FAIL(sprint("Got cache miss while reading range %s", r));
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
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(pk, s.schema());
        s.add_row(m1, s.make_ckey(0), "v0");
        s.add_row(m1, s.make_ckey(2), "v2");
        s.add_row(m1, s.make_ckey(4), "v4");
        s.add_row(m1, s.make_ckey(6), "v6");
        s.add_row(m1, s.make_ckey(8), "v8");
        unsigned max_key = 9;
        underlying.apply(m1);

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_sm = [&] (const query::partition_slice* slice = nullptr) {
            auto rd = cache.make_reader(s.schema(), pr, slice ? *slice : s.schema()->full_slice());
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return std::move(sm);
        };

        std::vector<query::clustering_range> ranges;

        ranges.push_back(query::clustering_range::make_ending_with({s.make_ckey(5)}));
        ranges.push_back(query::clustering_range::make_starting_with({s.make_ckey(5)}));
        for (unsigned i = 0; i <= max_key; ++i) {
            for (unsigned j = i; j <= max_key; ++j) {
                ranges.push_back(query::clustering_range::make({s.make_ckey(i)}, {s.make_ckey(j)}));
            }
        }

        std::random_device rnd;
        std::default_random_engine rng(rnd());
        std::shuffle(ranges.begin(), ranges.end(), rng);

        struct read {
            std::unique_ptr<query::partition_slice> slice;
            streamed_mutation sm;
            mutation result;
        };

        std::vector<read> readers;
        for (auto&& r : ranges) {
            auto slice = std::make_unique<query::partition_slice>(partition_slice_builder(*s.schema()).with_range(r).build());
            auto sm = make_sm(slice.get());
            readers.push_back(read{std::move(slice), std::move(sm), mutation(pk, s.schema())});
        }

        while (!readers.empty()) {
            auto i = readers.begin();
            while (i != readers.end()) {
                auto mfo = i->sm().get0();
                if (!mfo) {
                    auto&& ranges = i->slice->row_ranges(*s.schema(), pk.key());
                    assert_that(i->result).is_equal_to(m1, ranges);
                    i = readers.erase(i);
                } else {
                    i->result.apply(*mfo);
                    ++i;
                }
            }
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
            BOOST_TEST_MESSAGE(sprint("Reading {}", ranges));

            populate_range(cache, pr, ranges);
            check_continuous(cache, pr, ranges);
            auto s1 = tracker.get_stats();
            populate_range(cache, pr, ranges);
            auto s2 = tracker.get_stats();

            if (s1.reads_with_misses != s2.reads_with_misses) {
                BOOST_FAIL(sprint("Got cache miss when repeating read of %s on %s", ranges, m1));
            }
        }
    });
}

SEASTAR_TEST_CASE(test_continuity_is_populated_when_read_overlaps_with_older_version) {
    return seastar::async([] {
        simple_schema s;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(pk, s.schema());
        s.add_row(m1, s.make_ckey(2), "v2");
        s.add_row(m1, s.make_ckey(4), "v4");
        underlying.apply(m1);

        mutation m2(pk, s.schema());
        s.add_row(m2, s.make_ckey(6), "v6");
        s.add_row(m2, s.make_ckey(8), "v8");

        mutation m3(pk, s.schema());
        s.add_row(m3, s.make_ckey(10), "v");
        s.add_row(m3, s.make_ckey(12), "v");

        mutation m4(pk, s.schema());
        s.add_row(m4, s.make_ckey(14), "v");

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto apply = [&] (mutation m) {
            auto mt = make_lw_shared<memtable>(m.schema());
            mt->apply(m);
            cache.update([&] { underlying.apply(m); }, *mt).get();
        };

        auto make_sm = [&] {
            auto rd = cache.make_reader(s.schema(), pr);
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return std::move(sm);
        };

        {
            auto sm1 = make_sm(); // to keep the old version around

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

            assert_that(cache.make_reader(s.schema(), pr))
                .produces(m1 + m2)
                .produces_end_of_stream();
        }

        cache.evict();

        {
            populate_range(cache, pr, s.make_ckey_range(2, 2));
            populate_range(cache, pr, s.make_ckey_range(5, 5));
            populate_range(cache, pr, s.make_ckey_range(8, 8));

            auto sm1 = make_sm(); // to keep the old version around

            apply(m3);

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that(cache.make_reader(s.schema(), pr))
                .produces(m1 + m2 + m3)
                .produces_end_of_stream();
        }

        cache.evict();

        { // singular range case
            populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(4)));
            populate_range(cache, pr, query::clustering_range::make_singular(s.make_ckey(7)));

            auto sm1 = make_sm(); // to keep the old version around

            apply(m4);

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that(cache.make_reader(s.schema(), pr))
                .produces_compacted(m1 + m2 + m3 + m4)
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

        cache_tracker tracker;
        memtable_snapshot_source underlying(s);

        auto pk = dht::global_partitioner().decorate_key(*s,
            partition_key::from_single_value(*s, data_value(3).serialize()));
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

        mutation m34(pk, s);
        m34.partition().clustered_row(*s, ck3).apply(new_tombstone());
        m34.partition().clustered_row(*s, ck4).apply(new_tombstone());

        mutation m1(pk, s);
        m1.partition().clustered_row(*s, ck2).apply(new_tombstone());
        m1.apply(m34);
        underlying.apply(m1);

        mutation m2(pk, s);
        m2.partition().clustered_row(*s, ck6).apply(new_tombstone());

        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto apply = [&] (mutation m) {
            auto mt = make_lw_shared<memtable>(m.schema());
            mt->apply(m);
            cache.update([&] { underlying.apply(m); }, *mt).get();
        };

        auto make_sm = [&] (const query::partition_slice* slice = nullptr) {
            auto rd = cache.make_reader(s, pr, slice ? *slice : s->full_slice());
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return std::move(sm);
        };

        {
            auto range_3_4 = query::clustering_range::make_singular(ck_3_4);
            populate_range(cache, pr, range_3_4);
            check_continuous(cache, pr, range_3_4);

            auto slice1 = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_singular(ck2))
                .build();
            auto sm1 = make_sm(&slice1);

            apply(m2);

            populate_range(cache, pr, query::full_clustering_range);
            check_continuous(cache, pr, query::full_clustering_range);

            assert_that_stream(std::move(sm1))
                .produces_row_with_key(ck2)
                .produces_end_of_stream();

            assert_that(cache.make_reader(s, pr))
                .produces_compacted(m1 + m2)
                .produces_end_of_stream();

            auto slice34 = partition_slice_builder(*s)
                .with_range(range_3_4)
                .build();
            assert_that(cache.make_reader(s, pr, slice34))
                .produces_compacted(m34)
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(test_continuity_is_populated_for_single_row_reads) {
    return seastar::async([] {
        simple_schema s;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(pk, s.schema());
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

        assert_that(cache.make_reader(s.schema()))
            .produces_compacted(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_concurrent_setting_of_continuity_on_read_upper_bound) {
    return seastar::async([] {
        simple_schema s;
        cache_tracker tracker;
        memtable_snapshot_source underlying(s.schema());

        auto pk = s.make_pkey(0);
        auto pr = dht::partition_range::make_singular(pk);

        mutation m1(pk, s.schema());
        s.add_row(m1, s.make_ckey(0), "v1");
        s.add_row(m1, s.make_ckey(1), "v1");
        s.add_row(m1, s.make_ckey(2), "v1");
        s.add_row(m1, s.make_ckey(3), "v1");
        underlying.apply(m1);

        mutation m2(pk, s.schema());
        s.add_row(m2, s.make_ckey(4), "v2");

        row_cache cache(s.schema(), snapshot_source([&] { return underlying(); }), tracker);

        auto make_sm = [&] (const query::partition_slice* slice = nullptr) {
            auto rd = cache.make_reader(s.schema(), pr, slice ? *slice : s.schema()->full_slice());
            auto smo = rd().get0();
            BOOST_REQUIRE(smo);
            streamed_mutation& sm = *smo;
            sm.set_max_buffer_size(1);
            return std::move(sm);
        };

        {
            auto sm1 = make_sm(); // to keep the old version around

            populate_range(cache, pr, s.make_ckey_range(0, 0));
            populate_range(cache, pr, s.make_ckey_range(3, 3));

            apply(cache, underlying, m2);

            auto slice1 = partition_slice_builder(*s.schema())
                .with_range(s.make_ckey_range(0, 4))
                .build();
            auto sm2 = assert_that_stream(make_sm(&slice1));

            sm2.produces_row_with_key(s.make_ckey(0));
            sm2.produces_row_with_key(s.make_ckey(1));

            populate_range(cache, pr, s.make_ckey_range(2, 4));

            sm2.produces_row_with_key(s.make_ckey(2));
            sm2.produces_row_with_key(s.make_ckey(3));
            sm2.produces_row_with_key(s.make_ckey(4));
            sm2.produces_end_of_stream();

            // FIXME: [1, 2] will not be continuous due to concurrent population.
            // check_continuous(cache, pr, s.make_ckey_range(0, 4));

            assert_that(cache.make_reader(s.schema(), pr))
                .produces(m1 + m2)
                .produces_end_of_stream();
        }
    });
}
