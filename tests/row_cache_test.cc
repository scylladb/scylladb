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

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/mutation_source_test.hh"

#include "schema_builder.hh"
#include "row_cache.hh"
#include "core/thread.hh"
#include "memtable.hh"
#include "partition_slice_builder.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::chrono_literals;

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

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, mutation_source([m] (schema_ptr s, const dht::partition_range&) {
            assert(m.schema() == s);
            return make_reader_returning(m);
        }), tracker);

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
        row_cache cache(s, mutation_source([m] (schema_ptr s, const dht::partition_range&) {
            assert(m.schema() == s);
            return make_reader_returning(m);
        }), tracker);

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
        row_cache cache(s, mutation_source([&secondary_calls_count] (schema_ptr s, const dht::partition_range& range) {
            return make_counting_reader(make_empty_reader(), secondary_calls_count);
        }), tracker);

        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
        assert_that(cache.make_reader(s, query::full_partition_range))
            .produces_end_of_stream();
        BOOST_REQUIRE_EQUAL(secondary_calls_count, 1);
    });
}

void test_cache_delegates_to_underlying_only_once_with_single_partition(schema_ptr s,
                                                                        const mutation& m,
                                                                        const dht::partition_range& range) {
    int secondary_calls_count = 0;
    cache_tracker tracker;
    row_cache cache(s, mutation_source([m, &secondary_calls_count] (schema_ptr s, const dht::partition_range& range) {
        assert(m.schema() == s);
        if (range.contains(dht::ring_position(m.decorated_key()), dht::ring_position_comparator(*s))) {
            return make_counting_reader(make_reader_returning(m), secondary_calls_count);
        } else {
            return make_counting_reader(make_empty_reader(), secondary_calls_count);
        }
    }), tracker);

    assert_that(cache.make_reader(s, range))
        .produces(m)
        .produces_end_of_stream();
    BOOST_REQUIRE_EQUAL(secondary_calls_count, 2);
    assert_that(cache.make_reader(s, range))
        .produces(m)
        .produces_end_of_stream();
    BOOST_REQUIRE_EQUAL(secondary_calls_count, 2);
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_single_key_range) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, m,
            dht::partition_range::make_singular(query::ring_position(m.decorated_key())));
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_full_range) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, m, query::full_partition_range);
    });
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying_only_once_range_open) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);
        dht::partition_range::bound end = {dht::ring_position(m.decorated_key()), true};
        dht::partition_range range = dht::partition_range::make_ending_with(end);
        test_cache_delegates_to_underlying_only_once_with_single_partition(s, m, range);
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
            auto secondary = mutation_source([&mt, &secondary_calls_count] (schema_ptr s, const dht::partition_range& range) {
                return make_counting_reader(mt->as_data_source()(s, range), secondary_calls_count);
            });

            return make_lw_shared<row_cache>(s, secondary, tracker);
        };

        auto make_ds = [&make_cache](schema_ptr s, int& secondary_calls_count) -> mutation_source {
            auto cache = make_cache(s, secondary_calls_count);
            return mutation_source([cache] (schema_ptr s, const dht::partition_range& range) {
                return cache->make_reader(s, range);
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
            auto ds = mutation_source([cache] (schema_ptr s, const dht::partition_range& range) {
                    return cache->make_reader(s, range);
            });

            test(ds, query::full_partition_range, partitions.size() + 1);
            test(ds, query::full_partition_range, partitions.size() + 1);

            cache->invalidate(key_after_all);

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
        row_cache cache(s, mt->as_data_source(), tracker);

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
        row_cache cache(s, mt->as_data_source(), tracker);

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

            auto cache = make_lw_shared<row_cache>(s, mt->as_data_source(), tracker);
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

SEASTAR_TEST_CASE(test_eviction) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), tracker);

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 100000; i++) {
            auto m = make_new_mutation(s);
            keys.emplace_back(m.decorated_key());
            cache.populate(m);
        }

        std::random_shuffle(keys.begin(), keys.end());

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
    return bool(mo);
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

SEASTAR_TEST_CASE(test_update) {
    return seastar::async([] {
        auto s = make_schema();
        auto cache_mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, cache_mt->as_data_source(), tracker);

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

        cache.update(*mt, [] (auto&& key) {
            return partition_presence_checker_result::definitely_doesnt_exist;
        }).get();

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
        }

        cache.update(*mt2, [] (auto&& key) {
            return partition_presence_checker_result::maybe_exists;
        }).get();

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

        cache.update(*mt3, [] (auto&& key) {
            return partition_presence_checker_result::maybe_exists;
        }).get();

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
        row_cache cache(s, cache_mt->as_data_source(), tracker);

        int partition_count = 1000;

        // populate cache with some partitions
        for (int i = 0; i < partition_count / 2; i++) {
            auto m = make_new_mutation(s, i + partition_count / 2);
            cache.populate(m);
        }

        // populate memtable with more updated partitions
        auto mt = make_lw_shared<memtable>(s);
        using partitions_type = std::map<partition_key, mutation_partition, partition_key::less_compare>;
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
        tracker.region().make_evictable([ev, evicitons_left = int(10)] () mutable {
            if (evicitons_left == 0) {
                return memory::reclaiming_result::reclaimed_nothing;
            }
            --evicitons_left;
            return ev();
        });

        try {
            cache.update(*mt, [] (auto&& key) {
                return partition_presence_checker_result::definitely_doesnt_exist;
            }).get();
            BOOST_FAIL("updating cache should have failed");
        } catch (const std::bad_alloc&) {
            // expected
        }

        memory_hog.clear();

        // verify that there are no stale partitions
        auto reader = cache.make_reader(s, query::full_partition_range);
        for (int i = 0; i < partition_count; i++) {
            auto mopt = mutation_from_streamed_mutation(reader().get0()).get0();
            if (!mopt) {
                break;
            }
            auto it = updated_partitions.find(mopt->key());
            BOOST_REQUIRE(it != updated_partitions.end());
            BOOST_REQUIRE(it->second.equal(*s, mopt->partition()));
        }
        BOOST_REQUIRE(!reader().get0());
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
        ::throttle _throttle;
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
        impl(mutation_source underlying)
            : _underlying(std::move(underlying))
        { }

        mutation_reader make_reader(schema_ptr s, const dht::partition_range& pr) {
            return make_mutation_reader<reader>(_throttle, _underlying(s, pr));
        }

        ::throttle& throttle() { return _throttle; }
    };
    lw_shared_ptr<impl> _impl;
public:
    throttled_mutation_source(mutation_source underlying)
        : _impl(make_lw_shared<impl>(std::move(underlying)))
    { }

    void block() {
        _impl->throttle().block();
    }

    void unblock() {
        _impl->throttle().unblock();
    }

    operator mutation_source() const {
        return mutation_source([this] (schema_ptr s, const dht::partition_range& pr) {
            return _impl->make_reader(std::move(s), pr);
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

static mutation_source make_mutation_source(std::vector<lw_shared_ptr<memtable>>& memtables) {
    return mutation_source([&memtables] (schema_ptr s, const dht::partition_range& pr) {
        std::vector<mutation_reader> readers;
        for (auto&& mt : memtables) {
            readers.emplace_back(mt->make_reader(s, pr));
        }
        return make_combined_reader(std::move(readers));
    });
}

SEASTAR_TEST_CASE(test_continuity_flag_and_invalidate_race) {
    return seastar::async([] {
        auto s = make_schema();
        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), tracker);

        auto ring = make_ring(s, 4);
        for (auto&& m : ring) {
            mt->apply(m);
        }

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
        cache.invalidate(dht::partition_range::make_starting_with({ ring[2].ring_position(), true })).get();

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
        cache.clear().get();

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
        std::vector<lw_shared_ptr<memtable>> memtables;
        throttled_mutation_source cache_source(make_mutation_source(memtables));
        cache_tracker tracker;
        row_cache cache(s, cache_source, tracker);

        auto mt1 = make_lw_shared<memtable>(s);
        memtables.push_back(mt1);
        auto ring = make_ring(s, 3);
        for (auto&& m : ring) {
            mt1->apply(m);
        }

        auto mt2 = make_lw_shared<memtable>(s);
        auto ring2 = updated_ring(ring);
        for (auto&& m : ring2) {
            mt2->apply(m);
        }

        cache_source.block();

        auto m0_range = dht::partition_range::make_singular(ring[0].ring_position());
        auto rd1 = cache.make_reader(s, m0_range);
        auto rd1_result = rd1();

        auto rd2 = cache.make_reader(s);
        auto rd2_result = rd2();

        sleep(10ms).get();
        auto mt2_flushed = make_lw_shared<memtable>(s);
        mt2_flushed->apply(*mt2).get();
        memtables.push_back(mt2_flushed);

        // This update should miss on all partitions
        auto update_future = cache.update(*mt2, make_default_partition_presence_checker());

        auto rd3 = cache.make_reader(s);

        // rd2, which is in progress, should not prevent forward progress of update()
        cache_source.unblock();
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
        row_cache cache(s, mt->as_data_source(), tracker);

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
        cache.invalidate(*some_element).get();
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
        cache.invalidate(range).get();
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
        std::vector<lw_shared_ptr<memtable>> memtables;
        throttled_mutation_source cache_source(make_mutation_source(memtables));
        cache_tracker tracker;
        row_cache cache(s, cache_source, tracker);

        auto mt1 = make_lw_shared<memtable>(s);
        memtables.push_back(mt1);
        auto ring = make_ring(s, 3);
        for (auto&& m : ring) {
            mt1->apply(m);
        }

        auto mt2 = make_lw_shared<memtable>(s);
        auto ring2 = updated_ring(ring);
        for (auto&& m : ring2) {
            mt2->apply(m);
        }

        cache_source.block();

        auto rd1 = cache.make_reader(s);
        auto rd1_result = rd1();

        sleep(10ms).get();

        memtables.clear();
        memtables.push_back(mt2);

        // This update should miss on all partitions
        auto cache_cleared = cache.clear();

        auto rd2 = cache.make_reader(s);

        // rd1, which is in progress, should not prevent forward progress of clear()
        cache_source.unblock();
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

            auto mt = make_lw_shared<memtable>(s);
            partition_key::equality eq(*s);

            cache_tracker tracker;
            row_cache cache(s, mt->as_data_source(), tracker);

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
            mt->apply(m2);

            stdx::optional<mutation_reader> mt1_reader_opt;
            stdx::optional<streamed_mutation_opt> mt1_reader_sm_opt;
            if (with_active_memtable_reader) {
                mt1_reader_opt = mt1->make_reader(s);
                mt1_reader_sm_opt = (*mt1_reader_opt)().get0();
                BOOST_REQUIRE(*mt1_reader_sm_opt);
            }

            cache.update(*mt1, make_default_partition_presence_checker()).get();
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

            cache.clear().get0();

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

void test_sliced_read_row_presence(mutation_reader reader, schema_ptr s, const query::partition_slice& ps, std::deque<int> expected)
{
    clustering_key::equality ck_eq(*s);

    auto smopt = reader().get0();
    BOOST_REQUIRE(smopt);
    auto mfopt = (*smopt)().get0();
    while (mfopt) {
        if (mfopt->is_clustering_row()) {
            auto& cr = mfopt->as_clustering_row();
            BOOST_REQUIRE(ck_eq(cr.key(), clustering_key_prefix::from_single_value(*s, int32_type->decompose(expected.front()))));
            expected.pop_front();
        }
        mfopt = (*smopt)().get0();
    }

    BOOST_REQUIRE(!reader().get0());
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
        row_cache cache(s, mt->as_data_source(), tracker);

        auto run_tests = [&] (auto& ps, std::deque<int> expected) {
            cache.clear().get0();

            auto reader = cache.make_reader(s, query::full_partition_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, ps, expected);

            reader = cache.make_reader(s, query::full_partition_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, ps, expected);

            auto dk = dht::global_partitioner().decorate_key(*s, pk);
            auto singular_range = dht::partition_range::make_singular(dk);

            reader = cache.make_reader(s, singular_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, ps, expected);

            cache.clear().get0();

            reader = cache.make_reader(s, singular_range, ps);
            test_sliced_read_row_presence(std::move(reader), s, ps, expected);
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
        row_cache cache(s, cache_mt->as_data_source(), tracker);

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
