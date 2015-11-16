/*
 * Copyright 2015 Cloudius Systems
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

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/mutation_source_test.hh"

#include "schema_builder.hh"
#include "row_cache.hh"
#include "core/thread.hh"
#include "memtable.hh"

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

static
mutation make_new_mutation(schema_ptr s, partition_key key) {
    mutation m(key, s);
    static thread_local int next_value = 1;
    static thread_local api::timestamp_type next_timestamp = 1;
    m.set_clustered_cell(clustering_key::make_empty(*s), "v", data_value(to_bytes(sprint("v%d", next_value++))), next_timestamp++);
    return m;
}

static
mutation make_key_mutation(schema_ptr s, bytes key) {
    return make_new_mutation(s, partition_key::from_single_value(*s, key));
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

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, [m] (const query::partition_range&) {
            return make_reader_returning(m);
        }, [m] (auto&&) {
            return make_key_from_mutation_reader(make_reader_returning(m));
        }, tracker);

        assert_that(cache.make_reader(query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_works_after_clearing) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_new_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, [m] (const query::partition_range&) {
            return make_reader_returning(m);
        }, [m] (auto&&) {
            return make_key_from_mutation_reader(make_reader_returning(m));
        }, tracker);

        assert_that(cache.make_reader(query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();

        tracker.clear();

        assert_that(cache.make_reader(query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

// Less-comparator on partition_key yielding the ring order.
struct decorated_key_order {
    schema_ptr s;
    bool operator() (partition_key& k1, partition_key& k2) const {
        return dht::global_partitioner().decorate_key(*s, k1)
            .less_compare(*s, dht::global_partitioner().decorate_key(*s, k2));
    }
};

SEASTAR_TEST_CASE(test_query_of_incomplete_range_goes_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();

        std::vector<mutation> mutations = {
            make_key_mutation(s, "key1"),
            make_key_mutation(s, "key2"),
            make_key_mutation(s, "key3")
        };

        std::sort(mutations.begin(), mutations.end(), mutation_decorated_key_less_comparator());

        auto mt = make_lw_shared<memtable>(s);

        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return query::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        // Populate cache for first key
        assert_that(cache.make_reader(get_partition_range(mutations[0])))
            .produces(mutations[0])
            .produces_end_of_stream();

        // Populate cache for last key
        assert_that(cache.make_reader(get_partition_range(mutations[2])))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test single-key queries
        assert_that(cache.make_reader(get_partition_range(mutations[0])))
            .produces(mutations[0])
            .produces_end_of_stream();

        assert_that(cache.make_reader(get_partition_range(mutations[2])))
            .produces(mutations[2])
            .produces_end_of_stream();

        // Test range query
        assert_that(cache.make_reader(query::full_partition_range))
            .produces(mutations[0])
            .produces(mutations[1])
            .produces(mutations[2])
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_single_key_queries_after_population_in_reverse_order) {
    return seastar::async([] {
        auto s = make_schema();

        std::vector<mutation> mutations = {
            make_key_mutation(s, "key1"),
            make_key_mutation(s, "key2"),
            make_key_mutation(s, "key3")
        };

        std::sort(mutations.begin(), mutations.end(), mutation_decorated_key_less_comparator());

        auto mt = make_lw_shared<memtable>(s);

        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        auto get_partition_range = [] (const mutation& m) {
            return query::partition_range::make_singular(query::ring_position(m.decorated_key()));
        };

        for (int i = 0; i < 2; ++i) {
            assert_that(cache.make_reader(get_partition_range(mutations[2])))
                .produces(mutations[2])
                .produces_end_of_stream();

            assert_that(cache.make_reader(get_partition_range(mutations[1])))
                .produces(mutations[1])
                .produces_end_of_stream();

            assert_that(cache.make_reader(get_partition_range(mutations[0])))
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

            auto cache = make_lw_shared<row_cache>(s, mt->as_data_source(), mt->as_key_source(), tracker);
            return [cache] (const query::partition_range& range) {
                return cache->make_reader(range);
            };
        });
    });
}

SEASTAR_TEST_CASE(test_eviction) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 100000; i++) {
            auto m = make_new_mutation(s);
            keys.emplace_back(m.decorated_key());
            cache.populate(m);
        }

        std::random_shuffle(keys.begin(), keys.end());

        for (auto&& key : keys) {
            cache.make_reader(query::partition_range::make_singular(key));
        }

        while (tracker.region().occupancy().used_space() > 0) {
            logalloc::shard_tracker().reclaim(100);
        }
    });
}

bool has_key(row_cache& cache, const dht::decorated_key& key) {
    auto reader = cache.make_reader(query::partition_range::make_singular(key));
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
    auto reader = cache.make_reader(query::partition_range::make_singular(m.decorated_key()));
    auto mo = reader().get0();
    BOOST_REQUIRE(bool(mo));
    assert_that(*mo).is_equal_to(m);
}

SEASTAR_TEST_CASE(test_update) {
    return seastar::async([] {
        auto s = make_schema();
        auto mt = make_lw_shared<memtable>(s);

        cache_tracker tracker;
        row_cache cache(s, mt->as_data_source(), mt->as_key_source(), tracker);

        BOOST_MESSAGE("Check cache miss with populate");

        int partition_count = 1000;

        // populate cache with some partitions
        std::vector<dht::decorated_key> keys_in_cache;
        for (int i = 0; i < partition_count; i++) {
            auto m = make_new_mutation(s);
            keys_in_cache.push_back(m.decorated_key());
            cache.populate(m);
        }

        // populate memtable with partitions not in cache
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

        BOOST_MESSAGE("Check cache miss with drop");

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

        BOOST_MESSAGE("Check cache hit with merge");

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
