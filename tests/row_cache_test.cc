/*
 * Copyright 2015 Cloudius Systems
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
mutation make_key_mutation(schema_ptr s, bytes key) {
    mutation m(partition_key::from_single_value(*s, key), s);
    static thread_local int next_value = 1;
    static thread_local api::timestamp_type next_timestamp = 1;
    m.set_clustered_cell(clustering_key::make_empty(*s), "v", to_bytes(sprint("v%d", next_value++)), next_timestamp++);
    return m;
}

static
mutation make_unique_mutation(schema_ptr s) {
    static int key_sequence = 0;
    return make_key_mutation(s, bytes(sprint("key%d", key_sequence++).c_str()));
}

SEASTAR_TEST_CASE(test_cache_delegates_to_underlying) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_unique_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, [m] (const query::partition_range&) {
            return make_reader_returning(m);
        }, tracker);

        assert_that(cache.make_reader(query::full_partition_range))
            .produces(m)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_cache_works_after_clearing) {
    return seastar::async([] {
        auto s = make_schema();
        auto m = make_unique_mutation(s);

        cache_tracker tracker;
        row_cache cache(s, [m] (const query::partition_range&) {
            return make_reader_returning(m);
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
        row_cache cache(s, mt->as_data_source(), tracker);

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
        row_cache cache(s, mt->as_data_source(), tracker);

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

            auto cache = make_lw_shared<row_cache>(s, mt->as_data_source(), tracker);
            return [cache] (const query::partition_range& range) {
                return cache->make_reader(range);
            };
        });
    });
}
