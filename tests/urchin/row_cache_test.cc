/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "tests/urchin/mutation_assertions.hh"
#include "tests/urchin/mutation_reader_assertions.hh"

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
    m.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);
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

static
mutation_source as_data_source(lw_shared_ptr<memtable> mt) {
    return [mt] (const query::partition_range& range) {
        return mt->make_reader(range);
    };
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

        std::sort(mutations.begin(), mutations.end(), [s] (auto&& m1, auto&& m2) {
            return m1.decorated_key().less_compare(*s, m2.decorated_key());
        });

        auto mt = make_lw_shared<memtable>(s);

        for (auto&& m : mutations) {
            mt->apply(m);
        }

        cache_tracker tracker;
        row_cache cache(s, as_data_source(mt), tracker);

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

        assert_that(cache.make_reader(query::full_partition_range))
            .produces(mutations[0])
            .produces(mutations[1])
            .produces(mutations[2])
            .produces_end_of_stream();
    });
}
