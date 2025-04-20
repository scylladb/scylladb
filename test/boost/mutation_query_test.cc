/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "utils/assert.hh"
#include <fmt/ranges.h>

#include <boost/test/unit_test.hpp>
#include "query-result-set.hh"
#include "query-result-writer.hh"

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/mutation_assertions.hh"
#include "test/lib/result_set_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/test_utils.hh"

#include "querier.hh"
#include "mutation_query.hh"
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>
#include "schema/schema_builder.hh"
#include "partition_slice_builder.hh"
#include "readers/from_mutations_v2.hh"
#include "mutation/mutation_rebuilder.hh"
#include "readers/mutation_source.hh"

using namespace std::literals::chrono_literals;

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("ck", bytes_type, column_kind::clustering_key)
        .with_column("s1", bytes_type, column_kind::static_column)
        .with_column("s2", bytes_type, column_kind::static_column)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .build();
}

struct mutation_less_cmp {
    bool operator()(const mutation& m1, const mutation& m2) const {
        SCYLLA_ASSERT(m1.schema() == m2.schema());
        return m1.decorated_key().less_compare(*m1.schema(), m2.decorated_key());
    }
};
static mutation_source make_source(std::vector<mutation> mutations) {
    return mutation_source([mutations = std::move(mutations)] (schema_ptr s, reader_permit permit, const dht::partition_range& range, const query::partition_slice& slice,
            tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding fwd_mr) {
        SCYLLA_ASSERT(range.is_full()); // slicing not implemented yet
        for (auto&& m : mutations) {
            if (slice.is_reversed()) {
                SCYLLA_ASSERT(m.schema()->make_reversed()->version() == s->version());
            } else {
                SCYLLA_ASSERT(m.schema() == s);
            }
        }
        return make_mutation_reader_from_mutations_v2(s, std::move(permit), mutations, slice, fwd);
    });
}

static query::partition_slice make_full_slice(const schema& s) {
    return partition_slice_builder(s).build();
}

static auto inf32 = std::numeric_limits<unsigned>::max();

static query::result_memory_accounter make_accounter() {
    return query::result_memory_accounter{ query::result_memory_limiter::unlimited_result_size };
}

// Called from a seastar thread
query::result_set to_result_set(const reconcilable_result& r, schema_ptr s, const query::partition_slice& slice) {
    return query::result_set::from_raw_result(s, slice, to_data_query_result(r, s, slice, inf32, inf32).get());
}

static reconcilable_result mutation_query(schema_ptr s, reader_permit permit, const mutation_source& source, const dht::partition_range& range,
        const query::partition_slice& slice, uint64_t row_limit, uint32_t partition_limit, gc_clock::time_point query_time) {

    auto querier = query::querier(source, s, std::move(permit), range, slice, {});
    auto close_querier = deferred_close(querier);
    auto rrb = reconcilable_result_builder(*s, slice, make_accounter());
    return querier.consume_page(std::move(rrb), row_limit, partition_limit, query_time).get();
}

SEASTAR_TEST_CASE(test_reading_from_single_partition) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A:v")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", data_value(bytes("B:v")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("C")), "v1", data_value(bytes("C:v")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("D")), "v1", data_value(bytes("D:v")), 1);

        auto src = make_source({m1});

        // Test full slice, but with row limit
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, 2, query::max_partitions, now);

            // FIXME: use mutation assertions
            assert_that(to_result_set(result, s, slice))
                .has_size(2)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("A")))
                    .with_column("v1", data_value(bytes("A:v"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B:v"))));
        }

        // Test slicing in the middle
        {
            auto slice = partition_slice_builder(*s).
                with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("B"))))
                .build();

            reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, query::max_rows, query::max_partitions, now);

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B:v"))));
        }
    });
}

SEASTAR_TEST_CASE(test_cells_are_expired_according_to_query_timestamp) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));

        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(*s->get_column_definition("v1")->type, 
                                   api::timestamp_type(1), bytes("A:v1"), now + 1s, 1s));

        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(*s->get_column_definition("v1")->type, 
                                   api::timestamp_type(1), bytes("B:v1")));

        auto src = make_source({m1});

        // Not expired yet
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, 1, query::max_partitions, now);

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("A")))
                    .with_column("v1", data_value(bytes("A:v1"))));
        }

        // Expired
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, 1, query::max_partitions, now + 2s);

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B:v1"))));
        }
    });
}

SEASTAR_TEST_CASE(test_reverse_ordering_is_respected) {
    return seastar::async([] {
        auto table_schema = make_schema();
        auto query_schema = table_schema->make_reversed();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto now = gc_clock::now();

        mutation m1(table_schema, partition_key::from_single_value(*table_schema, "key1"));

        m1.set_clustered_cell(clustering_key::from_single_value(*table_schema, bytes("A")), "v1", data_value(bytes("A_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*table_schema, bytes("B")), "v1", data_value(bytes("B_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*table_schema, bytes("C")), "v1", data_value(bytes("C_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*table_schema, bytes("D")), "v1", data_value(bytes("D_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*table_schema, bytes("E")), "v1", data_value(bytes("E_v1")), 1);

        auto src = make_source({m1});

        {
            auto slice = partition_slice_builder(*query_schema)
                .reversed()
                .build();

            reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 3, query::max_partitions, now);

            assert_that(to_result_set(result, query_schema, slice))
                .has_size(3)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("E")))
                    .with_column("v1", data_value(bytes("E_v1"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("D")))
                    .with_column("v1", data_value(bytes("D_v1"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("C")))
                    .with_column("v1", data_value(bytes("C_v1"))));
        }

        {
            auto slice = partition_slice_builder(*query_schema)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("E"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("D"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("C"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 3, query::max_partitions, now);

            assert_that(to_result_set(result, query_schema, slice))
                .has_size(3)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("E")))
                    .with_column("v1", data_value(bytes("E_v1"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("D")))
                    .with_column("v1", data_value(bytes("D_v1"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("C")))
                    .with_column("v1", data_value(bytes("C_v1"))));
        }

        {
            auto slice = partition_slice_builder(*query_schema)
                .with_range(query::clustering_range(
                    {clustering_key_prefix::from_single_value(*query_schema, bytes("E"))},
                    {clustering_key_prefix::from_single_value(*query_schema, bytes("C"))}
                    ))
                .reversed()
                .build();

            {
                reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 10, query::max_partitions, now);

                assert_that(to_result_set(result, query_schema, slice))
                    .has_size(3)
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("E")))
                        .with_column("v1", data_value(bytes("E_v1"))))
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("D")))
                        .with_column("v1", data_value(bytes("D_v1"))))
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("C")))
                        .with_column("v1", data_value(bytes("C_v1"))));
            }

            {
                reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 1, query::max_partitions, now);

                assert_that(to_result_set(result, query_schema, slice))
                    .has_size(1)
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("E")))
                        .with_column("v1", data_value(bytes("E_v1"))));
            }

            {
                reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 2, query::max_partitions, now);

                assert_that(to_result_set(result, query_schema, slice))
                    .has_size(2)
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("E")))
                        .with_column("v1", data_value(bytes("E_v1"))))
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("D")))
                        .with_column("v1", data_value(bytes("D_v1"))));
            }
        }

        {
            auto slice = partition_slice_builder(*query_schema)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("E"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("D"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("C"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 2, query::max_partitions, now);

            assert_that(to_result_set(result, query_schema, slice))
                .has_size(2)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("E")))
                    .with_column("v1", data_value(bytes("E_v1"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("D")))
                    .with_column("v1", data_value(bytes("D_v1"))));
        }

        {
            auto slice = partition_slice_builder(*query_schema)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("E"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("C"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 3, query::max_partitions, now);

            assert_that(to_result_set(result, query_schema, slice))
                .has_size(2)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("E")))
                    .with_column("v1", data_value(bytes("E_v1"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("C")))
                    .with_column("v1", data_value(bytes("C_v1"))));
        }

        {
            auto slice = partition_slice_builder(*query_schema)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*query_schema, bytes("B"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, 3, query::max_partitions, now);

            assert_that(to_result_set(result, query_schema, slice))
                .has_only(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B_v1"))));
        }
    });
}

SEASTAR_TEST_CASE(test_query_when_partition_tombstone_covers_live_cells) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));

        m1.partition().apply(tombstone(api::timestamp_type(1), now));
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A:v")), 1);

        auto src = make_source({m1});
        auto slice = make_full_slice(*s);

        reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, query::max_rows, query::max_partitions, now);

        assert_that(to_result_set(result, s, slice))
            .is_empty();
    });
}

SEASTAR_TEST_CASE(test_partitions_with_only_expired_tombstones_are_dropped) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .set_gc_grace_seconds(0)
            .build();

        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto now = gc_clock::now();

        auto new_key = [s] {
            static int ctr = 0;
            return partition_key::from_singular(*s, data_value(to_bytes(format("key{:d}", ctr++))));
        };

        auto make_ring = [&] (int n) {
            std::vector<mutation> ring;
            while (n--) {
                ring.push_back(mutation(s, new_key()));
            }
            std::sort(ring.begin(), ring.end(), mutation_decorated_key_less_comparator());
            return ring;
        };

        std::vector<mutation> ring = make_ring(4);

        ring[0].set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v")), api::new_timestamp());

        {
            auto ts = api::new_timestamp();
            ring[1].partition().apply(tombstone(ts, now));
            ring[1].set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v")), ts);
        }

        ring[2].partition().apply(tombstone(api::new_timestamp(), now));
        ring[3].set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v")), api::new_timestamp());

        auto src = make_source(ring);
        auto slice = make_full_slice(*s);

        auto query_time = now + std::chrono::seconds(1);

        reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, query::max_rows, query::max_partitions, query_time);

        BOOST_REQUIRE_EQUAL(result.partitions().size(), 2);
        BOOST_REQUIRE_EQUAL(result.row_count(), 2);
    });
}

SEASTAR_TEST_CASE(test_result_row_count) {
    return seastar::async([] {
            auto s = make_schema();
            tests::reader_concurrency_semaphore_wrapper semaphore;
            auto now = gc_clock::now();
            auto slice = partition_slice_builder(*s).build();

            mutation m1(s, partition_key::from_single_value(*s, "key1"));

            auto src = make_source({m1});

            auto r = to_data_query_result(mutation_query(s, semaphore.make_permit(), make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now),
                    s, slice, inf32, inf32).get();
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 0);

            m1.set_static_cell("s1", data_value(bytes("S_v1")), 1);
            r = to_data_query_result(mutation_query(s, semaphore.make_permit(), make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now),
                    s, slice, inf32, inf32).get();
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 1);

            m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A_v1")), 1);
            r = to_data_query_result(mutation_query(s, semaphore.make_permit(), make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now),
                    s, slice, inf32, inf32).get();
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 1);

            m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", data_value(bytes("B_v1")), 1);
            r = to_data_query_result(mutation_query(s, semaphore.make_permit(), make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now),
                    s, slice, inf32, inf32).get();
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 2);

            mutation m2(s, partition_key::from_single_value(*s, "key2"));
            m2.set_static_cell("s1", data_value(bytes("S_v1")), 1);
            r = to_data_query_result(mutation_query(s, semaphore.make_permit(), make_source({m1, m2}), query::full_partition_range, slice, 10000, query::max_partitions, now),
                    s, slice, inf32, inf32).get();
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 3);
    });
}

SEASTAR_TEST_CASE(test_partition_limit) {
    return seastar::async([] {
        auto s = make_schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.partition().apply(tombstone(api::timestamp_type(1), now));
        mutation m2(s, partition_key::from_single_value(*s, "key2"));
        m2.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A:v")), 1);
        mutation m3(s, partition_key::from_single_value(*s, "key3"));
        m3.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", data_value(bytes("B:v")), 1);

        std::vector<mutation> muts = {m1, m2, m3};
        std::sort(muts.begin(), muts.end(), mutation_decorated_key_less_comparator{});

        auto src = make_source(muts);
        auto slice = make_full_slice(*s);

        {
            reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, query::max_rows, 10, now);

            assert_that(to_result_set(result, s, slice))
                .has_size(2)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key3")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B:v"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key2")))
                    .with_column("ck", data_value(bytes("A")))
                    .with_column("v1", data_value(bytes("A:v"))));
        }

        {
            reconcilable_result result = mutation_query(s, semaphore.make_permit(), src, query::full_partition_range, slice, query::max_rows, 1, now);

            assert_that(to_result_set(result, s, slice))
                .has_size(1)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key3")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B:v"))));
        }
    });
}

static void data_query(schema_ptr s, reader_permit permit, const mutation_source& source, const dht::partition_range& range,
        const query::partition_slice& slice, query::result::builder& builder) {
    auto querier = query::querier(source, s, std::move(permit), range, slice, {});
    auto close_querier = deferred_close(querier);
    auto qrb = query_result_builder(*s, builder);
    querier.consume_page(std::move(qrb), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), gc_clock::now()).get();
}

SEASTAR_THREAD_TEST_CASE(test_result_size_calculation) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    random_mutation_generator gen(random_mutation_generator::generate_counters::no);
    std::vector<mutation> mutations = gen(1);
    schema_ptr s = gen.schema();
    mutation_source source = make_source(std::move(mutations));
    query::result_memory_limiter l(std::numeric_limits<ssize_t>::max());
    query::partition_slice slice = make_full_slice(*s);
    slice.options.set<query::partition_slice::option::allow_short_read>();

    query::result::builder digest_only_builder(slice, query::result_options{query::result_request::only_digest, query::digest_algorithm::xxHash},
            l.new_digest_read(query::max_result_size(query::result_memory_limiter::maximum_result_size), query::short_read::yes).get(), query::max_tombstones);
    data_query(s, semaphore.make_permit(), source, query::full_partition_range, slice, digest_only_builder);

    query::result::builder result_and_digest_builder(slice, query::result_options{query::result_request::result_and_digest, query::digest_algorithm::xxHash},
            l.new_data_read(query::max_result_size(query::result_memory_limiter::maximum_result_size), query::short_read::yes).get(), query::max_tombstones);
    data_query(s, semaphore.make_permit(), source, query::full_partition_range, slice, result_and_digest_builder);

    BOOST_REQUIRE_EQUAL(digest_only_builder.memory_accounter().used_memory(), result_and_digest_builder.memory_accounter().used_memory());
}

SEASTAR_THREAD_TEST_CASE(test_frozen_mutation_consumer) {
    random_mutation_generator gen(random_mutation_generator::generate_counters::no);
    schema_ptr s = gen.schema();
    std::vector<mutation> mutations = gen(1);
    const mutation& m = mutations[0];
    frozen_mutation fm = freeze(m);

    // sanity check unfreeze first
    mutation um = fm.unfreeze(s);
    BOOST_REQUIRE_EQUAL(um, m);

    // Rebuild mutation by consuming from the frozen_mutation
    mutation_rebuilder_v2 rebuilder(s);
    auto res = fm.consume(s, rebuilder);
    BOOST_REQUIRE(res.result);
    const auto& rebuilt = *res.result;
    BOOST_REQUIRE_EQUAL(rebuilt, m);
}

// Reproducer for #10598
SEASTAR_THREAD_TEST_CASE(test_reverse_range_tombstones) {
    auto now = gc_clock::now();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    schema_ptr table_schema = make_schema();
    schema_ptr query_schema = table_schema->make_reversed();
    mutation m(table_schema, partition_key::from_single_value(*table_schema, "key1"));
    auto rt = range_tombstone(
            clustering_key::from_single_value(*table_schema, "ck0"), bound_kind::incl_start,
            clustering_key::from_single_value(*table_schema, "ck1"), bound_kind::incl_end,
            tombstone(api::timestamp_type(42), now)
    );
    m.partition().apply_delete(*table_schema, rt);
    auto src = make_source({m});
    auto slice = partition_slice_builder(*query_schema).reversed().build();
    reconcilable_result result = mutation_query(query_schema, semaphore.make_permit(), src, query::full_partition_range, slice, query::max_rows, query::max_partitions, now);
    assert_that(result.partitions().at(0).mut().unfreeze(query_schema)).is_equal_to(reverse(m));
}
