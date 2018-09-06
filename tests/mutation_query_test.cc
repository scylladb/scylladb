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


#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>

#include <boost/test/unit_test.hpp>
#include <query-result-set.hh>
#include <query-result-writer.hh>

#include "tests/test_services.hh"
#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/result_set_assertions.hh"
#include "tests/mutation_source_test.hh"

#include "mutation_query.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include "schema_builder.hh"
#include "partition_slice_builder.hh"

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
        assert(m1.schema() == m2.schema());
        return m1.decorated_key().less_compare(*m1.schema(), m2.decorated_key());
    }
};
mutation_source make_source(std::vector<mutation> mutations) {
    return mutation_source([mutations = std::move(mutations)] (schema_ptr s, const dht::partition_range& range, const query::partition_slice& slice,
            const io_priority_class& pc, tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding fwd_mr) {
        assert(range.is_full()); // slicing not implemented yet
        for (auto&& m : mutations) {
            assert(m.schema() == s);
        }
        return flat_mutation_reader_from_mutations(mutations, slice, fwd);
    });
}

static query::partition_slice make_full_slice(const schema& s) {
    return partition_slice_builder(s).build();
}

static auto inf32 = std::numeric_limits<unsigned>::max();

query::result_set to_result_set(const reconcilable_result& r, schema_ptr s, const query::partition_slice& slice) {
    return query::result_set::from_raw_result(s, slice, to_data_query_result(r, s, slice, inf32, inf32));
}

SEASTAR_TEST_CASE(test_reading_from_single_partition) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = make_schema();
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

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 2, query::max_partitions, now).get0();

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

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, query::max_rows, query::max_partitions, now).get0();

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
        storage_service_for_tests ssft;
        auto s = make_schema();
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

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 1, query::max_partitions, now).get0();

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("A")))
                    .with_column("v1", data_value(bytes("A:v1"))));
        }

        // Expired
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 1, query::max_partitions, now + 2s).get0();

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
        storage_service_for_tests ssft;
        auto s = make_schema();
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));

        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", data_value(bytes("B_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("C")), "v1", data_value(bytes("C_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("D")), "v1", data_value(bytes("D_v1")), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("E")), "v1", data_value(bytes("E_v1")), 1);

        auto src = make_source({m1});

        {
            auto slice = partition_slice_builder(*s)
                .reversed()
                .build();

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 3, query::max_partitions, now).get0();

            assert_that(to_result_set(result, s, slice))
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
            auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("E"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("D"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("C"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 3, query::max_partitions, now).get0();

            assert_that(to_result_set(result, s, slice))
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
            auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range(
                    {clustering_key_prefix::from_single_value(*s, bytes("C"))},
                    {clustering_key_prefix::from_single_value(*s, bytes("E"))}))
                .reversed()
                .build();

            {
                reconcilable_result result = mutation_query(s, src,
                    query::full_partition_range, slice, 10, query::max_partitions, now).get0();

                assert_that(to_result_set(result, s, slice))
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
                reconcilable_result result = mutation_query(s, src,
                    query::full_partition_range, slice, 1, query::max_partitions, now).get0();

                assert_that(to_result_set(result, s, slice))
                    .has_size(1)
                    .has(a_row()
                        .with_column("pk", data_value(bytes("key1")))
                        .with_column("ck", data_value(bytes("E")))
                        .with_column("v1", data_value(bytes("E_v1"))));
            }

            {
                reconcilable_result result = mutation_query(s, src,
                    query::full_partition_range, slice, 2, query::max_partitions, now).get0();

                assert_that(to_result_set(result, s, slice))
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
            auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("E"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("D"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("C"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 2, query::max_partitions, now).get0();

            assert_that(to_result_set(result, s, slice))
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
            auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("E"))))
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("C"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 3, query::max_partitions, now).get0();

            assert_that(to_result_set(result, s, slice))
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
            auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("B"))))
                .reversed()
                .build();

            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, 3, query::max_partitions, now).get0();

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", data_value(bytes("key1")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B_v1"))));
        }
    });
}

SEASTAR_TEST_CASE(test_query_when_partition_tombstone_covers_live_cells) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = make_schema();
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));

        m1.partition().apply(tombstone(api::timestamp_type(1), now));
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A:v")), 1);

        auto src = make_source({m1});
        auto slice = make_full_slice(*s);

        reconcilable_result result = mutation_query(s, src,
            query::full_partition_range, slice, query::max_rows, query::max_partitions, now).get0();

        assert_that(to_result_set(result, s, slice))
            .is_empty();
    });
}

SEASTAR_TEST_CASE(test_partitions_with_only_expired_tombstones_are_dropped) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .set_gc_grace_seconds(0)
            .build();

        auto now = gc_clock::now();

        auto new_key = [s] {
            static int ctr = 0;
            return partition_key::from_singular(*s, data_value(to_bytes(sprint("key%d", ctr++))));
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

        reconcilable_result result = mutation_query(s, src, query::full_partition_range, slice, query::max_rows, query::max_partitions, query_time).get0();

        BOOST_REQUIRE_EQUAL(result.partitions().size(), 2);
        BOOST_REQUIRE_EQUAL(result.row_count(), 2);
    });
}

SEASTAR_TEST_CASE(test_result_row_count) {
    return seastar::async([] {
            storage_service_for_tests ssft;
            auto s = make_schema();
            auto now = gc_clock::now();
            auto slice = partition_slice_builder(*s).build();

            mutation m1(s, partition_key::from_single_value(*s, "key1"));

            auto src = make_source({m1});

            auto r = to_data_query_result(mutation_query(s, make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now).get0(), s, slice, inf32, inf32);
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 0);

            m1.set_static_cell("s1", data_value(bytes("S_v1")), 1);
            r = to_data_query_result(mutation_query(s, make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now).get0(), s, slice, inf32, inf32);
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 1);

            m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A_v1")), 1);
            r = to_data_query_result(mutation_query(s, make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now).get0(), s, slice, inf32, inf32);
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 1);

            m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", data_value(bytes("B_v1")), 1);
            r = to_data_query_result(mutation_query(s, make_source({m1}), query::full_partition_range, slice, 10000, query::max_partitions, now).get0(), s, slice, inf32, inf32);
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 2);

            mutation m2(s, partition_key::from_single_value(*s, "key2"));
            m2.set_static_cell("s1", data_value(bytes("S_v1")), 1);
            r = to_data_query_result(mutation_query(s, make_source({m1, m2}), query::full_partition_range, slice, 10000, query::max_partitions, now).get0(), s, slice, inf32, inf32);
            BOOST_REQUIRE_EQUAL(r.row_count().value(), 3);
    });
}

SEASTAR_TEST_CASE(test_partition_limit) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = make_schema();
        auto now = gc_clock::now();

        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.partition().apply(tombstone(api::timestamp_type(1), now));
        mutation m2(s, partition_key::from_single_value(*s, "key2"));
        m2.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", data_value(bytes("A:v")), 1);
        mutation m3(s, partition_key::from_single_value(*s, "key3"));
        m3.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", data_value(bytes("B:v")), 1);

        auto src = make_source({m1, m2, m3});
        auto slice = make_full_slice(*s);

        {
            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, query::max_rows, 10, now).get0();

            assert_that(to_result_set(result, s, slice))
                .has_size(2)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key2")))
                    .with_column("ck", data_value(bytes("A")))
                    .with_column("v1", data_value(bytes("A:v"))))
                .has(a_row()
                    .with_column("pk", data_value(bytes("key3")))
                    .with_column("ck", data_value(bytes("B")))
                    .with_column("v1", data_value(bytes("B:v"))));
        }

        {
            reconcilable_result result = mutation_query(s, src,
                query::full_partition_range, slice, query::max_rows, 1, now).get0();

            assert_that(to_result_set(result, s, slice))
                .has_size(1)
                .has(a_row()
                    .with_column("pk", data_value(bytes("key2")))
                    .with_column("ck", data_value(bytes("A")))
                    .with_column("v1", data_value(bytes("A:v"))));
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_result_size_calculation) {
    random_mutation_generator gen(random_mutation_generator::generate_counters::no);
    std::vector<mutation> mutations = gen(1);
    schema_ptr s = gen.schema();
    mutation_source source = make_source(std::move(mutations));
    query::result_memory_limiter l(std::numeric_limits<ssize_t>::max());
    query::partition_slice slice = make_full_slice(*s);
    slice.options.set<query::partition_slice::option::allow_short_read>();

    query::result::builder digest_only_builder(slice, query::result_options{query::result_request::only_digest, query::digest_algorithm::xxHash}, l.new_digest_read(query::result_memory_limiter::maximum_result_size).get0());
    data_query(s, source, query::full_partition_range, slice, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), gc_clock::now(), digest_only_builder).get0();

    query::result::builder result_and_digest_builder(slice, query::result_options{query::result_request::result_and_digest, query::digest_algorithm::xxHash}, l.new_data_read(query::result_memory_limiter::maximum_result_size).get0());
    data_query(s, source, query::full_partition_range, slice, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), gc_clock::now(), result_and_digest_builder).get0();

    BOOST_REQUIRE_EQUAL(digest_only_builder.memory_accounter().used_memory(), result_and_digest_builder.memory_accounter().used_memory());
}

