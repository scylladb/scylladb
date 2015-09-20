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

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>

#include <boost/test/unit_test.hpp>
#include <query-result-set.hh>

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/result_set_assertions.hh"

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
    return [mutations = std::move(mutations)] (const query::partition_range& range) {
        assert(range.is_full()); // slicing not implemented yet
        return make_reader_returning_many(mutations);
    };
}

static query::partition_slice make_full_slice(const schema& s) {
    return partition_slice_builder(s).build();
}

query::result_set to_result_set(const reconcilable_result& r, schema_ptr s, const query::partition_slice& slice) {
    return query::result_set::from_raw_result(s, slice, to_data_query_result(r, s, slice));
}

SEASTAR_TEST_CASE(test_reading_from_single_partition) {
    return seastar::async([] {
        auto s = make_schema();
        auto now = gc_clock::now();

        mutation m1(partition_key::from_single_value(*s, "key1"), s);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", bytes("A:v"), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")), "v1", bytes("B:v"), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("C")), "v1", bytes("C:v"), 1);
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("D")), "v1", bytes("D:v"), 1);

        auto src = make_source({m1});

        // Test full slice, but with row limit
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(src,
                query::full_partition_range, slice, 2, now).get0();

            // FIXME: use mutation assertions
            assert_that(to_result_set(result, s, slice))
                .has_size(2)
                .has(a_row()
                    .with_column("pk", bytes("key1"))
                    .with_column("ck", bytes("A"))
                    .with_column("v1", bytes("A:v")))
                .has(a_row()
                    .with_column("pk", bytes("key1"))
                    .with_column("ck", bytes("B"))
                    .with_column("v1", bytes("B:v")));
        }

        // Test slicing in the middle
        {
            auto slice = partition_slice_builder(*s).
                with_range(query::clustering_range::make_singular(
                    clustering_key_prefix::from_single_value(*s, bytes("B"))))
                .build();

            reconcilable_result result = mutation_query(src, query::full_partition_range, slice, query::max_rows, now).get0();

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", bytes("key1"))
                    .with_column("ck", bytes("B"))
                    .with_column("v1", bytes("B:v")));
        }
    });
}

SEASTAR_TEST_CASE(test_cells_are_expired_according_to_query_timestamp) {
    return seastar::async([] {
        auto s = make_schema();
        auto now = gc_clock::now();

        mutation m1(partition_key::from_single_value(*s, "key1"), s);

        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(api::timestamp_type(1), bytes("A:v1"), now + 1s, 1s));

        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("B")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(api::timestamp_type(1), bytes("B:v1")));

        auto src = make_source({m1});

        // Not expired yet
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(src,
                query::full_partition_range, slice, 1, now).get0();

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", bytes("key1"))
                    .with_column("ck", bytes("A"))
                    .with_column("v1", bytes("A:v1")));
        }

        // Expired
        {
            auto slice = make_full_slice(*s);

            reconcilable_result result = mutation_query(src,
                query::full_partition_range, slice, 1, now + 2s).get0();

            assert_that(to_result_set(result, s, slice))
                .has_only(a_row()
                    .with_column("pk", bytes("key1"))
                    .with_column("ck", bytes("B"))
                    .with_column("v1", bytes("B:v1")));
        }
    });
}

SEASTAR_TEST_CASE(test_query_when_partition_tombstone_covers_live_cells) {
    return seastar::async([] {
        auto s = make_schema();
        auto now = gc_clock::now();

        mutation m1(partition_key::from_single_value(*s, "key1"), s);

        m1.partition().apply(tombstone(api::timestamp_type(1), now));
        m1.set_clustered_cell(clustering_key::from_single_value(*s, bytes("A")), "v1", bytes("A:v"), 1);

        auto src = make_source({m1});
        auto slice = make_full_slice(*s);

        reconcilable_result result = mutation_query(src,
            query::full_partition_range, slice, query::max_rows, now).get0();

        assert_that(to_result_set(result, s, slice))
            .is_empty();
    });
}
