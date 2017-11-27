/*
 * Copyright (C) 2017 ScyllaDB
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
#include "tests/test-utils.hh"

#include "simple_schema.hh"
#include "clustering_ranges_walker.hh"

using namespace std::chrono_literals;

struct step {
    position_in_partition_view pos;
    bool contained;
};

struct data_set {
    schema_ptr schema;
    query::clustering_row_ranges ranges;
    position_in_partition end_pos; // all >= than this is outside the range. TODO: derive from ranges
    std::vector<step> steps; // ordered by step::pos
    bool include_static_row = true;
};

static void run_tests(data_set ds) {
    position_in_partition::tri_compare cmp(*ds.schema);

    {
        clustering_ranges_walker walker(*ds.schema, ds.ranges, ds.include_static_row);

        auto check_all = [&] {
            for (auto&& e : ds.steps) {
                auto&& key = e.pos;
                BOOST_REQUIRE(walker.advance_to(key) == e.contained);
                BOOST_REQUIRE(walker.advance_to(key) == e.contained); // should be idempotent
                BOOST_REQUIRE(walker.out_of_range() == (cmp(key, ds.end_pos) >= 0));
            }
            BOOST_REQUIRE(walker.out_of_range());
        };

        check_all();
        walker.reset();
        check_all();
    }

    // check advance_to() on all pairs of steps
    {
        for (unsigned i = 0; i < ds.steps.size(); ++i) {
            for (unsigned j = i; j < ds.steps.size(); ++j) {
                clustering_ranges_walker walker(*ds.schema, ds.ranges, ds.include_static_row);
                BOOST_REQUIRE(walker.advance_to(ds.steps[i].pos) == ds.steps[i].contained);
                BOOST_REQUIRE(walker.out_of_range() == (cmp(ds.steps[i].pos, ds.end_pos) >= 0));
                BOOST_REQUIRE(walker.advance_to(ds.steps[j].pos) == ds.steps[j].contained);
                BOOST_REQUIRE(walker.out_of_range() == (cmp(ds.steps[j].pos, ds.end_pos) >= 0));
                BOOST_REQUIRE(!walker.advance_to(position_in_partition_view::after_all_clustered_rows()));
                BOOST_REQUIRE(walker.out_of_range());
            }
        }
    }

    // check advance_to() in case of trim_front()
    {
        for (unsigned i = 0; i < ds.steps.size(); ++i) {
            for (unsigned j = i; j < ds.steps.size(); ++j) {
                clustering_ranges_walker walker(*ds.schema, ds.ranges, ds.include_static_row);
                if (i) {
                    walker.advance_to(ds.steps[i - 1].pos);
                }

                walker.trim_front(position_in_partition(ds.steps[j].pos));

                // check that all before j are excluded
                for (unsigned k = i; k < j; ++k) {
                    BOOST_REQUIRE(!walker.advance_to(ds.steps[k].pos));
                }

                // check that all after j are as expected
                for (unsigned k = j; k < ds.steps.size(); ++k) {
                    BOOST_REQUIRE(walker.advance_to(ds.steps[k].pos) == ds.steps[k].contained);
                    BOOST_REQUIRE(walker.out_of_range() == (cmp(ds.steps[k].pos, ds.end_pos) >= 0));
                }
            }
        }
    }
}

SEASTAR_TEST_CASE(test_basic_operation_on_various_data_sets) {
    simple_schema s;
    auto keys = s.make_ckeys(10);

    {
        auto ranges = query::clustering_row_ranges({
            query::clustering_range::make({keys[1], true}, {keys[1], true}),
            query::clustering_range::make({keys[2], false}, {keys[3], false}),
            query::clustering_range::make({keys[4], true}, {keys[6], false}),
            query::clustering_range::make({keys[6], true}, {keys[6], true}),
        });

        auto end_pos = position_in_partition::after_key(keys[6]);

        auto steps = std::vector<step>({
            {position_in_partition_view::for_static_row(),            true},
            {position_in_partition_view::before_all_clustered_rows(), false},
            {position_in_partition_view::for_key(keys[1]),            true},
            {position_in_partition_view::for_key(keys[2]),            false},
            {position_in_partition_view::for_key(keys[3]),            false},
            {position_in_partition_view::for_key(keys[4]),            true},
            {position_in_partition_view::for_key(keys[5]),            true},
            {position_in_partition_view::for_key(keys[6]),            true},
            {position_in_partition_view::for_key(keys[7]),            false},
            {position_in_partition_view::after_all_clustered_rows(),  false},
        });

        run_tests(data_set{s.schema(), ranges, end_pos, steps});
    }

    {
        auto ranges = query::clustering_row_ranges({
            query::clustering_range::make({keys[1], false}, {keys[2], true}),
        });

        auto end_pos = position_in_partition::after_key(keys[2]);

        auto steps = std::vector<step>({
            {position_in_partition_view::for_static_row(),            true},
            {position_in_partition_view::before_all_clustered_rows(), false},
            {position_in_partition_view::for_key(keys[1]),            false},
            {position_in_partition_view::for_key(keys[2]),            true},
            {position_in_partition_view::after_all_clustered_rows(),  false},
        });

        run_tests(data_set{s.schema(), ranges, end_pos, steps});
    }

    {
        auto ranges = query::clustering_row_ranges({
            query::clustering_range::make({keys[1], false}, {keys[2], true}),
        });

        auto end_pos = position_in_partition::after_key(keys[2]);

        auto steps = std::vector<step>({
            {position_in_partition_view::for_static_row(),            false},
            {position_in_partition_view::before_all_clustered_rows(), false},
            {position_in_partition_view::for_key(keys[1]),            false},
            {position_in_partition_view::for_key(keys[2]),            true},
            {position_in_partition_view::after_all_clustered_rows(),  false},
        });

        bool include_static_row = false;

        run_tests(data_set{s.schema(), ranges, end_pos, steps, include_static_row});
    }

    {
        auto ranges = query::clustering_row_ranges({});

        auto end_pos = position_in_partition(position_in_partition_view::before_all_clustered_rows());

        auto steps = std::vector<step>({
            {position_in_partition_view::for_static_row(),            true},
            {position_in_partition_view::before_all_clustered_rows(), false},
            {position_in_partition_view::for_key(keys[1]),            false},
            {position_in_partition_view::after_all_clustered_rows(),  false},
        });

        run_tests(data_set{s.schema(), ranges, end_pos, steps});
    }

    {
        auto ranges = query::clustering_row_ranges({});

        auto end_pos = position_in_partition(position_in_partition_view::for_static_row());

        auto steps = std::vector<step>({
            {position_in_partition_view::for_static_row(),            false},
            {position_in_partition_view::before_all_clustered_rows(), false},
            {position_in_partition_view::for_key(keys[1]),            false},
            {position_in_partition_view::after_all_clustered_rows(),  false},
        });

        bool include_static_row = false;

        run_tests(data_set{s.schema(), ranges, end_pos, steps, include_static_row});
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_range_overlap) {
    simple_schema s;
    auto keys = s.make_ckeys(10);

    auto range1 = query::clustering_range::make({keys[1], true}, {keys[2], false});
    auto range2 = query::clustering_range::make({keys[4], true}, {keys[6], false});
    auto ranges = query::clustering_row_ranges({range1, range2});

    clustering_ranges_walker walker(*s.schema(), ranges);
    position_in_partition::equal_compare eq(*s.schema());

    auto lbcc = walker.lower_bound_change_counter();

    BOOST_REQUIRE(eq(walker.lower_bound(), position_in_partition(position_in_partition_view::for_static_row())));

    BOOST_REQUIRE(walker.advance_to(
        position_in_partition(position_in_partition_view::for_static_row()),
        position_in_partition::for_key(keys[1])));

    BOOST_REQUIRE(!walker.advance_to(
        position_in_partition::for_key(keys[0]),
        position_in_partition::after_key(keys[0])));

    ++lbcc;
    BOOST_REQUIRE(walker.lower_bound_change_counter() == lbcc);
    BOOST_REQUIRE(eq(walker.lower_bound(), position_in_partition::for_range_start(range1)));

    BOOST_REQUIRE(walker.advance_to(
        position_in_partition::after_key(keys[0]),
        position_in_partition::for_key(keys[1])));

    BOOST_REQUIRE(walker.lower_bound_change_counter() == lbcc);
    BOOST_REQUIRE(eq(walker.lower_bound(), position_in_partition::for_range_start(range1)));

    BOOST_REQUIRE(walker.advance_to(
        position_in_partition::for_key(keys[1]),
        position_in_partition::for_key(keys[7])));

    BOOST_REQUIRE(!walker.advance_to(
        position_in_partition::for_key(keys[2]),
        position_in_partition::for_key(keys[3])));

    ++lbcc;
    BOOST_REQUIRE(walker.lower_bound_change_counter() == lbcc);
    BOOST_REQUIRE(eq(walker.lower_bound(), position_in_partition::for_range_start(range2)));

    BOOST_REQUIRE(walker.advance_to(
        position_in_partition::after_key(keys[2]),
        position_in_partition::for_key(keys[4])));

    BOOST_REQUIRE(walker.advance_to(
        position_in_partition::for_key(keys[3]),
        position_in_partition::for_key(keys[7])));

    BOOST_REQUIRE(walker.advance_to(
        position_in_partition::for_key(keys[4]),
        position_in_partition::for_key(keys[4])));

    BOOST_REQUIRE(!walker.advance_to(
        position_in_partition::for_key(keys[6]),
        position_in_partition::for_key(keys[7])));

    ++lbcc;
    BOOST_REQUIRE(walker.lower_bound_change_counter() == lbcc);
    BOOST_REQUIRE(walker.out_of_range());

    BOOST_REQUIRE(!walker.advance_to(
        position_in_partition::for_key(keys[7]),
        position_in_partition::for_key(keys[7])));

    BOOST_REQUIRE(!walker.advance_to(
        position_in_partition::for_key(keys[8]),
        position_in_partition::after_all_clustered_rows()));

    BOOST_REQUIRE(!walker.advance_to(
        position_in_partition::after_all_clustered_rows(),
        position_in_partition::after_all_clustered_rows()));

    BOOST_REQUIRE(walker.lower_bound_change_counter() == lbcc);
    BOOST_REQUIRE(walker.out_of_range());

    return make_ready_future<>();
}
