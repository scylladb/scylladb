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


#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <seastar/core/thread.hh>

#include "partition_version.hh"
#include "partition_snapshot_row_cursor.hh"
#include "disk-error-handler.hh"

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/simple_schema.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_apply_to_incomplete) {
    return seastar::async([] {
        logalloc::region r;
        simple_schema table;
        auto&& s = *table.schema();

        auto new_mutation = [&] {
            return mutation(table.make_pkey(0), table.schema());
        };

        auto mutation_with_row = [&] (clustering_key ck) {
            auto m = new_mutation();
            table.add_row(m, ck, "v");
            return m;
        };

        // FIXME: There is no assert_that() for mutation_partition
        auto assert_equal = [&] (mutation_partition mp1, mutation_partition mp2) {
            auto key = table.make_pkey(0);
            assert_that(mutation(table.schema(), key, std::move(mp1)))
                .is_equal_to(mutation(table.schema(), key, std::move(mp2)));
        };

        auto apply = [&] (partition_entry& e, const mutation& m) {
            e.apply_to_incomplete(s, partition_entry(m.partition()), s);
        };

        auto ck1 = table.make_ckey(1);
        auto ck2 = table.make_ckey(2);

        BOOST_TEST_MESSAGE("Check that insert falling into discontinuous range is dropped");
        with_allocator(r.allocator(), [&] {
            logalloc::reclaim_lock l(r);
            auto e = partition_entry(mutation_partition::make_incomplete(s));
            auto m = new_mutation();
            table.add_row(m, ck1, "v");
            apply(e, m);
            assert_equal(e.squashed(s), mutation_partition::make_incomplete(s));
        });

        BOOST_TEST_MESSAGE("Check that continuity from latest version wins");
        with_allocator(r.allocator(), [&] {
            logalloc::reclaim_lock l(r);
            auto m1 = mutation_with_row(ck2);
            auto e = partition_entry(m1.partition());

            auto snap1 = e.read(r, table.schema());

            auto m2 = mutation_with_row(ck2);
            apply(e, m2);

            partition_version* latest = &*e.version();
            partition_version* prev = latest->next();

            for (rows_entry& row : prev->partition().clustered_rows()) {
                row.set_continuous(is_continuous::no);
            }

            auto m3 = mutation_with_row(ck1);
            apply(e, m3);
            assert_equal(e.squashed(s), (m2 + m3).partition());

            // Check that snapshot data is not stolen when its entry is applied
            auto e2 = partition_entry(mutation_partition(table.schema()));
            e2.apply_to_incomplete(s, std::move(e), s);
            assert_equal(snap1->squashed(), m1.partition());
            assert_equal(e2.squashed(s), (m2 + m3).partition());
        });
    });

}

SEASTAR_TEST_CASE(test_schema_upgrade_preserves_continuity) {
    return seastar::async([] {
        logalloc::region r;
        simple_schema table;

        auto new_mutation = [&] {
            return mutation(table.make_pkey(0), table.schema());
        };

        auto mutation_with_row = [&] (clustering_key ck) {
            auto m = new_mutation();
            table.add_row(m, ck, "v");
            return m;
        };

        // FIXME: There is no assert_that() for mutation_partition
        auto assert_entry_equal = [&] (schema_ptr e_schema, partition_entry& e, mutation m) {
            auto key = table.make_pkey(0);
            assert_that(mutation(e_schema, key, e.squashed(*e_schema)))
                .is_equal_to(m)
                .has_same_continuity(m);
        };

        auto apply = [&] (schema_ptr e_schema, partition_entry& e, const mutation& m) {
            e.apply_to_incomplete(*e_schema, partition_entry(m.partition()), *m.schema());
        };

      with_allocator(r.allocator(), [&] {
        logalloc::reclaim_lock l(r);
        auto m1 = mutation_with_row(table.make_ckey(1));
        m1.partition().clustered_rows().begin()->set_continuous(is_continuous::no);
        m1.partition().set_static_row_continuous(false);
        m1.partition().ensure_last_dummy(*m1.schema());

        auto e = partition_entry(m1.partition());
        auto rd1 = e.read(r, table.schema());

        auto m2 = mutation_with_row(table.make_ckey(3));
        m2.partition().ensure_last_dummy(*m2.schema());
        apply(table.schema(), e, m2);

        auto new_schema = schema_builder(table.schema()).with_column("__new_column", utf8_type).build();

        e.upgrade(table.schema(), new_schema);
        rd1 = {};

        assert_entry_equal(new_schema, e, m1 + m2);

        auto m3 = mutation_with_row(table.make_ckey(2));
        apply(new_schema, e, m3);

        auto m4 = mutation_with_row(table.make_ckey(0));
        table.add_static_row(m4, "s_val");
        apply(new_schema, e, m4);

        assert_entry_equal(new_schema, e, m1 + m2 + m3);
      });
    });
}

SEASTAR_TEST_CASE(test_full_eviction_marks_affected_range_as_discontinuous) {
    return seastar::async([] {
        logalloc::region r;
        with_allocator(r.allocator(), [&] {
            logalloc::reclaim_lock l(r);

            simple_schema table;
            auto&& s = *table.schema();
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey(2);

            auto e = partition_entry(mutation_partition(table.schema()));

            auto t = table.new_tombstone();
            auto&& p1 = e.open_version(s).partition();
            p1.clustered_row(s, ck2);
            p1.apply(t);

            auto snap1 = e.read(r, table.schema());

            auto&& p2 = e.open_version(s).partition();
            p2.clustered_row(s, ck1);

            auto snap2 = e.read(r, table.schema());

            e.evict();

            BOOST_REQUIRE(snap1->squashed().fully_discontinuous(s, position_range(
                position_in_partition::before_all_clustered_rows(),
                position_in_partition::after_key(ck2)
            )));

            BOOST_REQUIRE(snap2->squashed().fully_discontinuous(s, position_range(
                position_in_partition::before_all_clustered_rows(),
                position_in_partition::after_key(ck2)
            )));

            BOOST_REQUIRE(!snap1->squashed().static_row_continuous());
            BOOST_REQUIRE(!snap2->squashed().static_row_continuous());

            BOOST_REQUIRE_EQUAL(snap1->squashed().partition_tombstone(), t);
            BOOST_REQUIRE_EQUAL(snap2->squashed().partition_tombstone(), t);
        });
    });
}

SEASTAR_TEST_CASE(test_eviction_with_active_reader) {
    return seastar::async([] {
        logalloc::region r;
        with_allocator(r.allocator(), [&] {
            simple_schema table;
            auto&& s = *table.schema();
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey(2);

            auto e = partition_entry(mutation_partition(table.schema()));

            auto&& p1 = e.open_version(s).partition();
            p1.clustered_row(s, ck2);
            p1.ensure_last_dummy(s); // needed by partition_snapshot_row_cursor

            auto snap1 = e.read(r, table.schema());

            auto&& p2 = e.open_version(s).partition();
            p2.clustered_row(s, ck1);

            auto snap2 = e.read(r, table.schema());

            partition_snapshot_row_cursor cursor(s, *snap2);
            cursor.advance_to(position_in_partition_view::before_all_clustered_rows());
            BOOST_REQUIRE(cursor.continuous());
            BOOST_REQUIRE(cursor.key().equal(s, ck1));

            e.evict();

            cursor.maybe_refresh();
            do {
                BOOST_REQUIRE(!cursor.continuous());
                BOOST_REQUIRE(cursor.dummy());
            } while (cursor.next());
        });
    });
}
