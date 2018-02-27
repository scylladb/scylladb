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

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/simple_schema.hh"
#include "tests/mutation_source_test.hh"
#include "tests/failure_injecting_allocation_strategy.hh"
#include "tests/range_tombstone_list_assertions.hh"

using namespace std::chrono_literals;


// Verifies that tombstones in "list" are monotonic, overlap with the requested range,
// and have information equivalent with "expected" in that range.
static
void check_tombstone_slice(const schema& s, std::vector<range_tombstone> list,
    const query::clustering_range& range,
    std::initializer_list<range_tombstone> expected)
{
    range_tombstone_list actual(s);
    position_in_partition::less_compare less(s);
    position_in_partition prev_pos = position_in_partition::before_all_clustered_rows();

    for (auto&& rt : list) {
        if (!less(rt.position(), position_in_partition::for_range_end(range))) {
            BOOST_FAIL(sprint("Range tombstone out of range: %s, range: %s", rt, range));
        }
        if (!less(position_in_partition::for_range_start(range), rt.end_position())) {
            BOOST_FAIL(sprint("Range tombstone out of range: %s, range: %s", rt, range));
        }
        if (!less(prev_pos, rt.position())) {
            BOOST_FAIL(sprint("Range tombstone breaks position monotonicity: %s, list: %s", rt, list));
        }
        prev_pos = position_in_partition(rt.position());
        actual.apply(s, rt);
    }

    actual.trim(s, query::clustering_row_ranges{range});

    range_tombstone_list expected_list(s);
    for (auto&& rt : expected) {
        expected_list.apply(s, rt);
    }
    expected_list.trim(s, query::clustering_row_ranges{range});

    assert_that(s, actual).is_equal_to(expected_list);
}

SEASTAR_TEST_CASE(test_range_tombstone_slicing) {
    return seastar::async([] {
        logalloc::region r;
        simple_schema table;
        auto s = table.schema();
        with_allocator(r.allocator(), [&] {
            logalloc::reclaim_lock l(r);

            auto rt1 = table.make_range_tombstone(table.make_ckey_range(1, 2));
            auto rt2 = table.make_range_tombstone(table.make_ckey_range(4, 7));
            auto rt3 = table.make_range_tombstone(table.make_ckey_range(6, 9));

            mutation_partition m1(s);
            m1.apply_delete(*s, rt1);
            m1.apply_delete(*s, rt2);
            m1.apply_delete(*s, rt3);

            partition_entry e(m1);

            auto snap = e.read(r, s, no_cache_tracker);

            auto check_range = [&s] (partition_snapshot& snap, const query::clustering_range& range,
                    std::initializer_list<range_tombstone> expected) {
                auto tombstones = snap.range_tombstones(
                    position_in_partition::for_range_start(range),
                    position_in_partition::for_range_end(range));
                check_tombstone_slice(*s, tombstones, range, expected);
            };

            check_range(*snap, table.make_ckey_range(0, 0), {});
            check_range(*snap, table.make_ckey_range(1, 1), {rt1});
            check_range(*snap, table.make_ckey_range(3, 4), {rt2});
            check_range(*snap, table.make_ckey_range(3, 5), {rt2});
            check_range(*snap, table.make_ckey_range(3, 6), {rt2, rt3});
            check_range(*snap, table.make_ckey_range(6, 6), {rt2, rt3});
            check_range(*snap, table.make_ckey_range(7, 10), {rt2, rt3});
            check_range(*snap, table.make_ckey_range(8, 10), {rt3});
            check_range(*snap, table.make_ckey_range(10, 10), {});
            check_range(*snap, table.make_ckey_range(0, 10), {rt1, rt2, rt3});

            auto rt4 = table.make_range_tombstone(table.make_ckey_range(1, 2));
            auto rt5 = table.make_range_tombstone(table.make_ckey_range(5, 8));

            mutation_partition m2(s);
            m2.apply_delete(*s, rt4);
            m2.apply_delete(*s, rt5);

            auto&& v2 = e.add_version(*s, no_cache_tracker);
            v2.partition().apply_weak(*s, m2, *s);
            auto snap2 = e.read(r, s, no_cache_tracker);

            check_range(*snap2, table.make_ckey_range(0, 0), {});
            check_range(*snap2, table.make_ckey_range(1, 1), {rt4});
            check_range(*snap2, table.make_ckey_range(3, 4), {rt2});
            check_range(*snap2, table.make_ckey_range(3, 5), {rt2, rt5});
            check_range(*snap2, table.make_ckey_range(3, 6), {rt2, rt3, rt5});
            check_range(*snap2, table.make_ckey_range(4, 4), {rt2});
            check_range(*snap2, table.make_ckey_range(5, 5), {rt2, rt5});
            check_range(*snap2, table.make_ckey_range(6, 6), {rt2, rt3, rt5});
            check_range(*snap2, table.make_ckey_range(7, 10), {rt2, rt3, rt5});
            check_range(*snap2, table.make_ckey_range(8, 8), {rt3, rt5});
            check_range(*snap2, table.make_ckey_range(9, 9), {rt3});
            check_range(*snap2, table.make_ckey_range(8, 10), {rt3, rt5});
            check_range(*snap2, table.make_ckey_range(10, 10), {});
            check_range(*snap2, table.make_ckey_range(0, 10), {rt4, rt2, rt3, rt5});
        });
    });

}

SEASTAR_TEST_CASE(test_apply_to_incomplete) {
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        simple_schema table;
        auto&& s = *table.schema();

        auto new_mutation = [&] {
            return mutation(table.schema(), table.make_pkey(0));
        };

        auto mutation_with_row = [&] (clustering_key ck) {
            auto m = new_mutation();
            table.add_row(m, ck, "v");
            return m;
        };

        auto apply = [&] (partition_entry& e, const mutation& m) {
            e.apply_to_incomplete(s, partition_entry(m.partition()), s, r, tracker);
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
            assert_that(table.schema(), e.squashed(s)).is_equal_to(mutation_partition::make_incomplete(s));
        });

        BOOST_TEST_MESSAGE("Check that continuity is a union");
        with_allocator(r.allocator(), [&] {
            logalloc::reclaim_lock l(r);
            auto m1 = mutation_with_row(ck2);
            auto e = partition_entry(m1.partition());

            auto snap1 = e.read(r, table.schema(), &tracker);

            auto m2 = mutation_with_row(ck2);
            apply(e, m2);

            partition_version* latest = &*e.version();
            for (rows_entry& row : latest->partition().clustered_rows()) {
                row.set_continuous(is_continuous::no);
            }

            auto m3 = mutation_with_row(ck1);
            apply(e, m3);
            assert_that(table.schema(), e.squashed(s)).is_equal_to((m2 + m3).partition());

            // Check that snapshot data is not stolen when its entry is applied
            auto e2 = partition_entry::make_evictable(s, mutation_partition(table.schema()));
            e2.apply_to_incomplete(s, std::move(e), s, r, tracker);
            assert_that(table.schema(), snap1->squashed()).is_equal_to(m1.partition());
            assert_that(table.schema(), e2.squashed(s)).is_equal_to((m2 + m3).partition());
        });
    });

}

SEASTAR_TEST_CASE(test_schema_upgrade_preserves_continuity) {
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        simple_schema table;

        auto new_mutation = [&] {
            return mutation(table.schema(), table.make_pkey(0));
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
                .is_equal_to(m);
        };

        auto apply = [&] (schema_ptr e_schema, partition_entry& e, const mutation& m) {
            e.apply_to_incomplete(*e_schema, partition_entry(m.partition()), *m.schema(), r, tracker);
        };

      with_allocator(r.allocator(), [&] {
        logalloc::reclaim_lock l(r);
        auto m1 = mutation_with_row(table.make_ckey(1));
        m1.partition().clustered_rows().begin()->set_continuous(is_continuous::no);
        m1.partition().set_static_row_continuous(false);
        m1.partition().ensure_last_dummy(*m1.schema());

        auto e = partition_entry(m1.partition());
        auto rd1 = e.read(r, table.schema(), &tracker);

        auto m2 = mutation_with_row(table.make_ckey(3));
        m2.partition().ensure_last_dummy(*m2.schema());
        apply(table.schema(), e, m2);

        auto new_schema = schema_builder(table.schema()).with_column("__new_column", utf8_type).build();

        auto cont_before = e.squashed(*table.schema()).get_continuity(*table.schema());
        e.upgrade(table.schema(), new_schema, no_cache_tracker);
        auto cont_after = e.squashed(*new_schema).get_continuity(*new_schema);
        rd1 = {};

        auto expected = m1 + m2;
        expected.partition().set_static_row_continuous(false); // apply_to_incomplete()
        assert_entry_equal(new_schema, e, expected);
        BOOST_REQUIRE(cont_after.equals(*new_schema, cont_before));

        auto m3 = mutation_with_row(table.make_ckey(2));
        apply(new_schema, e, m3);

        auto m4 = mutation_with_row(table.make_ckey(0));
        table.add_static_row(m4, "s_val");
        apply(new_schema, e, m4);

        expected += m3;
        expected.partition().set_static_row_continuous(false); // apply_to_incomplete()
        assert_entry_equal(new_schema, e, expected);
      });
    });
}

SEASTAR_TEST_CASE(test_full_eviction_marks_affected_range_as_discontinuous) {
    return seastar::async([] {
        cache_tracker tracker;
        logalloc::region& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            logalloc::reclaim_lock l(r);

            simple_schema table;
            auto&& s = *table.schema();
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey(2);

            auto e = partition_entry::make_evictable(s, mutation_partition(table.schema()));

            auto t = table.new_tombstone();
            auto&& p1 = e.open_version(s, &tracker).partition();
            p1.clustered_row(s, ck2);
            p1.apply(t);

            auto snap1 = e.read(r, table.schema(), &tracker);

            auto&& p2 = e.open_version(s, &tracker).partition();
            p2.clustered_row(s, ck1);

            auto snap2 = e.read(r, table.schema(), &tracker);

            e.evict(tracker);

            BOOST_REQUIRE(snap1->squashed().fully_discontinuous(s, position_range::all_clustered_rows()));
            BOOST_REQUIRE(snap2->squashed().fully_discontinuous(s, position_range::all_clustered_rows()));

            BOOST_REQUIRE(!snap1->squashed().static_row_continuous());
            BOOST_REQUIRE(!snap2->squashed().static_row_continuous());

            BOOST_REQUIRE_EQUAL(snap1->squashed().partition_tombstone(), t);
            BOOST_REQUIRE_EQUAL(snap2->squashed().partition_tombstone(), t);
        });
    });
}

SEASTAR_TEST_CASE(test_eviction_with_active_reader) {
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            simple_schema table;
            auto&& s = *table.schema();
            auto pk = table.make_pkey();
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey(2);

            auto e = partition_entry::make_evictable(s, mutation_partition(table.schema()));

            mutation m1(table.schema(), pk);
            m1.partition().clustered_row(s, ck2);
            e.apply_to_incomplete(s, partition_entry(m1.partition()), s, r, tracker);

            auto snap1 = e.read(r, table.schema(), &tracker);

            mutation m2(table.schema(), pk);
            m2.partition().clustered_row(s, ck1);
            e.apply_to_incomplete(s, partition_entry(m2.partition()), s, r, tracker);

            auto snap2 = e.read(r, table.schema(), &tracker);

            partition_snapshot_row_cursor cursor(s, *snap2);
            cursor.advance_to(position_in_partition_view::before_all_clustered_rows());
            BOOST_REQUIRE(cursor.continuous());
            BOOST_REQUIRE(cursor.key().equal(s, ck1));

            e.evict(tracker);

            cursor.maybe_refresh();
            do {
                BOOST_REQUIRE(!cursor.continuous());
                BOOST_REQUIRE(cursor.dummy());
            } while (cursor.next());
        });
    });
}

SEASTAR_TEST_CASE(test_apply_to_incomplete_respects_continuity) {
    // Test that apply_to_incomplete() drops entries from source which fall outside continuity
    // and that continuity is not affected.
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            random_mutation_generator gen(random_mutation_generator::generate_counters::no);
            auto s = gen.schema();

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();
            mutation to_apply = gen();
            to_apply.partition().make_fully_continuous();

            // Without active reader
            auto test = [&] (bool with_active_reader) {
                logalloc::reclaim_lock rl(r);
                auto e = partition_entry::make_evictable(*s, m3.partition());

                auto snap1 = e.read(r, s, &tracker);
                m2.partition().make_fully_continuous();
                e.apply_to_incomplete(*s, partition_entry(m2.partition()), *s, r, tracker);

                auto snap2 = e.read(r, s, &tracker);
                m3.partition().make_fully_continuous();
                e.apply_to_incomplete(*s, partition_entry(m1.partition()), *s, r, tracker);

                lw_shared_ptr<partition_snapshot> snap;
                if (with_active_reader) {
                    snap = e.read(r, s, &tracker);
                }

                auto before = e.squashed(*s);
                auto e_continuity = before.get_continuity(*s);

                auto expected_to_apply_slice = to_apply.partition();
                if (!before.static_row_continuous()) {
                    expected_to_apply_slice.static_row() = {};
                }

                auto expected = before;
                expected.apply_weak(*s, std::move(expected_to_apply_slice));

                e.apply_to_incomplete(*s, partition_entry(to_apply.partition()), *s, r, tracker);
                assert_that(s, e.squashed(*s))
                    .is_equal_to(expected, e_continuity.to_clustering_row_ranges())
                    .has_same_continuity(before);
            };

            test(false);
            test(true);
        });
    });
}

// Call with region locked.
static mutation_partition read_using_cursor(partition_snapshot& snap) {
    partition_snapshot_row_cursor cur(*snap.schema(), snap);
    cur.maybe_refresh();
    auto mp = cur.read_partition();
    for (auto&& rt : snap.range_tombstones()) {
        mp.apply_delete(*snap.schema(), rt);
    }
    mp.apply(*snap.schema(), static_row(snap.static_row(false)));
    mp.set_static_row_continuous(snap.static_row_continuous());
    mp.apply(snap.partition_tombstone());
    return mp;
}

SEASTAR_TEST_CASE(test_snapshot_cursor_is_consistent_with_merging) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            random_mutation_generator gen(random_mutation_generator::generate_counters::no);
            auto s = gen.schema();

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();

            m2.partition().make_fully_continuous();
            m3.partition().make_fully_continuous();

            {
                logalloc::reclaim_lock rl(r);
                auto e = partition_entry::make_evictable(*s, m1.partition());
                auto snap1 = e.read(r, s, &tracker);
                e.apply_to_incomplete(*s, partition_entry(m2.partition()), *s, r, tracker);
                auto snap2 = e.read(r, s, &tracker);
                e.apply_to_incomplete(*s, partition_entry(m3.partition()), *s, r, tracker);

                auto expected = e.squashed(*s);
                auto snap = e.read(r, s, &tracker);
                auto actual = read_using_cursor(*snap);

                assert_that(s, actual).has_same_continuity(expected);

                // Drop empty rows
                can_gc_fn never_gc = [] (tombstone) { return false; };
                actual.compact_for_compaction(*s, never_gc, gc_clock::now());
                expected.compact_for_compaction(*s, never_gc, gc_clock::now());

                assert_that(s, actual).is_equal_to(expected);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_snapshot_cursor_is_consistent_with_merging_for_nonevictable) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        logalloc::region r;
        with_allocator(r.allocator(), [&] {
            random_mutation_generator gen(random_mutation_generator::generate_counters::no);
            auto s = gen.schema();

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();

            m1.partition().make_fully_continuous();
            m2.partition().make_fully_continuous();
            m3.partition().make_fully_continuous();

            {
                logalloc::reclaim_lock rl(r);
                auto e = partition_entry(m3.partition());
                auto snap1 = e.read(r, s, no_cache_tracker);
                e.apply(*s, m2.partition(), *s);
                auto snap2 = e.read(r, s, no_cache_tracker);
                e.apply(*s, m1.partition(), *s);

                auto expected = e.squashed(*s);
                auto snap = e.read(r, s, no_cache_tracker);
                auto actual = read_using_cursor(*snap);

                BOOST_REQUIRE(expected.is_fully_continuous());
                BOOST_REQUIRE(actual.is_fully_continuous());

                assert_that(s, actual)
                    .is_equal_to(expected);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_continuity_merging_in_evictable) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            simple_schema ss;
            auto s = ss.schema();

            auto base_m = mutation(s, ss.make_pkey(0));

            auto m1 = base_m; // continuous in [-inf, 0]
            m1.partition().clustered_row(*s, ss.make_ckey(0), is_dummy::no, is_continuous::no);
            m1.partition().clustered_row(*s, position_in_partition::after_all_clustered_rows(), is_dummy::no, is_continuous::no);

            {
                logalloc::reclaim_lock rl(r);
                auto e = partition_entry::make_evictable(*s, m1.partition());
                auto snap1 = e.read(r, s, &tracker);
                e.add_version(*s, &tracker).partition()
                    .clustered_row(*s, ss.make_ckey(1), is_dummy::no, is_continuous::no);
                e.add_version(*s, &tracker).partition()
                    .clustered_row(*s, ss.make_ckey(2), is_dummy::no, is_continuous::no);

                auto expected = m1.partition();
                expected.clustered_row(*s, ss.make_ckey(1), is_dummy::no, is_continuous::no);
                expected.clustered_row(*s, ss.make_ckey(2), is_dummy::no, is_continuous::no);

                auto snap = e.read(r, s, &tracker);
                auto actual = read_using_cursor(*snap);
                auto actual2 = e.squashed(*s);

                assert_that(s, actual)
                    .has_same_continuity(expected)
                    .is_equal_to(expected);
                assert_that(s, actual2)
                    .has_same_continuity(expected)
                    .is_equal_to(expected);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_partition_snapshot_row_cursor) {
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            simple_schema table;
            auto&& s = *table.schema();

            auto e = partition_entry::make_evictable(s, mutation_partition(table.schema()));
            auto snap1 = e.read(r, table.schema(), &tracker);

            {
                auto&& p1 = snap1->version()->partition();
                p1.clustered_row(s, table.make_ckey(0), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(1), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(2), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(3), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(6), is_dummy::no, is_continuous::no);
                p1.ensure_last_dummy(s);
            }

            auto snap2 = e.read(r, table.schema(), &tracker, 1);

            partition_snapshot_row_cursor cur(s, *snap2);
            position_in_partition::equal_compare eq(s);

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(0)));
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(0)));
                BOOST_REQUIRE(!cur.continuous());
            }

            r.full_compaction();

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(0)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(1)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(2)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(2)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(2), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(2)));

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(3)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(4), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);

                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(3)));

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(6)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), position_in_partition::after_all_clustered_rows()));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(!cur.next());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());
            }

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);

                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(5)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(6)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());
            }

            e.evict(tracker);

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(!cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(5)));
                BOOST_REQUIRE(cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(!cur.advance_to(table.make_ckey(4)));
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(5)));
                BOOST_REQUIRE(cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(5)));
                BOOST_REQUIRE(cur.continuous());
            }
        });
    });
}

SEASTAR_TEST_CASE(test_apply_is_atomic) {
    auto do_test = [](auto&& gen) {
        failure_injecting_allocation_strategy alloc(standard_allocator());
        with_allocator(alloc, [&] {
            auto target = gen();
            auto second = gen();
            target.partition().make_fully_continuous();
            second.partition().make_fully_continuous();

            auto expected = target + second;

            size_t fail_offset = 0;
            while (true) {
                mutation_partition m2 = second.partition();
                auto e = partition_entry(target.partition());
                //auto snap1 = e.read(r, gen.schema());

                alloc.fail_after(fail_offset++);
                try {
                    e.apply(*target.schema(), std::move(m2), *second.schema());
                    alloc.stop_failing();
                    break;
                } catch (const std::bad_alloc&) {
                    assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                        .is_equal_to(target)
                        .has_same_continuity(target);
                    e.apply(*target.schema(), std::move(m2), *second.schema());
                    assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                        .is_equal_to(expected)
                        .has_same_continuity(expected);
                }
                assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                    .is_equal_to(expected)
                    .has_same_continuity(expected);
            }
        });
    };

    do_test(random_mutation_generator(random_mutation_generator::generate_counters::no));
    do_test(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    return make_ready_future<>();
}
