/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <boost/range/size.hpp>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "partition_version.hh"
#include "partition_snapshot_row_cursor.hh"
#include "partition_snapshot_reader.hh"
#include "clustering_interval_set.hh"

#include <seastar/testing/test_case.hh>
#include "test/lib/mutation_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/failure_injecting_allocation_strategy.hh"
#include "test/lib/log.hh"
#include "test/lib/schema_registry.hh"
#include "test/boost/range_tombstone_list_assertions.hh"
#include "real_dirty_memory_accounter.hh"

using namespace std::chrono_literals;


static thread_local mutation_application_stats app_stats_for_tests;

// Verifies that tombstones in "list" are monotonic, overlap with the requested range,
// and have information equivalent with "expected" in that range.
static
void check_tombstone_slice(const schema& s, const utils::chunked_vector<range_tombstone>& list,
    const query::clustering_range& range,
    std::initializer_list<range_tombstone> expected)
{
    range_tombstone_list actual(s);
    position_in_partition::less_compare less(s);
    position_in_partition prev_pos = position_in_partition::before_all_clustered_rows();

    for (auto&& rt : list) {
        if (!less(rt.position(), position_in_partition::for_range_end(range))) {
            BOOST_FAIL(format("Range tombstone out of range: {}, range: {}", rt, range));
        }
        if (!less(position_in_partition::for_range_start(range), rt.end_position())) {
            BOOST_FAIL(format("Range tombstone out of range: {}, range: {}", rt, range));
        }
        if (less(rt.position(), prev_pos)) {
            BOOST_FAIL(format("Range tombstone breaks position monotonicity: {}, list: {}", rt, list));
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

// Reads the rest of the partition into a mutation_partition object.
// There must be at least one entry ahead of the cursor.
// The cursor must be pointing at a row and valid.
// The cursor will not be pointing at a row after this.
static mutation_partition read_partition_from(const schema& schema, partition_snapshot_row_cursor& cur) {
    mutation_partition p(schema.shared_from_this());
    do {
        p.clustered_row(schema, cur.position(), is_dummy(cur.dummy()), is_continuous(cur.continuous()))
            .apply(schema, cur.row().as_deletable_row());
    } while (cur.next());
    return p;
}

SEASTAR_TEST_CASE(test_range_tombstone_slicing) {
    return seastar::async([] {
        logalloc::region r;
        mutation_cleaner cleaner(r, no_cache_tracker, app_stats_for_tests);
        simple_schema table;
        auto s = table.schema();
        with_allocator(r.allocator(), [&] {
            mutation_application_stats app_stats;
            logalloc::reclaim_lock l(r);

            auto rt1 = table.make_range_tombstone(table.make_ckey_range(1, 2));
            auto rt2 = table.make_range_tombstone(table.make_ckey_range(4, 7));
            auto rt3 = table.make_range_tombstone(table.make_ckey_range(6, 9));

            mutation_partition m1(s);
            m1.apply_delete(*s, rt1);
            m1.apply_delete(*s, rt2);
            m1.apply_delete(*s, rt3);

            partition_entry e(mutation_partition(*s, m1));

            auto snap = e.read(r, cleaner, s, no_cache_tracker);

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
            v2.partition().apply_weak(*s, m2, *s, app_stats);
            auto snap2 = e.read(r, cleaner, s, no_cache_tracker);

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

class mvcc_partition;

// Together with mvcc_partition abstracts memory management details of dealing with MVCC.
class mvcc_container {
    schema_ptr _schema;
    std::optional<cache_tracker> _tracker;
    std::optional<logalloc::region> _region_holder;
    std::optional<mutation_cleaner> _cleaner_holder;
    partition_snapshot::phase_type _phase = partition_snapshot::min_phase;
    dirty_memory_manager _mgr;
    std::optional<real_dirty_memory_accounter> _acc;
    logalloc::region* _region;
    mutation_cleaner* _cleaner;
public:
    struct no_tracker {};
    mvcc_container(schema_ptr s)
        : _schema(s)
        , _tracker(std::make_optional<cache_tracker>())
        , _acc(std::make_optional<real_dirty_memory_accounter>(_mgr, *_tracker, 0))
        , _region(&_tracker->region())
        , _cleaner(&_tracker->cleaner())
    { }
    mvcc_container(schema_ptr s, no_tracker)
        : _schema(s)
        , _region_holder(std::make_optional<logalloc::region>())
        , _cleaner_holder(std::make_optional<mutation_cleaner>(*_region_holder, nullptr, app_stats_for_tests))
        , _region(&*_region_holder)
        , _cleaner(&*_cleaner_holder)
    { }
    mvcc_container(mvcc_container&&) = delete;

    // Call only when this container was constructed with a tracker
    mvcc_partition make_evictable(const mutation_partition& mp);
    // Call only when this container was constructed without a tracker
    mvcc_partition make_not_evictable(const mutation_partition& mp);
    logalloc::region& region() { return *_region; }
    cache_tracker* tracker() { return &*_tracker; }
    mutation_cleaner& cleaner() { return *_cleaner; }
    partition_snapshot::phase_type next_phase() { return ++_phase; }
    partition_snapshot::phase_type phase() const { return _phase; }
    real_dirty_memory_accounter& accounter() { return *_acc; }

    mutation_partition squashed(partition_snapshot_ptr& snp) {
        logalloc::allocating_section as;
        return as(region(), [&] {
            return snp->squashed();
        });
    }

    // Merges other into this
    void merge(mvcc_container& other) {
        _region->merge(*other._region);
        _cleaner->merge(*other._cleaner);
    }
};

class mvcc_partition {
    schema_ptr _s;
    partition_entry _e;
    mvcc_container& _container;
    bool _evictable;
private:
    void apply_to_evictable(partition_entry&& src, schema_ptr src_schema);
    void apply(const mutation_partition& mp, schema_ptr mp_schema);
public:
    mvcc_partition(schema_ptr s, partition_entry&& e, mvcc_container& container, bool evictable)
        : _s(s), _e(std::move(e)), _container(container), _evictable(evictable) {
    }

    mvcc_partition(mvcc_partition&&) = default;

    ~mvcc_partition() {
        with_allocator(region().allocator(), [&] {
            _e = {};
        });
    }

    partition_entry& entry() { return _e; }
    schema_ptr schema() const { return _s; }
    logalloc::region& region() const { return _container.region(); }

    mvcc_partition& operator+=(const mutation&);
    mvcc_partition& operator+=(mvcc_partition&&);

    mutation_partition squashed() {
        logalloc::allocating_section as;
        return as(region(), [&] {
            return _e.squashed(*_s);
        });
    }

    void upgrade(schema_ptr new_schema) {
        logalloc::allocating_section as;
        with_allocator(region().allocator(), [&] {
            as(region(), [&] {
                _e.upgrade(_s, new_schema, _container.cleaner(), _container.tracker());
                _s = new_schema;
            });
        });
    }

    partition_snapshot_ptr read() {
        logalloc::allocating_section as;
        return as(region(), [&] {
            return _e.read(region(), _container.cleaner(), schema(), _container.tracker(), _container.phase());
        });
    }

    void evict() {
        with_allocator(region().allocator(), [&] {
            _e.evict(_container.cleaner());
        });
    }
};

void mvcc_partition::apply_to_evictable(partition_entry&& src, schema_ptr src_schema) {
    with_allocator(region().allocator(), [&] {
        logalloc::allocating_section as;
        mutation_cleaner src_cleaner(region(), no_cache_tracker, app_stats_for_tests);
        auto c = as(region(), [&] {
            if (_s != src_schema) {
                src.upgrade(src_schema, _s, src_cleaner, no_cache_tracker);
            }
            return _e.apply_to_incomplete(*schema(), std::move(src), src_cleaner, as, region(),
                *_container.tracker(), _container.next_phase(), _container.accounter());
        });
        repeat([&] {
            return c.run();
        }).get();
    });
}

mvcc_partition& mvcc_partition::operator+=(mvcc_partition&& src) {
    assert(_evictable);
    apply_to_evictable(std::move(src.entry()), src.schema());
    return *this;
}

mvcc_partition& mvcc_partition::operator+=(const mutation& m) {
    with_allocator(region().allocator(), [&] {
        apply(m.partition(), m.schema());
    });
    return *this;
}

void mvcc_partition::apply(const mutation_partition& mp, schema_ptr mp_s) {
    with_allocator(region().allocator(), [&] {
        if (_evictable) {
            apply_to_evictable(partition_entry(mutation_partition(*mp_s, mp)), mp_s);
        } else {
            logalloc::allocating_section as;
            as(region(), [&] {
                mutation_application_stats app_stats;
                _e.apply(*_s, mp, *mp_s, app_stats);
            });
        }
    });
}

mvcc_partition mvcc_container::make_evictable(const mutation_partition& mp) {
    return with_allocator(region().allocator(), [&] {
        logalloc::allocating_section as;
        return as(region(), [&] {
            return mvcc_partition(_schema, partition_entry::make_evictable(*_schema, mp), *this, true);
        });
    });
}

mvcc_partition mvcc_container::make_not_evictable(const mutation_partition& mp) {
    return with_allocator(region().allocator(), [&] {
        logalloc::allocating_section as;
        return as(region(), [&] {
            return mvcc_partition(_schema, partition_entry(mutation_partition(*_schema, mp)), *this, false);
        });
    });
}

SEASTAR_TEST_CASE(test_apply_to_incomplete) {
    return seastar::async([] {
        simple_schema table;
        mvcc_container ms(table.schema());
        auto&& s = *table.schema();

        auto new_mutation = [&] {
            return mutation(table.schema(), table.make_pkey(0));
        };

        auto mutation_with_row = [&] (clustering_key ck) {
            auto m = new_mutation();
            table.add_row(m, ck, "v");
            return m;
        };

        auto ck1 = table.make_ckey(1);
        auto ck2 = table.make_ckey(2);

        testlog.info("Check that insert falling into discontinuous range is dropped");
        {
            auto e = ms.make_evictable(mutation_partition::make_incomplete(s));
            auto m = new_mutation();
            table.add_row(m, ck1, "v");
            e += m;
            assert_that(table.schema(), e.squashed()).is_equal_to(mutation_partition::make_incomplete(s));
        }

        testlog.info("Check that continuity is a union");
        {
            auto m1 = mutation_with_row(ck2);
            auto e = ms.make_evictable(m1.partition());

            auto snap1 = e.read();

            auto m2 = mutation_with_row(ck2);
            e += m2;

            partition_version* latest = &*e.entry().version();
            for (rows_entry& row : latest->partition().clustered_rows()) {
                row.set_continuous(is_continuous::no);
            }

            auto m3 = mutation_with_row(ck1);
            e += m3;
            assert_that(table.schema(), e.squashed()).is_equal_to((m2 + m3).partition());

            // Check that snapshot data is not stolen when its entry is applied
            auto e2 = ms.make_evictable(mutation_partition(table.schema()));
            e2 += std::move(e);
            assert_that(table.schema(), ms.squashed(snap1)).is_equal_to(m1.partition());
            assert_that(table.schema(), e2.squashed()).is_equal_to((m2 + m3).partition());
        }
    });
}

SEASTAR_TEST_CASE(test_schema_upgrade_preserves_continuity) {
    return seastar::async([] {
        simple_schema table;
        mvcc_container ms(table.schema());

        auto new_mutation = [&] {
            return mutation(table.schema(), table.make_pkey(0));
        };

        auto mutation_with_row = [&] (clustering_key ck) {
            auto m = new_mutation();
            table.add_row(m, ck, "v");
            return m;
        };

        // FIXME: There is no assert_that() for mutation_partition
        auto assert_entry_equal = [&] (mvcc_partition& e, mutation m) {
            auto key = table.make_pkey(0);
            assert_that(mutation(e.schema(), key, e.squashed()))
                .is_equal_to(m);
        };

        auto m1 = mutation_with_row(table.make_ckey(1));
        m1.partition().clustered_rows().begin()->set_continuous(is_continuous::no);
        m1.partition().set_static_row_continuous(false);
        m1.partition().ensure_last_dummy(*m1.schema());

        auto e = ms.make_evictable(m1.partition());
        auto rd1 = e.read();

        auto m2 = mutation_with_row(table.make_ckey(3));
        m2.partition().ensure_last_dummy(*m2.schema());
        e += m2;

        auto new_schema = schema_builder(table.schema()).with_column("__new_column", utf8_type).build();

        auto cont_before = e.squashed().get_continuity(*table.schema());
        e.upgrade(new_schema);
        auto cont_after = e.squashed().get_continuity(*new_schema);
        rd1 = {};

        auto expected = m1 + m2;
        expected.partition().set_static_row_continuous(false); // apply_to_incomplete()
        assert_entry_equal(e, expected);
        BOOST_REQUIRE(cont_after.equals(*new_schema, cont_before));

        auto m3 = mutation_with_row(table.make_ckey(2));
        e += m3;

        auto m4 = mutation_with_row(table.make_ckey(0));
        table.add_static_row(m4, "s_val");
        e += m4;

        expected += m3;
        expected.partition().set_static_row_continuous(false); // apply_to_incomplete()
        assert_entry_equal(e, expected);
    });
}

SEASTAR_TEST_CASE(test_eviction_with_active_reader) {
    return seastar::async([] {
        {
            simple_schema table;
            mvcc_container ms(table.schema());
            auto&& s = *table.schema();
            auto pk = table.make_pkey();
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey(2);

            auto e = ms.make_evictable(mutation_partition(table.schema()));

            mutation m1(table.schema(), pk);
            m1.partition().clustered_row(s, ck2);
            e += m1;

            auto snap1 = e.read();

            mutation m2(table.schema(), pk);
            m2.partition().clustered_row(s, ck1);
            e += m2;

            auto snap2 = e.read();

            partition_snapshot_row_cursor cursor(s, *snap2);
            cursor.advance_to(position_in_partition_view::before_all_clustered_rows());
            BOOST_REQUIRE(cursor.continuous());
            BOOST_REQUIRE(cursor.key().equal(s, ck1));

            e.evict();

            {
                logalloc::reclaim_lock rl(ms.region());
                cursor.maybe_refresh();
                auto mp = read_partition_from(s, cursor);
                assert_that(table.schema(), mp).is_equal_to(s, (m1 + m2).partition());
            }
        }
    });
}

SEASTAR_TEST_CASE(test_apply_to_incomplete_respects_continuity) {
    // Test that apply_to_incomplete() drops entries from source which fall outside continuity
    // and that continuity is not affected.
    return seastar::async([] {
        {
            tests::schema_registry_wrapper registry;
            random_mutation_generator gen(registry, random_mutation_generator::generate_counters::no);
            auto s = gen.schema();
            mvcc_container ms(s);

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();
            mutation to_apply = gen();
            to_apply.partition().make_fully_continuous();

            // Without active reader
            auto test = [&] (bool with_active_reader) {
                mutation_application_stats app_stats;
                auto e = ms.make_evictable(m3.partition());

                auto snap1 = e.read();
                m2.partition().make_fully_continuous();
                e += m2;

                auto snap2 = e.read();
                m1.partition().make_fully_continuous();
                e += m1;

                partition_snapshot_ptr snap;
                if (with_active_reader) {
                    snap = e.read();
                }

                auto before = e.squashed();
                auto e_continuity = before.get_continuity(*s);

                auto expected_to_apply_slice = mutation_partition(*s, to_apply.partition());
                if (!before.static_row_continuous()) {
                    expected_to_apply_slice.static_row() = {};
                }

                auto expected = mutation_partition(*s, before);
                expected.apply_weak(*s, std::move(expected_to_apply_slice), app_stats);

                e += to_apply;
                assert_that(s, e.squashed())
                    .is_equal_to(expected, e_continuity.to_clustering_row_ranges())
                    .has_same_continuity(before);
            };

            test(false);
            test(true);
        }
    });
}

// Call with region locked.
static mutation_partition read_using_cursor(partition_snapshot& snap) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    partition_snapshot_row_cursor cur(*snap.schema(), snap);
    cur.maybe_refresh();
    auto mp = read_partition_from(*snap.schema(), cur);
    for (auto&& rt : snap.range_tombstones()) {
        mp.apply_delete(*snap.schema(), rt);
    }
    mp.apply(*snap.schema(), mutation_fragment(*snap.schema(), semaphore.make_permit(), static_row(snap.static_row(false))));
    mp.set_static_row_continuous(snap.static_row_continuous());
    mp.apply(snap.partition_tombstone());
    return mp;
}

SEASTAR_TEST_CASE(test_snapshot_cursor_is_consistent_with_merging) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        {
            tests::schema_registry_wrapper registry;
            random_mutation_generator gen(registry, random_mutation_generator::generate_counters::no);
            auto s = gen.schema();
            mvcc_container ms(s);

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();

            m2.partition().make_fully_continuous();
            m3.partition().make_fully_continuous();

            {
                auto e = ms.make_evictable(m1.partition());
                auto snap1 = e.read();
                e += m2;
                auto snap2 = e.read();
                e += m3;

                auto expected = e.squashed();
                auto snap = e.read();
                auto actual = read_using_cursor(*snap);

                assert_that(s, actual).has_same_continuity(expected);

                // Drop empty rows
                can_gc_fn never_gc = [] (tombstone) { return false; };
                actual.compact_for_compaction(*s, never_gc, gc_clock::now());
                expected.compact_for_compaction(*s, never_gc, gc_clock::now());

                assert_that(s, actual).is_equal_to(expected);
            }
        }
    });
}

SEASTAR_TEST_CASE(test_snapshot_cursor_is_consistent_with_merging_for_nonevictable) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        logalloc::region r;
        mutation_cleaner cleaner(r, no_cache_tracker, app_stats_for_tests);
        with_allocator(r.allocator(), [&] {
            random_mutation_generator gen(registry, random_mutation_generator::generate_counters::no);
            auto s = gen.schema();

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();

            m1.partition().make_fully_continuous();
            m2.partition().make_fully_continuous();
            m3.partition().make_fully_continuous();

            {
                mutation_application_stats app_stats;
                logalloc::reclaim_lock rl(r);
                auto e = partition_entry(mutation_partition(*s, m3.partition()));
                auto snap1 = e.read(r, cleaner, s, no_cache_tracker);
                e.apply(*s, m2.partition(), *s, app_stats);
                auto snap2 = e.read(r, cleaner, s, no_cache_tracker);
                e.apply(*s, m1.partition(), *s, app_stats);

                auto expected = e.squashed(*s);
                auto snap = e.read(r, cleaner, s, no_cache_tracker);
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
                auto snap1 = e.read(r, tracker.cleaner(), s, &tracker);
                e.add_version(*s, &tracker).partition()
                    .clustered_row(*s, ss.make_ckey(1), is_dummy::no, is_continuous::no);
                e.add_version(*s, &tracker).partition()
                    .clustered_row(*s, ss.make_ckey(2), is_dummy::no, is_continuous::no);

                auto expected = mutation_partition(*s, m1.partition());
                expected.clustered_row(*s, ss.make_ckey(1), is_dummy::no, is_continuous::no);
                expected.clustered_row(*s, ss.make_ckey(2), is_dummy::no, is_continuous::no);

                auto snap = e.read(r, tracker.cleaner(), s, &tracker);
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
            auto snap1 = e.read(r, tracker.cleaner(), table.schema(), &tracker);

            {
                auto&& p1 = snap1->version()->partition();
                p1.clustered_row(s, table.make_ckey(0), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(1), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(2), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(3), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(6), is_dummy::no, is_continuous::no);
                p1.ensure_last_dummy(s);
            }

            auto snap2 = e.read(r, tracker.cleaner(), table.schema(), &tracker, 1);

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

            e.evict(tracker.cleaner());

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(4)));
                BOOST_REQUIRE(eq(cur.position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());
                BOOST_REQUIRE(cur.next());
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
                mutation_partition m2 = mutation_partition(*second.schema(), second.partition());
                auto e = partition_entry(mutation_partition(*target.schema(), target.partition()));
                //auto snap1 = e.read(r, gen.schema());

                alloc.fail_after(fail_offset++);
                try {
                    mutation_application_stats app_stats;
                    e.apply(*target.schema(), std::move(m2), *second.schema(), app_stats);
                    alloc.stop_failing();
                    break;
                } catch (const std::bad_alloc&) {
                    mutation_application_stats app_stats;
                    assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                        .is_equal_to(target)
                        .has_same_continuity(target);
                    e.apply(*target.schema(), std::move(m2), *second.schema(), app_stats);
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

    tests::schema_registry_wrapper registry;
    do_test(random_mutation_generator(registry, random_mutation_generator::generate_counters::no));
    do_test(random_mutation_generator(registry, random_mutation_generator::generate_counters::yes));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_versions_are_merged_when_snapshots_go_away) {
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        logalloc::region r;
        mutation_cleaner cleaner(r, nullptr, app_stats_for_tests);
        with_allocator(r.allocator(), [&] {
            random_mutation_generator gen(registry, random_mutation_generator::generate_counters::no);
            auto s = gen.schema();

            mutation m1 = gen();
            mutation m2 = gen();
            mutation m3 = gen();

            m1.partition().make_fully_continuous();
            m2.partition().make_fully_continuous();
            m3.partition().make_fully_continuous();

            {
                auto e = partition_entry(mutation_partition(*s, m1.partition()));
                auto snap1 = e.read(r, cleaner, s, nullptr);

                {
                    mutation_application_stats app_stats;
                    logalloc::reclaim_lock rl(r);
                    e.apply(*s, m2.partition(), *s, app_stats);
                }

                auto snap2 = e.read(r, cleaner, s, nullptr);

                snap1 = {};
                snap2 = {};

                cleaner.drain().get();

                BOOST_REQUIRE_EQUAL(1, boost::size(e.versions()));
                assert_that(s, e.squashed(*s)).is_equal_to((m1 + m2).partition());
            }

            {
                auto e = partition_entry(mutation_partition(*s, m1.partition()));
                auto snap1 = e.read(r, cleaner, s, nullptr);

                {
                    mutation_application_stats app_stats;
                    logalloc::reclaim_lock rl(r);
                    e.apply(*s, m2.partition(), *s, app_stats);
                }

                auto snap2 = e.read(r, cleaner, s, nullptr);

                snap2 = {};
                snap1 = {};

                cleaner.drain().get();

                BOOST_REQUIRE_EQUAL(1, boost::size(e.versions()));
                assert_that(s, e.squashed(*s)).is_equal_to((m1 + m2).partition());
            }
        });
    });
}

// Reproducer of #4030
SEASTAR_TEST_CASE(test_snapshot_merging_after_container_is_destroyed) {
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        random_mutation_generator gen(registry, random_mutation_generator::generate_counters::no);
        auto s = gen.schema();

        mutation m1 = gen();
        m1.partition().make_fully_continuous();

        mutation m2 = gen();
        m2.partition().make_fully_continuous();

        auto c1 = std::make_unique<mvcc_container>(s, mvcc_container::no_tracker{});
        auto c2 = std::make_unique<mvcc_container>(s, mvcc_container::no_tracker{});

        auto e = std::make_unique<mvcc_partition>(c1->make_not_evictable(m1.partition()));
        auto snap1 = e->read();

        *e += m2;

        auto snap2 = e->read();

        while (!need_preempt()) {} // Ensure need_preempt() to force snapshot destruction to defer

        snap1 = {};

        c2->merge(*c1);

        snap2 = {};
        e.reset();
        c1 = {};

        c2->cleaner().drain().get();
    });
}
