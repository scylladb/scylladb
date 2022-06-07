/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/mutation_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/failure_injecting_allocation_strategy.hh"
#include "test/lib/log.hh"
#include "test/boost/range_tombstone_list_assertions.hh"
#include "real_dirty_memory_accounter.hh"

using namespace std::chrono_literals;


static thread_local mutation_application_stats app_stats_for_tests;

// Reads the rest of the partition into a mutation_partition object.
// There must be at least one entry ahead of the cursor.
// The cursor must be pointing at a row and valid.
// The cursor will not be pointing at a row after this.
static mutation_partition read_partition_from(const schema& schema, partition_snapshot_row_cursor& cur) {
    mutation_partition p(schema.shared_from_this());
    position_in_partition prev = position_in_partition::before_all_clustered_rows();
    do {
        testlog.trace("cur: {}", cur);
        p.clustered_row(schema, cur.position(), is_dummy(cur.dummy()), is_continuous(cur.continuous()))
            .apply(schema, cur.row().as_deletable_row());
        auto after_pos = position_in_partition::after_key(schema, cur.position());
        auto before_pos = position_in_partition::before_key(cur.position());
        if (cur.range_tombstone()) {
            p.apply_row_tombstone(schema, range_tombstone(prev, before_pos, cur.range_tombstone()));
        }
        if (cur.range_tombstone_for_row()) {
            p.apply_row_tombstone(schema, range_tombstone(before_pos, after_pos, cur.range_tombstone_for_row()));
        }
        prev = std::move(after_pos);
    } while (cur.next());

    if (cur.range_tombstone()) {
        p.apply_row_tombstone(schema, range_tombstone(prev, position_in_partition::after_all_clustered_rows(), cur.range_tombstone()));
    }

    return p;
}

class mvcc_partition;

// Together with mvcc_partition abstracts memory management details of dealing with MVCC.
class mvcc_container {
    schema_ptr _schema;
    std::optional<cache_tracker> _tracker;
    std::optional<logalloc::region> _region_holder;
    std::optional<mutation_cleaner> _cleaner_holder;
    partition_snapshot::phase_type _phase = partition_snapshot::min_phase;
    replica::dirty_memory_manager _mgr;
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
    cache_tracker* tracker() { return _tracker ? &*_tracker : nullptr; }
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
        evict();
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
            apply_to_evictable(partition_entry(mutation_partition_v2(*mp_s, mp)), mp_s);
        } else {
            logalloc::allocating_section as;
            as(region(), [&] {
                mutation_application_stats app_stats;
                _e.apply(region(), _container.cleaner(), *_s, mp, *mp_s, app_stats);
            });
        }
    });
}

mvcc_partition mvcc_container::make_evictable(const mutation_partition& mp) {
    return with_allocator(region().allocator(), [&] {
        logalloc::allocating_section as;
        return as(region(), [&] {
            auto p = mvcc_partition(_schema, partition_entry::make_evictable(*_schema, mp), *this, true);
            if (_tracker) {
                _tracker->insert(p.entry());
            }
            return p;
        });
    });
}

mvcc_partition mvcc_container::make_not_evictable(const mutation_partition& mp) {
    return with_allocator(region().allocator(), [&] {
        logalloc::allocating_section as;
        return as(region(), [&] {
            return mvcc_partition(_schema, partition_entry(mutation_partition_v2(*_schema, mp)), *this, false);
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
            random_mutation_generator gen(random_mutation_generator::generate_counters::no);
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


                auto sq = e.squashed();

                // After applying to_apply the continuity can be more narrow due to compaction with tombstones
                // present in to_apply.
                auto continuity_after = sq.get_continuity(*s);
                if (!continuity_after.contained_in(e_continuity)) {
                    BOOST_FAIL(format("Expected later continuity to be contained in earlier, later={}\n, earlier={}",
                                      continuity_after, e_continuity));
                }

                assert_that(s, std::move(sq))
                    .is_equal_to_compacted(expected, e_continuity.to_clustering_row_ranges());
            };

            test(false);
            test(true);
        }
    });
}

// Call with region locked.
static mutation_partition read_using_cursor(partition_snapshot& snap, bool reversed = false) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = snap.schema();
    if (reversed) {
        s = s->make_reversed();
    }
    partition_snapshot_row_cursor cur(*s, snap, false, reversed);
    cur.advance_to(position_in_partition::before_all_clustered_rows());
    auto mp = read_partition_from(*s, cur);
    mp.apply(*s, mutation_fragment(*s, semaphore.make_permit(), static_row(snap.static_row(false))));
    mp.set_static_row_continuous(snap.static_row_continuous());
    mp.apply(snap.partition_tombstone());
    return mp;
}

SEASTAR_TEST_CASE(test_snapshot_cursor_is_consistent_with_merging) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        {
            random_mutation_generator gen(random_mutation_generator::generate_counters::no);
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
                assert_that(s, actual).is_equal_to_compacted(expected);
            }
        }
    });
}

SEASTAR_TEST_CASE(test_snapshot_cursor_is_consistent_with_merging_for_nonevictable) {
    // Tests that reading many versions using a cursor gives the logical mutation back.
    return seastar::async([] {
        logalloc::region r;
        mutation_cleaner cleaner(r, no_cache_tracker, app_stats_for_tests);
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
                mutation_application_stats app_stats;
                logalloc::reclaim_lock rl(r);
                auto e = partition_entry(mutation_partition_v2(*s, m3.partition()));
                auto snap1 = e.read(r, cleaner, s, no_cache_tracker);
                e.apply(r, cleaner, *s, m2.partition(), *s, app_stats);
                auto snap2 = e.read(r, cleaner, s, no_cache_tracker);
                e.apply(r, cleaner, *s, m1.partition(), *s, app_stats);

                auto expected = e.squashed(*s);
                auto snap = e.read(r, cleaner, s, no_cache_tracker);
                auto actual = read_using_cursor(*snap);

                BOOST_REQUIRE(expected.is_fully_continuous());
                BOOST_REQUIRE(actual.is_fully_continuous());

                assert_that(s, actual)
                    .is_equal_to_compacted(expected);
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
            m1.partition().clustered_row(*s, position_in_partition::after_all_clustered_rows(), is_dummy::yes, is_continuous::no);

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
                    .is_equal_to_compacted(expected);
                assert_that(s, actual2)
                    .has_same_continuity(expected)
                    .is_equal_to_compacted(expected);
                e.evict(tracker.cleaner());
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
            e.evict(tracker.cleaner());
        });
    });
}

SEASTAR_TEST_CASE(test_partition_snapshot_row_cursor_reversed) {
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            simple_schema table;
            auto&& s = *table.schema();

            auto e = partition_entry::make_evictable(s, mutation_partition(table.schema()));
            auto snap1 = e.read(r, tracker.cleaner(), table.schema(), &tracker);

            int ck_0 = 10;
            int ck_1 = 9;
            int ck_2 = 8;
            int ck_3 = 7;
            int ck_4 = 6;
            int ck_5 = 5;
            int ck_6 = 4;

            {
                auto&& p1 = snap1->version()->partition();
                p1.clustered_row(s, table.make_ckey(ck_0), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(ck_1), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(ck_2), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(ck_3), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(ck_6), is_dummy::no, is_continuous::no);
                p1.ensure_last_dummy(s);
            }

            auto snap2 = e.read(r, tracker.cleaner(), table.schema(), &tracker, 1);

            auto rev_s = s.make_reversed();
            partition_snapshot_row_cursor cur(*rev_s, *snap2, false, true);
            position_in_partition::equal_compare eq(s);

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(ck_0)));
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_0)));
                BOOST_REQUIRE(cur.continuous());
            }

            r.full_compaction();

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_0)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_1)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_2)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_2)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_2)));
                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_3)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(ck_4), is_dummy::no, is_continuous::no);
            }

            {
                logalloc::reclaim_lock rl(r);

                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_3)));

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_4)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_6)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(!cur.next());
            }

            {
                logalloc::reclaim_lock rl(r);

                BOOST_REQUIRE(cur.advance_to(position_in_partition::before_all_clustered_rows()));
                BOOST_REQUIRE(cur.continuous());
                BOOST_REQUIRE(cur.next());

                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_0)));
                BOOST_REQUIRE(cur.continuous());
                BOOST_REQUIRE(cur.next());

                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_1)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(ck_3)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_3)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(ck_5), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);

                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_3)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_4)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_5)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_6)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(!cur.next());
            }

            // Test refresh after eviction
            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(ck_3)));
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_3)));
            }

            e.evict(tracker.cleaner());

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(ck_5), is_dummy::no, is_continuous::yes);
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_3)));
                BOOST_REQUIRE(!cur.continuous());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(ck_4)));
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_4)));
                BOOST_REQUIRE(!cur.continuous());
                BOOST_REQUIRE(cur.next());
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(ck_5)));
                BOOST_REQUIRE(!cur.continuous());
            }
        });
    });
}

SEASTAR_TEST_CASE(test_cursor_tracks_continuity_in_reversed_mode) {
    return seastar::async([] {
        cache_tracker tracker;
        auto& r = tracker.region();
        with_allocator(r.allocator(), [&] {
            simple_schema table;
            auto&& s = *table.schema();

            auto e = partition_entry::make_evictable(s, mutation_partition(table.schema()));
            tracker.insert(e);
            auto snap1 = e.read(r, tracker.cleaner(), table.schema(), &tracker);

            {
                auto&& p1 = snap1->version()->partition();
                tracker.insert(
                    p1.clustered_rows_entry(s, table.make_ckey(0), is_dummy::no, is_continuous::no));
                tracker.insert(
                    p1.clustered_rows_entry(s, table.make_ckey(4), is_dummy::no, is_continuous::no));
            }

            auto snap2 = e.read(r, tracker.cleaner(), table.schema(), &tracker, 1);

            {
                auto&& p2 = snap2->version()->partition();
                tracker.insert(
                    p2.clustered_rows_entry(s, table.make_ckey(3), is_dummy::no, is_continuous::yes));
                tracker.insert(
                    p2.clustered_rows_entry(s, table.make_ckey(5), is_dummy::no, is_continuous::no));
            }

            auto rev_s = s.make_reversed();
            partition_snapshot_row_cursor cur(*rev_s, *snap2, false, true);
            position_in_partition::equal_compare eq(s);

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(4)));
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(3)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(0)));
                BOOST_REQUIRE(cur.continuous());
            }

            r.full_compaction();

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(0)));
                BOOST_REQUIRE(cur.continuous());
            }

            {
                auto&& p2 = snap2->version()->partition();
                tracker.insert(
                    p2.clustered_rows_entry(s, table.make_ckey(1), is_dummy::no, is_continuous::yes));
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.maybe_refresh());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(0)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(eq(cur.get_iterator_in_latest_version()->position(), table.make_ckey(1)));

                {
                    auto res = cur.ensure_entry_in_latest();
                    BOOST_REQUIRE(res.inserted);
                    BOOST_REQUIRE(eq(res.row.position(), table.make_ckey(0)));
                }
            }

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(position_in_partition::before_all_clustered_rows()));
                BOOST_REQUIRE(eq(cur.table_position(), position_in_partition::after_all_clustered_rows()));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(5)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(4)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(3)));
                BOOST_REQUIRE(!cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(1)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(cur.next());
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(0)));
                BOOST_REQUIRE(cur.continuous());

                BOOST_REQUIRE(!cur.next());
            }
            e.evict(tracker.cleaner());
        });
    });
}

SEASTAR_TEST_CASE(test_ensure_entry_in_latest_in_reversed_mode) {
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
                p1.clustered_row(s, table.make_ckey(3), is_dummy::no, is_continuous::no);
                p1.clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::no);
                p1.ensure_last_dummy(s);
            }

            auto snap2 = e.read(r, tracker.cleaner(), table.schema(), &tracker, 1);

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(1), is_dummy::no, is_continuous::yes);
                p2.clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::no);
                p2.ensure_last_dummy(s);
            }

            auto rev_s = s.make_reversed();
            partition_snapshot_row_cursor cur(*rev_s, *snap2, false, true);
            position_in_partition::equal_compare eq(s);

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(3)));
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(3)));
                BOOST_REQUIRE(!cur.continuous());

                {
                    auto res = cur.ensure_entry_in_latest();
                    BOOST_REQUIRE(res.inserted);
                    BOOST_REQUIRE(eq(res.row.position(), table.make_ckey(3)));
                }

                BOOST_REQUIRE(cur.advance_to(table.make_ckey(3)));
                BOOST_REQUIRE(!cur.continuous());

                {
                    auto res = cur.ensure_entry_in_latest();
                    BOOST_REQUIRE(!res.inserted);
                }
            }
            e.evict(tracker.cleaner());
        });
    });
}

SEASTAR_TEST_CASE(test_ensure_entry_in_latest_does_not_set_continuity_in_reversed_mode) {
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
                p1.clustered_row(s, table.make_ckey(2), is_dummy::no, is_continuous::yes);
                p1.ensure_last_dummy(s);
            }

            auto snap2 = e.read(r, tracker.cleaner(), table.schema(), &tracker, 1);

            {
                auto&& p2 = snap2->version()->partition();
                p2.clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::no);
                p2.ensure_last_dummy(s);
            }

            auto rev_s = s.make_reversed();
            partition_snapshot_row_cursor cur(*rev_s, *snap2, false, true);
            position_in_partition::equal_compare eq(s);

            {
                logalloc::reclaim_lock rl(r);
                BOOST_REQUIRE(cur.advance_to(table.make_ckey(2)));
                BOOST_REQUIRE(eq(cur.table_position(), table.make_ckey(2)));
                BOOST_REQUIRE(cur.continuous());

                {
                    auto res = cur.ensure_entry_in_latest();
                    BOOST_REQUIRE(res.inserted);
                    BOOST_REQUIRE(eq(res.row.position(), table.make_ckey(2)));
                }

                BOOST_REQUIRE(cur.advance_to(table.make_ckey(0)));
                // the entry for ckey 2 in latest version should not be marked as continuous.
                BOOST_REQUIRE(!cur.continuous());
            }
            e.evict(tracker.cleaner());
        });
    });
}

SEASTAR_TEST_CASE(test_apply_is_atomic) {
    auto do_test = [](auto&& gen) {
        logalloc::region r;
        mutation_cleaner cleaner(r, no_cache_tracker, app_stats_for_tests);
        failure_injecting_allocation_strategy alloc(r.allocator());
        with_allocator(alloc, [&] {
            auto target = gen();
            auto second = gen();
            target.partition().make_fully_continuous();
            second.partition().make_fully_continuous();

            auto expected = target + second;

            size_t fail_offset = 0;
            while (true) {
                mutation_partition m2 = mutation_partition(*second.schema(), second.partition());
                auto e = partition_entry(mutation_partition_v2(*target.schema(), target.partition()));
                //auto snap1 = e.read(r, gen.schema());

                alloc.fail_after(fail_offset++);
                try {
                    mutation_application_stats app_stats;
                    e.apply(r, cleaner, *target.schema(), std::move(m2), *second.schema(), app_stats);
                    alloc.stop_failing();
                    break;
                } catch (const std::bad_alloc&) {
                    mutation_application_stats app_stats;
                    assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                        .is_equal_to_compacted(target)
                        .has_same_continuity(target);
                    e.apply(r, cleaner, *target.schema(), std::move(m2), *second.schema(), app_stats);
                    assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                        .is_equal_to_compacted(expected)
                        .has_same_continuity(expected);
                }
                assert_that(mutation(target.schema(), target.decorated_key(), e.squashed(*target.schema())))
                    .is_equal_to_compacted(expected)
                    .has_same_continuity(expected);
            }
        });
    };

    do_test(random_mutation_generator(random_mutation_generator::generate_counters::no));
    do_test(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_versions_are_merged_when_snapshots_go_away) {
    return seastar::async([] {
        logalloc::region r;
        mutation_cleaner cleaner(r, nullptr, app_stats_for_tests);
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
                auto e = partition_entry(mutation_partition_v2(*s, m1.partition()));
                auto snap1 = e.read(r, cleaner, s, nullptr);

                {
                    mutation_application_stats app_stats;
                    logalloc::reclaim_lock rl(r);
                    e.apply(r, cleaner, *s, m2.partition(), *s, app_stats);
                }

                auto snap2 = e.read(r, cleaner, s, nullptr);

                snap1 = {};
                snap2 = {};

                cleaner.drain().get();

                BOOST_REQUIRE_EQUAL(1, boost::size(e.versions()));
                assert_that(s, e.squashed(*s)).is_equal_to_compacted((m1 + m2).partition());
            }

            {
                auto e = partition_entry(mutation_partition_v2(*s, m1.partition()));
                auto snap1 = e.read(r, cleaner, s, nullptr);

                {
                    mutation_application_stats app_stats;
                    logalloc::reclaim_lock rl(r);
                    e.apply(r, cleaner, *s, m2.partition(), *s, app_stats);
                }

                auto snap2 = e.read(r, cleaner, s, nullptr);

                snap2 = {};
                snap1 = {};

                cleaner.drain().get();

                BOOST_REQUIRE_EQUAL(1, boost::size(e.versions()));
                assert_that(s, e.squashed(*s)).is_equal_to_compacted((m1 + m2).partition());
            }
        });
    });
}

// Reproducer of #4030
SEASTAR_TEST_CASE(test_snapshot_merging_after_container_is_destroyed) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
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
