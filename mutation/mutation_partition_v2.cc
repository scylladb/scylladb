/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <boost/range/adaptor/reversed.hpp>
#include "mutation_partition_v2.hh"
#include "clustering_interval_set.hh"
#include "converting_mutation_partition_applier.hh"
#include "partition_builder.hh"
#include "query-result-writer.hh"
#include "counters.hh"
#include "row_cache.hh"
#include <seastar/core/execution_stage.hh>
#include "compaction/compaction_garbage_collector.hh"
#include "mutation_partition_view.hh"
#include "utils/assert.hh"
#include "utils/unconst.hh"

extern logging::logger mplog;

mutation_partition_v2::mutation_partition_v2(const schema& s, const mutation_partition_v2& x)
        : _tombstone(x._tombstone)
        , _static_row(s, column_kind::static_column, x._static_row)
        , _static_row_continuous(x._static_row_continuous)
        , _rows()
#ifdef SEASTAR_DEBUG
        , _schema_version(s.version())
#endif
{
#ifdef SEASTAR_DEBUG
    SCYLLA_ASSERT(x._schema_version == _schema_version);
#endif
    auto cloner = [&s] (const rows_entry* x) -> rows_entry* {
        return current_allocator().construct<rows_entry>(s, *x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
}

mutation_partition_v2::mutation_partition_v2(const schema& s, mutation_partition&& x)
    : _tombstone(x.partition_tombstone())
    , _static_row(std::move(x.static_row()))
    , _static_row_continuous(x.static_row_continuous())
    , _rows(std::move(x.mutable_clustered_rows()))
#ifdef SEASTAR_DEBUG
    , _schema_version(s.version())
#endif
{
    auto&& tombstones = x.mutable_row_tombstones();
    if (!tombstones.empty()) {
        try {
            mutation_partition_v2 p(s);

            for (auto&& t: tombstones) {
                range_tombstone & rt = t.tombstone();
                p.clustered_rows_entry(s, rt.position(), is_dummy::yes, is_continuous::no);
                p.clustered_rows_entry(s, rt.end_position(), is_dummy::yes, is_continuous::yes)
                        .set_range_tombstone(rt.tomb);
            }

            apply(s, std::move(p));
        } catch (...) {
            _rows.clear_and_dispose(current_deleter<rows_entry>());
            throw;
        }
    }
}

mutation_partition_v2::mutation_partition_v2(const schema& s, const mutation_partition& x)
    : mutation_partition_v2(s, mutation_partition(s, x))
{ }

mutation_partition_v2::~mutation_partition_v2() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
}

mutation_partition_v2&
mutation_partition_v2::operator=(mutation_partition_v2&& x) noexcept {
    if (this != &x) {
        this->~mutation_partition_v2();
        new (this) mutation_partition_v2(std::move(x));
    }
    return *this;
}

void mutation_partition_v2::ensure_last_dummy(const schema& s) {
    check_schema(s);
    if (_rows.empty() || !_rows.rbegin()->is_last_dummy()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, rows_entry::last_dummy_tag(), is_continuous::yes));
        _rows.insert_before(_rows.end(), std::move(e));
    }
}

template <>
struct fmt::formatter<apply_resume> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const apply_resume& res, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}, {}}}", int(res._stage), res._pos);
    }
};

void mutation_partition_v2::apply(const schema& s, mutation_partition&& p) {
    apply(s, mutation_partition_v2(s, std::move(p)));
}
void mutation_partition_v2::apply(const schema& s, mutation_partition_v2&& p, cache_tracker* tracker, is_evictable evictable) {
    mutation_application_stats app_stats;
    apply_resume res;
    apply_monotonically(s, s, std::move(p), tracker, app_stats, never_preempt(), res, evictable);
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, const schema& p_s, mutation_partition_v2&& p, cache_tracker* tracker,
        mutation_application_stats& app_stats, preemption_check need_preempt, apply_resume& res, is_evictable evictable) {
#ifdef SEASTAR_DEBUG
    SCYLLA_ASSERT(_schema_version == s.version());
    SCYLLA_ASSERT(p._schema_version == p_s.version());
#endif
    bool same_schema = s.version() == p_s.version();
    _tombstone.apply(p._tombstone);
    if (same_schema) [[likely]] {
        _static_row.apply_monotonically(s, column_kind::static_column, std::move(p._static_row));
    } else {
        _static_row.apply_monotonically(s, p_s, column_kind::static_column, std::move(p._static_row));
    }
    _static_row_continuous |= p._static_row_continuous;

    rows_entry::tri_compare cmp(s);
    position_in_partition::equal_compare eq(s);
    auto del = current_deleter<rows_entry>();

    auto compact = [&] (rows_entry& e) {
        ++app_stats.rows_compacted_with_tombstones;
        e.compact(s, _tombstone);
    };

    if (p._tombstone) {
        rows_type::iterator i;
        if (res._stage == apply_resume::stage::partition_tombstone_compaction) {
            i = _rows.upper_bound(res._pos, cmp);
        } else {
            i = _rows.begin();
        }
        auto prev_i = (i == _rows.begin()) ? rows_type::iterator() : std::prev(i);
        while (i != _rows.end()) {
            compact(*i);
            if (prev_i) {
                maybe_drop(s, tracker, prev_i, app_stats);
            }
            if (need_preempt() && i != _rows.end()) {
                res = apply_resume(apply_resume::stage::partition_tombstone_compaction, i->position());
                return stop_iteration::no;
            }
            prev_i = i;
            ++i;
        }
        if (prev_i != _rows.end()) {
            maybe_drop(s, tracker, prev_i, app_stats);
        }
        // TODO: Drop redundant range tombstones
        p._tombstone = {};
    }

    // Inserting new entries into LRU here is generally unsafe because
    // it may violate the "older versions are evicted first" rule (see row_cache.md).
    // It could happen, that there are newer versions in the MVCC chain with the same
    // key, not involved in this merge. Inserting an entry here would put this
    // entry ahead in the LRU, and the newer entry could get evicted earlier leading
    // to apparent loss of writes.
    // To avoid this, when inserting sentinels we must use lru::add_before() so that
    // they are put right before in the same place in the LRU.

    // Note: This procedure is not violating the "older versions are evicted first" rule.
    // It may move some entries from the newer version into the old version,
    // so the older version may have entries while the new version is already experiencing
    // eviction. However, the original information which was there in the old version
    // is guaranteed to be evicted prior to that, so there is no way for old information
    // to be exposed by such eviction.

    auto p_i = p._rows.begin();
    auto i = _rows.begin();
    rows_type::iterator lb_i; // iterator into _rows for previously inserted entry.

    // When resuming, the predecessor of the sentinel may have been compacted.
    bool prev_compacted = true;

    if (res._stage < apply_resume::stage::merging_rows) {
        prev_compacted = false;
        res = apply_resume::merging_rows();
    }

    bool made_progress = false;

    // Engaged p_sentinel indicates that information in p up to sentinel->position() was
    // merged into this instance and that flags on the entry pointed to by p_i are
    // only valid for the key range up to sentinel->position().
    // We should insert the sentinel back before returning so that the sum of p and this instance
    // remains consistent, and attributes like continuity and range_tombstone do not
    // extend to before_all_clustering_keys() in p.
    // If this_sentinel is engaged then it will be inserted into this instance at
    // the same position as p_sentinel, and reflects information about the interval
    // preceding the sentinel.
    // We need two sentinels so that there is no gap in continuity in case there is no entry
    // in this instance at the position of p_sentinel.
    // The sentinel never has a clustering key position, so it carries no row information.
    alloc_strategy_unique_ptr<rows_entry> p_sentinel;
    alloc_strategy_unique_ptr<rows_entry> this_sentinel;
    auto insert_sentinel_back = defer([&] {
        // Note: this lambda will be run by a destructor (of the `defer` guard),
        // so it mustn't throw, or else it will crash the node.
        //
        // To prevent a `bad_alloc` during the tree insertion, we have to preallocate
        // some memory for the new tree nodes. This is done by the `hold_reserve`
        // constructed after the lambda.
        if (this_sentinel) {
            SCYLLA_ASSERT(p_i != p._rows.end());
            auto rt = this_sentinel->range_tombstone();
            auto insert_result = _rows.insert_before_hint(i, std::move(this_sentinel), cmp);
            auto i2 = insert_result.first;
            if (insert_result.second) {
                mplog.trace("{}: inserting sentinel at {}", fmt::ptr(this), i2->position());
                if (tracker) {
                    tracker->insert(*std::prev(i2), *i2);
                }
            } else {
                mplog.trace("{}: merging sentinel at {}", fmt::ptr(this), i2->position());
                i2->set_continuous(true);
                i2->set_range_tombstone(rt);
            }
        }
        if (p_sentinel) {
            SCYLLA_ASSERT(p_i != p._rows.end());
            if (cmp(p_i->position(), p_sentinel->position()) == 0) {
                mplog.trace("{}: clearing attributes on {}", fmt::ptr(&p), p_i->position());
                SCYLLA_ASSERT(p_i->dummy());
                p_i->set_continuous(false);
                p_i->set_range_tombstone({});
            } else {
                mplog.trace("{}: inserting sentinel at {}", fmt::ptr(&p), p_sentinel->position());
                auto insert_result = p._rows.insert_before_hint(p_i, std::move(p_sentinel), cmp);
                if (tracker) {
                    tracker->insert(*p_i, *insert_result.first);
                }
            }
        }
    });

    // This guard will ensure that LSA reserves one free segment more than it
    // needs for internal reasons.
    //
    // It will be destroyed immediately before the sentinel-inserting `defer`
    // happens, ensuring that the sentinel insertion has at least one free LSA segment
    // to work with. This should be enough, since we only need to allocate a few
    // B-tree nodes.
    auto memory_reserve_for_sentinel_inserts = hold_reserve(logalloc::segment_size);

    while (p_i != p._rows.end()) {
        rows_entry& src_e = *p_i;

        bool miss = true;
        if (i != _rows.end()) {
            auto x = cmp(*i, src_e);
            if (x < 0) {
                bool match;
                i = _rows.lower_bound(src_e, match, cmp);
                miss = !match;
            } else {
                miss = x > 0;
            }
        }

        // Invariants:
        //   i->position() >= p_i->position()

        // The block below reflects the information from interval (lb_i->position(), p_i->position()) to _rows,
        // up to the last entry in _rows which has position() < p_i->position(). The remainder is reflected by the act of
        // moving p_i itself.
        bool prev_interval_loaded = (evictable && src_e.continuous()) || (!evictable && src_e.range_tombstone());
        if (prev_interval_loaded) {
            // lb_i is only valid if prev_interval_loaded.
            rows_type::iterator prev_lb_i;

            if (lb_i) {
                // If there is lb_i, it means the interval starts exactly at lb_i->position() in p.
                // Increment is needed, we don't want to set attributes on the lower bound of the interval.
                prev_lb_i = lb_i;
                ++lb_i;
            } else {
                lb_i = _rows.begin();
            }

            while (lb_i != i) {
                bool compaction_worthwhile = src_e.range_tombstone() > lb_i->range_tombstone();

                // This works for both evictable and non-evictable snapshots.
                // For evictable snapshots we could replace the tombstone with newer, but due to
                // the "information monotonicity" rule, adding tombstone works too.
                lb_i->set_range_tombstone(lb_i->range_tombstone() + src_e.range_tombstone());
                lb_i->set_continuous(true);

                if (prev_compacted && prev_lb_i) {
                    maybe_drop(s, tracker, prev_lb_i, app_stats);
                }

                prev_compacted = false;
                if (lb_i->dummy()) {
                    prev_compacted = true;
                } else if (compaction_worthwhile) {
                    compact(*lb_i);
                    prev_compacted = true;
                }

                if (need_preempt()) {
                    auto s1 = alloc_strategy_unique_ptr<rows_entry>(
                            current_allocator().construct<rows_entry>(p_s,
                                 position_in_partition::after_key(p_s, lb_i->position()), is_dummy::yes, is_continuous::no));
                    alloc_strategy_unique_ptr<rows_entry> s2;
                    if (lb_i->position().is_clustering_row()) {
                        s2 = alloc_strategy_unique_ptr<rows_entry>(
                                current_allocator().construct<rows_entry>(s, s1->position(), is_dummy::yes, is_continuous::yes));
                        auto lb_i_next = std::next(lb_i);
                        if (lb_i_next != _rows.end() && lb_i_next->continuous()) {
                            s2->set_range_tombstone(lb_i_next->range_tombstone() + src_e.range_tombstone());
                        } else {
                            s2->set_range_tombstone(src_e.range_tombstone());
                        }
                    }
                    p_sentinel = std::move(s1);
                    this_sentinel = std::move(s2);
                    mplog.trace("preempted, res={}", res);
                    return stop_iteration::no;
                }

                prev_lb_i = lb_i;
                ++lb_i;
            }
        }

        auto next_p_i = std::next(p_i);

        // next_interval_loaded is true iff there are attributes on next_p_i which apply
        // to the interval (p_i->position(), next_p_i->position), and we
        // have to prepare a sentinel when removing p_i from p in case merging
        // needs to stop before next_p_i is moved.
        bool next_interval_loaded = next_p_i != p._rows.end()
                && ((evictable && next_p_i->continuous()) || (!evictable && next_p_i->range_tombstone()));

        bool do_compact = false;
        if (miss) {
            alloc_strategy_unique_ptr<rows_entry> s1;
            alloc_strategy_unique_ptr<rows_entry> s2;
            if (next_interval_loaded) {
                // FIXME: Avoid reallocation
                s1 = alloc_strategy_unique_ptr<rows_entry>(
                    current_allocator().construct<rows_entry>(p_s,
                        position_in_partition::after_key(p_s, src_e.position()), is_dummy::yes, is_continuous::no));
                if (src_e.position().is_clustering_row()) {
                    s2 = alloc_strategy_unique_ptr<rows_entry>(
                            current_allocator().construct<rows_entry>(s,
                                s1->position(), is_dummy::yes, is_continuous::yes));
                    if (i != _rows.end() && i->continuous()) {
                        s2->set_range_tombstone(i->range_tombstone() + src_e.range_tombstone());
                    } else {
                        s2->set_range_tombstone(src_e.range_tombstone());
                    }
                }
            }

            if (same_schema) [[likely]] {
                rows_type::key_grabber pi_kg(p_i);
                lb_i = _rows.insert_before(i, std::move(pi_kg));
            } else {
                // FIXME: avoid cell reallocation.
                // We are copying the row to make exception safety simpler,
                // but it's not inherently necessary and could be avoided.
                auto new_e = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(s, p_s, src_e));
                lb_i = _rows.insert_before(i, std::move(new_e));
                lb_i->swap(src_e);
                p_i = p._rows.erase_and_dispose(p_i, del);
            }
            p_sentinel = std::move(s1);
            this_sentinel = std::move(s2);

            // Check if src_e (now: lb_i) fell into a continuous range.
            // The range past the last entry is also always implicitly continuous.
            if (i == _rows.end() || i->continuous()) {
                tombstone i_rt = i != _rows.end() ? i->range_tombstone() : tombstone();
                // Cannot apply only-row range tombstone falling into a continuous range without inserting extra entry.
                // Should not occur in practice due to the "older versions are evicted first" rule.
                // Never occurs in non-evictable snapshots because they are continuous.
                if (!lb_i->continuous() && lb_i->range_tombstone() > i_rt) {
                    if (lb_i->dummy()) {
                        lb_i->set_range_tombstone(i_rt);
                    } else {
                        position_in_partition_view i_pos = i != _rows.end() ? i->position()
                                : position_in_partition_view::after_all_clustered_rows();
                        // See the "no singular tombstones" rule.
                        mplog.error("Cannot merge entry {} with rt={}, cont=0 into continuous range before {} with rt={}",
                                lb_i->position(), lb_i->range_tombstone(), i_pos, i_rt);
                        abort();
                    }
                } else {
                    lb_i->set_range_tombstone(lb_i->range_tombstone() + i_rt);
                }
                lb_i->set_continuous(true);
            }
        } else {
            SCYLLA_ASSERT(i->dummy() == src_e.dummy());
            alloc_strategy_unique_ptr<rows_entry> s1;
            alloc_strategy_unique_ptr<rows_entry> s2;

            if (next_interval_loaded) {
                // FIXME: Avoid reallocation
                s1 = alloc_strategy_unique_ptr<rows_entry>(
                        current_allocator().construct<rows_entry>(p_s,
                            position_in_partition::after_key(p_s, src_e.position()), is_dummy::yes, is_continuous::no));
                if (src_e.position().is_clustering_row()) {
                    s2 = alloc_strategy_unique_ptr<rows_entry>(
                            current_allocator().construct<rows_entry>(s, s1->position(), is_dummy::yes, is_continuous::yes));
                    auto next_i = std::next(i);
                    if (next_i != _rows.end() && next_i->continuous()) {
                        s2->set_range_tombstone(next_i->range_tombstone() + src_e.range_tombstone());
                    } else {
                        s2->set_range_tombstone(src_e.range_tombstone());
                    }
                }
            }

            {
                // FIXME: This can be an evictable snapshot even if !tracker, see partition_entry::squashed()
                // So we need to handle continuity as if it was an evictable snapshot.
                if (i->continuous()) {
                    if (src_e.range_tombstone() > i->range_tombstone()) {
                        // Cannot apply range tombstone in such a case.
                        // Should not occur in practice due to the "older versions are evicted first" rule.
                        if (!src_e.continuous()) {
                            // range tombstone on a discontinuous dummy does not matter
                            if (!src_e.dummy()) {
                                // See the "no singular tombstones" rule.
                                mplog.error("Cannot merge entry {} with rt={}, cont=0 into an entry which has rt={}, cont=1",
                                        src_e.position(), src_e.range_tombstone(), i->range_tombstone());
                                abort();
                            }
                        } else {
                            i->set_range_tombstone(i->range_tombstone() + src_e.range_tombstone());
                        }
                    }
                } else {
                    i->set_continuous(src_e.continuous());
                    i->set_range_tombstone(i->range_tombstone() + src_e.range_tombstone());
                }
            }
            if (tracker) {
                if (same_schema) [[likely]] {
                    // Newer evictable versions store complete rows
                    i->row() = std::move(src_e.row());
                } else {
                    i->apply_monotonically(s, p_s, std::move(src_e));
                }
                // Need to preserve the LRU link of the later version in case it's
                // the last dummy entry which holds the partition entry linked in LRU.
                i->swap(src_e);
                tracker->remove(src_e);
            } else {
                // Avoid row compaction if no newer range tombstone.
                do_compact = (src_e.range_tombstone() + src_e.row().deleted_at().regular()) >
                            (i->range_tombstone() + i->row().deleted_at().regular());
                memory::on_alloc_point();
                if (same_schema) [[likely]] {
                    i->apply_monotonically(s, std::move(src_e));
                } else {
                    i->apply_monotonically(s, p_s, std::move(src_e));
                }
            }
            ++app_stats.row_hits;
            p_i = p._rows.erase_and_dispose(p_i, del);
            lb_i = i;
            ++i;
            p_sentinel = std::move(s1);
            this_sentinel = std::move(s2);
        }
        // All operations above up to each insert_before() must be noexcept.
        if (prev_compacted && lb_i != _rows.begin()) {
            maybe_drop(s, tracker, std::prev(lb_i), app_stats);
        }
        if (lb_i->dummy()) {
            prev_compacted = true;
        } else if (do_compact) {
            compact(*lb_i);
            prev_compacted = true;
        } else {
            prev_compacted = false;
        }
        if (prev_compacted && !next_interval_loaded) {
            // next_p_i will not see prev_interval_loaded so will not attempt to drop predecessors.
            // We have to do it now.
            maybe_drop(s, tracker, lb_i, app_stats);
            lb_i = {};
        }
        ++app_stats.row_writes;

        // We must not return stop_iteration::no if we removed the last element from p._rows.
        // Otherwise, p_i will be left empty, and thus fully continuous, violating the
        // invariant that the sum of this and p has the same continuity as before merging.
        if (made_progress && need_preempt() && p_i != p._rows.end()) {
            return stop_iteration::no;
        }

        made_progress = true;
    }
    if (prev_compacted && lb_i != _rows.end()) {
        maybe_drop(s, tracker, lb_i, app_stats);
    }
    return stop_iteration::yes;
}

void
mutation_partition_v2::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    check_schema(schema);
    SCYLLA_ASSERT(!prefix.is_full(schema));
    auto start = prefix;
    apply_row_tombstone(schema, range_tombstone{std::move(start), std::move(prefix), std::move(t)});
}

void
mutation_partition_v2::apply_row_tombstone(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    mutation_partition mp(schema);
    mp.apply_row_tombstone(schema, std::move(rt));
    apply(schema, std::move(mp));
}

void
mutation_partition_v2::apply_delete(const schema& schema, const clustering_key_prefix& prefix, tombstone t) {
    check_schema(schema);
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, prefix).apply(t);
    } else {
        apply_row_tombstone(schema, prefix, t);
    }
}

void
mutation_partition_v2::apply_delete(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    if (range_tombstone::is_single_clustering_row_tombstone(schema, rt.start, rt.start_kind, rt.end, rt.end_kind)) {
        apply_delete(schema, std::move(rt.start), std::move(rt.tomb));
        return;
    }
    apply_row_tombstone(schema, std::move(rt));
}

void
mutation_partition_v2::apply_delete(const schema& schema, clustering_key&& prefix, tombstone t) {
    check_schema(schema);
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, std::move(prefix)).apply(t);
    } else {
        apply_row_tombstone(schema, std::move(prefix), t);
    }
}

void
mutation_partition_v2::apply_delete(const schema& schema, clustering_key_prefix_view prefix, tombstone t) {
    check_schema(schema);
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, prefix).apply(t);
    } else {
        apply_row_tombstone(schema, prefix, t);
    }
}

void
mutation_partition_v2::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(row_marker(created_at));
}
void mutation_partition_v2::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at,
        gc_clock::duration ttl, gc_clock::time_point expiry) {
    clustered_row(s, key).apply(row_marker(created_at, ttl, expiry));
}
void mutation_partition_v2::insert_row(const schema& s, const clustering_key& key, deletable_row&& row) {
    auto e = alloc_strategy_unique_ptr<rows_entry>(
        current_allocator().construct<rows_entry>(key, std::move(row)));
    _rows.insert_before_hint(_rows.end(), std::move(e), rows_entry::tri_compare(s));
}

void mutation_partition_v2::insert_row(const schema& s, const clustering_key& key, const deletable_row& row) {
    check_schema(s);
    auto e = alloc_strategy_unique_ptr<rows_entry>(
        current_allocator().construct<rows_entry>(s, key, row));
    _rows.insert_before_hint(_rows.end(), std::move(e), rows_entry::tri_compare(s));
}

const row*
mutation_partition_v2::find_row(const schema& s, const clustering_key& key) const {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, clustering_key&& key) {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(std::move(key)));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return i->row();
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, const clustering_key& key) {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(key));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return i->row();
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, clustering_key_view key) {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(key));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return i->row();
}

rows_entry&
mutation_partition_v2::clustered_rows_entry(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    check_schema(s);
    auto i = _rows.find(pos, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(s, pos, dummy, continuous));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return *i;
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    return clustered_rows_entry(s, pos, dummy, continuous).row();
}

rows_entry&
mutation_partition_v2::clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy) {
    check_schema(s);
    auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.lower_bound(pos, cmp);
    if (i == _rows.end() || cmp(i->position(), pos) != 0) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(s, pos, dummy, is_continuous::no));
        if (i != _rows.end()) {
            e->set_continuous(i->continuous());
            e->set_range_tombstone(i->range_tombstone());
        }
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return *i;
}

deletable_row&
mutation_partition_v2::append_clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    check_schema(s);
    const auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.end();
    if (!_rows.empty() && (cmp(*std::prev(i), pos) >= 0)) {
        throw std::runtime_error(format("mutation_partition_v2::append_clustered_row(): cannot append clustering row with key {} to the partition"
                ", last clustering row is equal or greater: {}", pos, std::prev(i)->key()));
    }
    auto e = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(s, pos, dummy, continuous));
    i = _rows.insert_before_hint(i, std::move(e), cmp).first;

    return i->row();
}

mutation_partition_v2::rows_type::const_iterator
mutation_partition_v2::lower_bound(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    if (!r.start()) {
        return std::cbegin(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_start(r), rows_entry::tri_compare(schema));
}

mutation_partition_v2::rows_type::const_iterator
mutation_partition_v2::upper_bound(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    if (!r.end()) {
        return std::cend(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_end(r), rows_entry::tri_compare(schema));
}

boost::iterator_range<mutation_partition_v2::rows_type::const_iterator>
mutation_partition_v2::range(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    return boost::make_iterator_range(lower_bound(schema, r), upper_bound(schema, r));
}

boost::iterator_range<mutation_partition_v2::rows_type::iterator>
mutation_partition_v2::range(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition_v2*>(this)->range(schema, r));
}

mutation_partition_v2::rows_type::iterator
mutation_partition_v2::lower_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition_v2*>(this)->lower_bound(schema, r));
}

mutation_partition_v2::rows_type::iterator
mutation_partition_v2::upper_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition_v2*>(this)->upper_bound(schema, r));
}

template<typename Func>
void mutation_partition_v2::for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const
{
    check_schema(schema);
    auto r = range(schema, row_range);
    if (!reversed) {
        for (const auto& e : r) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    } else {
        for (const auto& e : r | boost::adaptors::reversed) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    }
}

// Transforms given range of printable into a range of strings where each element
// in the original range is prefxied with given string.
template<typename RangeOfPrintable>
static auto prefixed(const sstring& prefix, const RangeOfPrintable& r) {
    return r | boost::adaptors::transformed([&] (auto&& e) { return format("{}{}", prefix, e); });
}

auto fmt::formatter<mutation_partition_v2::printer>::format(const mutation_partition_v2::printer& p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    const auto indent = "";

    auto& mp = p._mutation_partition;
    auto out = fmt::format_to(ctx.out(), "mutation_partition_v2: {{\n");
    if (mp._tombstone) {
        out = fmt::format_to(out, "{:2}tombstone: {},\n", indent, mp._tombstone);
    }

    if (!mp.static_row().empty()) {
        out = fmt::format_to(out, "{:2}static_row: {{\n", indent);
        const auto& srow = mp.static_row().get();
        srow.for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::static_column, c_id);
            out = fmt::format_to(out, "{:4}'{}': {},\n",
                       indent, column_def.name_as_text(), atomic_cell_or_collection::printer(column_def, cell));
        });
        out = fmt::format_to(out, "{:2}}},\n", indent);
    }

    out = fmt::format_to(out, "{:2}rows: [\n", indent);

    for (const auto& re : mp.clustered_rows()) {
        out = fmt::format_to(out, "{:4}{{\n", indent);

        const auto& row = re.row();
        out = fmt::format_to(out, "{:6}cont: {},\n", indent, re.continuous());
        out = fmt::format_to(out, "{:6}dummy: {},\n", indent, re.dummy());
        if (!row.marker().is_missing()) {
            out = fmt::format_to(out, "{:6}marker: {},\n", indent, row.marker());
        }
        if (row.deleted_at()) {
            out = fmt::format_to(out, "{:6}tombstone: {},\n", indent, row.deleted_at());
        }
        if (re.range_tombstone()) {
            out = fmt::format_to(out, "{:6}rt: {},\n", "", re.range_tombstone());
        }

        position_in_partition pip(re.position());
        if (pip.get_clustering_key_prefix()) {
            out = fmt::format_to(out, "{:6}position: {{\n", indent);

            auto ck = *pip.get_clustering_key_prefix();
            auto type_iterator = ck.get_compound_type(p._schema)->types().begin();
            auto column_iterator = p._schema.clustering_key_columns().begin();

            out = fmt::format_to(out, "{:8}bound_weight: {},\n",
                                 indent, int32_t(pip.get_bound_weight()));

            for (auto&& e : ck.components(p._schema)) {
                out = fmt::format_to(out, "{:8}'{}': {},\n",
                                     indent, column_iterator->name_as_text(),
                                     (*type_iterator)->to_string(to_bytes(e)));
                ++type_iterator;
                ++column_iterator;
            }

            out = fmt::format_to(out, "{:6}}},\n", indent);
        }

        row.cells().for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::regular_column, c_id);
            out = fmt::format_to(out, "{:6}'{}': {},\n",
                       indent, column_def.name_as_text(),
                       atomic_cell_or_collection::printer(column_def, cell));
        });

        out = fmt::format_to(out, "{:4}}},\n", indent);
    }

    return fmt::format_to(out, "{:2}]\n}}", indent);
}

bool mutation_partition_v2::equal(const schema& s, const mutation_partition_v2& p) const {
    return equal(s, p, s);
}

bool mutation_partition_v2::equal(const schema& this_schema, const mutation_partition_v2& p, const schema& p_schema) const {
#ifdef SEASTAR_DEBUG
    SCYLLA_ASSERT(_schema_version == this_schema.version());
    SCYLLA_ASSERT(p._schema_version == p_schema.version());
#endif
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!boost::equal(non_dummy_rows(), p.non_dummy_rows(),
        [&] (const rows_entry& e1, const rows_entry& e2) {
            return e1.equal(this_schema, e2, p_schema);
        }
    )) {
        return false;
    }

    return _static_row.equal(column_kind::static_column, this_schema, p._static_row, p_schema);
}

bool mutation_partition_v2::equal_continuity(const schema& s, const mutation_partition_v2& p) const {
    return _static_row_continuous == p._static_row_continuous
        && get_continuity(s).equals(s, p.get_continuity(s));
}

size_t mutation_partition_v2::external_memory_usage(const schema& s) const {
    check_schema(s);
    size_t sum = 0;
    sum += static_row().external_memory_usage(s, column_kind::static_column);
    sum += clustered_rows().external_memory_usage();
    for (auto& clr : clustered_rows()) {
        sum += clr.memory_usage(s);
    }

    return sum;
}

// Returns true if the mutation_partition_v2 represents no writes.
bool mutation_partition_v2::empty() const
{
    if (_tombstone.timestamp != api::missing_timestamp) {
        return false;
    }
    return !_static_row.size() && _rows.empty();
}

bool
mutation_partition_v2::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    check_schema(s);
    return has_any_live_data(s, column_kind::static_column, static_row().get(), _tombstone, query_time);
}

uint64_t
mutation_partition_v2::row_count() const {
    return _rows.calculate_size();
}

void mutation_partition_v2::accept(const schema& s, mutation_partition_visitor& v) const {
    check_schema(s);
    v.accept_partition_tombstone(_tombstone);
    _static_row.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        const column_definition& def = s.static_column_at(id);
        if (def.is_atomic()) {
            v.accept_static_cell(id, cell.as_atomic_cell(def));
        } else {
            v.accept_static_cell(id, cell.as_collection_mutation());
        }
    });
    std::optional<position_in_partition> prev_pos;
    for (const rows_entry& e : _rows) {
        const deletable_row& dr = e.row();
        if (e.range_tombstone()) {
            if (!e.continuous()) {
                v.accept_row_tombstone(range_tombstone(position_in_partition::before_key(e.position()),
                                                       position_in_partition::after_key(s, e.position()),
                                                       e.range_tombstone()));
            } else {
                v.accept_row_tombstone(range_tombstone(prev_pos ? position_in_partition::after_key(s, *prev_pos)
                                                                : position_in_partition::before_all_clustered_rows(),
                                                       position_in_partition::after_key(s, e.position()),
                                                       e.range_tombstone()));
            }
        }
        v.accept_row(e.position(), dr.deleted_at(), dr.marker(), e.dummy(), e.continuous());
        dr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& def = s.regular_column_at(id);
            if (def.is_atomic()) {
                v.accept_row_cell(id, cell.as_atomic_cell(def));
            } else {
                v.accept_row_cell(id, cell.as_collection_mutation());
            }
        });
        prev_pos = e.position();
    }
}

void
mutation_partition_v2::upgrade(const schema& old_schema, const schema& new_schema) {
    // We need to copy to provide strong exception guarantees.
    mutation_partition tmp(new_schema);
    tmp.set_static_row_continuous(_static_row_continuous);
    converting_mutation_partition_applier v(old_schema.get_column_mapping(), new_schema, tmp);
    accept(old_schema, v);
    *this = mutation_partition_v2(new_schema, std::move(tmp));
}

mutation_partition mutation_partition_v2::as_mutation_partition(const schema& s) const {
    mutation_partition tmp(s);
    tmp.set_static_row_continuous(_static_row_continuous);
    partition_builder v(s, tmp);
    accept(s, v);
    return tmp;
}

mutation_partition_v2::mutation_partition_v2(mutation_partition_v2::incomplete_tag, const schema& s, tombstone t)
    : _tombstone(t)
    , _static_row_continuous(!s.has_static_columns())
    , _rows()
#ifdef SEASTAR_DEBUG
    , _schema_version(s.version())
#endif
{
    auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(s, rows_entry::last_dummy_tag(), is_continuous::no));
    _rows.insert_before(_rows.end(), std::move(e));
}

bool mutation_partition_v2::is_fully_continuous() const {
    if (!_static_row_continuous) {
        return false;
    }
    for (auto&& row : _rows) {
        if (!row.continuous()) {
            return false;
        }
    }
    return true;
}

void mutation_partition_v2::make_fully_continuous() {
    _static_row_continuous = true;
    auto i = _rows.begin();
    while (i != _rows.end()) {
        i->set_continuous(true);
        ++i;
    }
}

void mutation_partition_v2::set_continuity(const schema& s, const position_range& pr, is_continuous cont) {
    auto cmp = rows_entry::tri_compare(s);

    if (cmp(pr.start(), pr.end()) >= 0) {
        return; // empty range
    }

    auto end = _rows.lower_bound(pr.end(), cmp);
    if (end == _rows.end() || cmp(pr.end(), end->position()) < 0) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, pr.end(), is_dummy::yes,
                    end == _rows.end() ? is_continuous::yes : end->continuous()));
        end = _rows.insert_before(end, std::move(e));
    }

    auto i = _rows.lower_bound(pr.start(), cmp);
    if (cmp(pr.start(), i->position()) < 0) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, pr.start(), is_dummy::yes, i->continuous()));
        i = _rows.insert_before(i, std::move(e));
    }

    SCYLLA_ASSERT(i != end);
    ++i;

    while (1) {
        i->set_continuous(cont);
        if (i == end) {
            break;
        }
        ++i;
    }
}

clustering_interval_set mutation_partition_v2::get_continuity(const schema& s, is_continuous cont) const {
    check_schema(s);
    clustering_interval_set result;
    auto i = _rows.begin();
    auto prev_pos = position_in_partition::before_all_clustered_rows();
    while (i != _rows.end()) {
        if (i->continuous() == cont) {
            result.add(s, position_range(std::move(prev_pos), position_in_partition::before_key(i->position())));
        }
        if (i->position().is_clustering_row() && cont) {
            result.add(s, position_range(position_in_partition::before_key(i->position()),
                                         position_in_partition::after_key(s, i->position())));
        }
        prev_pos = position_in_partition::after_key(s, i->position());
        ++i;
    }
    if (cont) {
        result.add(s, position_range(std::move(prev_pos), position_in_partition::after_all_clustered_rows()));
    }
    return result;
}

stop_iteration mutation_partition_v2::clear_gently(cache_tracker* tracker) noexcept {
    auto del = current_deleter<rows_entry>();
    auto i = _rows.begin();
    auto end = _rows.end();
    while (i != end) {
        if (tracker) {
            tracker->remove(*i);
        }
        i = _rows.erase_and_dispose(i, del);

        // The iterator comparison below is to not defer destruction of now empty
        // mutation_partition_v2 objects. Not doing this would cause eviction to leave garbage
        // versions behind unnecessarily.
        if (need_preempt() && i != end) {
            return stop_iteration::no;
        }
    }

    return stop_iteration::yes;
}

bool
mutation_partition_v2::check_continuity(const schema& s, const position_range& r, is_continuous cont) const {
    check_schema(s);
    auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.lower_bound(r.start(), cmp);
    auto end = _rows.lower_bound(r.end(), cmp);
    if (cmp(r.start(), r.end()) >= 0) {
        return bool(cont);
    }
    if (i != end) {
        if (no_clustering_row_between(s, r.start(), i->position())) {
            ++i;
        }
        while (i != end) {
            if (i->continuous() != cont) {
                return false;
            }
            ++i;
        }
        if (end != _rows.begin() && no_clustering_row_between(s, std::prev(end)->position(), r.end())) {
            return true;
        }
    }
    return (end == _rows.end() ? is_continuous::yes : end->continuous()) == cont;
}

bool
mutation_partition_v2::fully_continuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::yes);
}

bool
mutation_partition_v2::fully_discontinuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::no);
}

mutation_partition_v2::rows_type::iterator
mutation_partition_v2::maybe_drop(const schema& s,
      cache_tracker* tracker,
      mutation_partition_v2::rows_type::iterator i,
      mutation_application_stats& app_stats)
{
    rows_entry& e = *i;
    auto next_i = std::next(i);

    if (!e.row().empty() || e.is_last_dummy()) {
        return next_i;
    }

    // Pass only if continuity is the same on both sides and
    // range tombstones for the intervals are the same on both sides (if intervals are continuous).
    bool next_continuous = next_i == _rows.end() || next_i->continuous();
    if (e.continuous() && next_continuous) {
        tombstone next_range_tombstone = (next_i == _rows.end() ? tombstone{} : next_i->range_tombstone());
        if (e.range_tombstone() != next_range_tombstone) {
            return next_i;
        }
    } else if (!e.continuous() && !next_continuous) {
        if (!e.dummy() && e.range_tombstone()) {
            return next_i;
        }
    } else {
        return next_i;
    }

    ++app_stats.rows_dropped_by_tombstones; // FIXME: it's more general than that now

    auto del = current_deleter<rows_entry>();
    i = _rows.erase(i);
    if (tracker) {
        tracker->remove(e);
    }
    del(&e);
    return next_i;
}

void mutation_partition_v2::compact(const schema& s, cache_tracker* tracker) {
    mutation_application_stats stats;
    auto i = _rows.begin();
    rows_type::iterator prev_i;
    while (i != _rows.end()) {
        i->compact(s, _tombstone);
        if (prev_i) {
            // We cannot call maybe_drop() on i because the entry may become redundant
            // only after the next entry is compacted, e.g. when next entry's range tombstone is dropped.
            maybe_drop(s, tracker, prev_i, stats);
        }
        prev_i = i++;
    }
    if (prev_i) {
        maybe_drop(s, tracker, prev_i, stats);
    }
}

bool has_redundant_dummies(const mutation_partition_v2& p) {
    bool last_dummy = false;
    bool last_cont = false;
    tombstone last_rt;
    auto i = p.clustered_rows().begin();
    while (i != p.clustered_rows().end()) {
        const rows_entry& e = *i;
        if (last_dummy) {
            bool redundant = last_cont == bool(e.continuous()) && last_rt == e.range_tombstone();
            if (redundant) {
                return true;
            }
        }
        last_dummy = bool(e.dummy());
        last_rt = e.range_tombstone();
        last_cont = bool(e.continuous());
        ++i;
    }
    return false;
}
