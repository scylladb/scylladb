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
#include "atomic_cell_hash.hh"
#include "reversibly_mergeable.hh"
#include "mutation_fragment.hh"
#include "mutation_query.hh"
#include "service/priority_manager.hh"
#include "mutation_compactor.hh"
#include "counters.hh"
#include "row_cache.hh"
#include "view_info.hh"
#include "mutation_cleaner.hh"
#include <seastar/core/execution_stage.hh>
#include "types/map.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "utils/exceptions.hh"
#include "clustering_key_filter.hh"
#include "mutation_partition_view.hh"
#include "tombstone_gc.hh"
#include "utils/unconst.hh"

extern logging::logger mplog;

mutation_partition_v2::mutation_partition_v2(const schema& s, const mutation_partition_v2& x)
        : _tombstone(x._tombstone)
        , _static_row(s, column_kind::static_column, x._static_row)
        , _static_row_continuous(x._static_row_continuous)
        , _rows()
        , _row_tombstones(x._row_tombstones)
#ifdef SEASTAR_DEBUG
        , _schema_version(s.version())
#endif
{
#ifdef SEASTAR_DEBUG
    assert(x._schema_version == _schema_version);
#endif
    auto cloner = [&s] (const rows_entry* x) -> rows_entry* {
        return current_allocator().construct<rows_entry>(s, *x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
}

mutation_partition_v2::mutation_partition_v2(const mutation_partition_v2& x, const schema& schema,
        query::clustering_key_filter_ranges ck_ranges)
        : _tombstone(x._tombstone)
        , _static_row(schema, column_kind::static_column, x._static_row)
        , _static_row_continuous(x._static_row_continuous)
        , _rows()
        , _row_tombstones(x._row_tombstones, range_tombstone_list::copy_comparator_only())
#ifdef SEASTAR_DEBUG
        , _schema_version(schema.version())
#endif
{
#ifdef SEASTAR_DEBUG
    assert(x._schema_version == _schema_version);
#endif
    try {
        for(auto&& r : ck_ranges) {
            for (const rows_entry& e : x.range(schema, r)) {
                auto ce = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(schema, e));
                _rows.insert_before_hint(_rows.end(), std::move(ce), rows_entry::tri_compare(schema));
            }
            for (auto&& rt : x._row_tombstones.slice(schema, r)) {
                _row_tombstones.apply(schema, rt.tombstone());
            }
        }
    } catch (...) {
        _rows.clear_and_dispose(current_deleter<rows_entry>());
        throw;
    }
}

mutation_partition_v2::mutation_partition_v2(mutation_partition_v2&& x, const schema& schema,
    query::clustering_key_filter_ranges ck_ranges)
    : _tombstone(x._tombstone)
    , _static_row(std::move(x._static_row))
    , _static_row_continuous(x._static_row_continuous)
    , _rows(std::move(x._rows))
    , _row_tombstones(schema)
#ifdef SEASTAR_DEBUG
    , _schema_version(schema.version())
#endif
{
#ifdef SEASTAR_DEBUG
    assert(x._schema_version == _schema_version);
#endif
    {
        auto deleter = current_deleter<rows_entry>();
        auto it = _rows.begin();
        for (auto&& range : ck_ranges.ranges()) {
            _rows.erase_and_dispose(it, lower_bound(schema, range), deleter);
            it = upper_bound(schema, range);
        }
        _rows.erase_and_dispose(it, _rows.end(), deleter);
    }
    {
        for (auto&& range : ck_ranges.ranges()) {
            for (auto&& x_rt : x._row_tombstones.slice(schema, range)) {
                auto rt = x_rt.tombstone();
                rt.trim(schema,
                        position_in_partition_view::for_range_start(range),
                        position_in_partition_view::for_range_end(range));
                _row_tombstones.apply(schema, std::move(rt));
            }
        }
    }
}

mutation_partition_v2::mutation_partition_v2(const schema& s, mutation_partition&& x)
    : _tombstone(x.partition_tombstone())
    , _static_row(std::move(x.static_row()))
    , _static_row_continuous(x.static_row_continuous())
    , _rows(std::move(x.mutable_clustered_rows()))
    , _row_tombstones(std::move(x.mutable_row_tombstones()))
{ }

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

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, cache_tracker* tracker,
        mutation_application_stats& app_stats, is_preemptible preemptible, apply_resume& res) {
#ifdef SEASTAR_DEBUG
    assert(s.version() == _schema_version);
    assert(p._schema_version == _schema_version);
#endif
    _tombstone.apply(p._tombstone);
    _static_row.apply_monotonically(s, column_kind::static_column, std::move(p._static_row));
    _static_row_continuous |= p._static_row_continuous;

    rows_entry::tri_compare cmp(s);
    auto del = current_deleter<rows_entry>();

    // Compacts rows in [i, end) with the tombstone.
    // Erases entries which are left empty by compaction.
    // Does not affect continuity.
    auto apply_tombstone_to_rows = [&] (apply_resume::stage stage, tombstone tomb, rows_type::iterator i, rows_type::iterator end) -> stop_iteration {
        if (!preemptible) {
            // Compaction is attempted only in preemptible contexts because it can be expensive to perform and is not
            // necessary for correctness.
            return stop_iteration::yes;
        }

        while (i != end) {
            rows_entry& e = *i;
            can_gc_fn never_gc = [](tombstone) { return false; };

            ++app_stats.rows_compacted_with_tombstones;
            bool all_dead = e.dummy() || !e.row().compact_and_expire(s,
                                                                     tomb,
                                                                     gc_clock::time_point::min(),  // no TTL expiration
                                                                     never_gc,                     // no GC
                                                                     gc_clock::time_point::min()); // no GC

            auto next_i = std::next(i);
            bool inside_continuous_range = !tracker ||
                    (e.continuous() && (next_i != _rows.end() && next_i->continuous()));

            if (all_dead && e.row().empty() && inside_continuous_range) {
                ++app_stats.rows_dropped_by_tombstones;
                i = _rows.erase(i);
                if (tracker) {
                    tracker->remove(e);
                }
                del(&e);
            } else {
                i = next_i;
            }

            if (need_preempt() && i != end) {
                res = apply_resume(stage, i->position());
                return stop_iteration::no;
            }
        }
        return stop_iteration::yes;
    };

    if (res._stage <= apply_resume::stage::range_tombstone_compaction) {
        bool filtering_tombstones = res._stage == apply_resume::stage::range_tombstone_compaction;
        for (const range_tombstone_entry& rt : p._row_tombstones) {
            position_in_partition_view pos = rt.position();
            if (filtering_tombstones) {
                if (cmp(res._pos, rt.end_position()) >= 0) {
                    continue;
                }
                filtering_tombstones = false;
                if (cmp(res._pos, rt.position()) > 0) {
                    pos = res._pos;
                }
            }
            auto i = _rows.lower_bound(pos, cmp);
            if (i == _rows.end()) {
                break;
            }
            auto end = _rows.lower_bound(rt.end_position(), cmp);

            auto tomb = _tombstone;
            tomb.apply(rt.tombstone().tomb);

            if (apply_tombstone_to_rows(apply_resume::stage::range_tombstone_compaction, tomb, i, end) == stop_iteration::no) {
                return stop_iteration::no;
            }
        }
    }

    if (_row_tombstones.apply_monotonically(s, std::move(p._row_tombstones), preemptible) == stop_iteration::no) {
        res = apply_resume::merging_range_tombstones();
        return stop_iteration::no;
    }

    if (p._tombstone) {
        // p._tombstone is already applied to _tombstone
        rows_type::iterator i;
        if (res._stage == apply_resume::stage::partition_tombstone_compaction) {
            i = _rows.lower_bound(res._pos, cmp);
        } else {
            i = _rows.begin();
        }
        if (apply_tombstone_to_rows(apply_resume::stage::partition_tombstone_compaction,
                                               _tombstone, i, _rows.end()) == stop_iteration::no) {
            return stop_iteration::no;
        }
        // TODO: Drop redundant range tombstones
        p._tombstone = {};
    }

    res = apply_resume::merging_rows();

    auto p_i = p._rows.begin();
    auto i = _rows.begin();
    while (p_i != p._rows.end()) {
      try {
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
        if (miss) {
            bool insert = true;
            if (i != _rows.end() && i->continuous()) {
                // When falling into a continuous range, preserve continuity.
                src_e.set_continuous(true);

                if (src_e.dummy()) {
                    p_i = p._rows.erase(p_i);
                    if (tracker) {
                        tracker->remove(src_e);
                    }
                    del(&src_e);
                    insert = false;
                }
            }
            if (insert) {
                rows_type::key_grabber pi_kg(p_i);
                _rows.insert_before(i, std::move(pi_kg));
            }
        } else {
            auto continuous = i->continuous() || src_e.continuous();
            auto dummy = i->dummy() && src_e.dummy();
            i->set_continuous(continuous);
            i->set_dummy(dummy);
            // Clear continuity in the source first, so that in case of exception
            // we don't end up with the range up to src_e being marked as continuous,
            // violating exception guarantees.
            src_e.set_continuous(false);
            if (tracker) {
                // Newer evictable versions store complete rows
                i->replace_with(std::move(src_e));
                tracker->remove(src_e);
            } else {
                memory::on_alloc_point();
                i->apply_monotonically(s, std::move(src_e));
            }
            ++app_stats.row_hits;
            p_i = p._rows.erase_and_dispose(p_i, del);
        }
        ++app_stats.row_writes;
        if (preemptible && need_preempt() && p_i != p._rows.end()) {
            // We cannot leave p with the clustering range up to p_i->position()
            // marked as continuous because some of its sub-ranges may have originally been discontinuous.
            // This would result in the sum of this and p to have broader continuity after preemption,
            // also possibly violating the invariant of non-overlapping continuity between MVCC versions,
            // if that's what we're merging here.
            // It's always safe to mark the range as discontinuous.
            p_i->set_continuous(false);
            return stop_iteration::no;
        }
      } catch (...) {
          // We cannot leave p with the clustering range up to p_i->position()
          // marked as continuous because some of its sub-ranges may have originally been discontinuous.
          // This would result in the sum of this and p to have broader continuity after preemption,
          // also possibly violating the invariant of non-overlapping continuity between MVCC versions,
          // if that's what we're merging here.
          // It's always safe to mark the range as discontinuous.
          p_i->set_continuous(false);
          throw;
      }
    }
    return stop_iteration::yes;
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, const schema& p_schema,
        mutation_application_stats& app_stats, is_preemptible preemptible, apply_resume& res) {
    if (s.version() == p_schema.version()) {
        return apply_monotonically(s, std::move(p), no_cache_tracker, app_stats, preemptible, res);
    } else {
        mutation_partition_v2 p2(s, p);
        p2.upgrade(p_schema, s);
        return apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::no, res); // FIXME: make preemptible
    }
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, cache_tracker *tracker,
                                                       mutation_application_stats& app_stats) {
    apply_resume res;
    return apply_monotonically(s, std::move(p), tracker, app_stats, is_preemptible::no, res);
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, const schema& p_schema,
                                                       mutation_application_stats& app_stats) {
    apply_resume res;
    return apply_monotonically(s, std::move(p), p_schema, app_stats, is_preemptible::no, res);
}

void mutation_partition_v2::apply(const schema& s, const mutation_partition_v2& p, const schema& p_schema,
                               mutation_application_stats& app_stats) {
    apply_monotonically(s, mutation_partition_v2(p_schema, std::move(p)), p_schema, app_stats);
}

void mutation_partition_v2::apply(const schema& s, mutation_partition_v2&& p, mutation_application_stats& app_stats) {
    apply_monotonically(s, mutation_partition_v2(s, std::move(p)), no_cache_tracker, app_stats);
}

void
mutation_partition_v2::apply_weak(const schema& s, mutation_partition_view p,
                                  const schema& p_schema, mutation_application_stats& app_stats) {
    // FIXME: Optimize
    mutation_partition p2(p_schema.shared_from_this());
    partition_builder b(p_schema, p2);
    p.accept(p_schema, b);
    apply_monotonically(s, mutation_partition_v2(p_schema, std::move(p2)), p_schema, app_stats);
}

void mutation_partition_v2::apply_weak(const schema& s, const mutation_partition& p,
                                       const schema& p_schema, mutation_application_stats& app_stats) {
    // FIXME: Optimize
    apply_monotonically(s, mutation_partition_v2(s, p), p_schema, app_stats);
}

void mutation_partition_v2::apply_weak(const schema& s, mutation_partition&& p, mutation_application_stats& app_stats) {
    apply_monotonically(s, mutation_partition_v2(s, std::move(p)), no_cache_tracker, app_stats);
}

tombstone
mutation_partition_v2::range_tombstone_for_row(const schema& schema, const clustering_key& key) const {
    check_schema(schema);
    tombstone t = _tombstone;
    if (!_row_tombstones.empty()) {
        auto found = _row_tombstones.search_tombstone_covering(schema, key);
        t.apply(found);
    }
    return t;
}

row_tombstone
mutation_partition_v2::tombstone_for_row(const schema& schema, const clustering_key& key) const {
    check_schema(schema);
    row_tombstone t = row_tombstone(range_tombstone_for_row(schema, key));

    auto j = _rows.find(key, rows_entry::tri_compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().deleted_at(), j->row().marker());
    }

    return t;
}

row_tombstone
mutation_partition_v2::tombstone_for_row(const schema& schema, const rows_entry& e) const {
    check_schema(schema);
    row_tombstone t = e.row().deleted_at();
    t.apply(range_tombstone_for_row(schema, e.key()));
    return t;
}

void
mutation_partition_v2::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    check_schema(schema);
    assert(!prefix.is_full(schema));
    auto start = prefix;
    _row_tombstones.apply(schema, {std::move(start), std::move(prefix), std::move(t)});
}

void
mutation_partition_v2::apply_row_tombstone(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    _row_tombstones.apply(schema, std::move(rt));
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

std::ostream&
operator<<(std::ostream& os, const mutation_partition_v2::printer& p) {
    const auto indent = "  ";

    auto& mp = p._mutation_partition;
    os << "mutation_partition_v2: {\n";
    if (mp._tombstone) {
        os << indent << "tombstone: " << mp._tombstone << ",\n";
    }
    if (!mp._row_tombstones.empty()) {
        os << indent << "range_tombstones: {" << ::join(",", prefixed("\n    ", mp._row_tombstones)) << "},\n";
    }

    if (!mp.static_row().empty()) {
        os << indent << "static_row: {\n";
        const auto& srow = mp.static_row().get();
        srow.for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::static_column, c_id);
            os << indent << indent <<  "'" << column_def.name_as_text() 
               << "': " << atomic_cell_or_collection::printer(column_def, cell) << ",\n";
        }); 
        os << indent << "},\n";
    }

    os << indent << "rows: [\n";

    for (const auto& re : mp.clustered_rows()) {
        os << indent << indent << "{\n";

        const auto& row = re.row();
        os << indent << indent << indent << "cont: " << re.continuous() << ",\n";
        os << indent << indent << indent << "dummy: " << re.dummy() << ",\n";
        if (!row.marker().is_missing()) {
            os << indent << indent << indent << "marker: " << row.marker() << ",\n";
        }
        if (row.deleted_at()) {
            os << indent << indent << indent << "tombstone: " << row.deleted_at() << ",\n";
        }

        position_in_partition pip(re.position());
        if (pip.get_clustering_key_prefix()) {
            os << indent << indent << indent << "position: {\n";

            auto ck = *pip.get_clustering_key_prefix();
            auto type_iterator = ck.get_compound_type(p._schema)->types().begin();
            auto column_iterator = p._schema.clustering_key_columns().begin();

            os << indent << indent << indent << indent << "bound_weight: " << int32_t(pip.get_bound_weight()) << ",\n";

            for (auto&& e : ck.components(p._schema)) {
                os << indent << indent << indent << indent << "'" << column_iterator->name_as_text() 
                   << "': " << (*type_iterator)->to_string(to_bytes(e)) << ",\n";
                ++type_iterator;
                ++column_iterator;
            }

            os << indent << indent << indent << "},\n";
        }

        row.cells().for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::regular_column, c_id);
            os << indent << indent << indent <<  "'" << column_def.name_as_text() 
               << "': " << atomic_cell_or_collection::printer(column_def, cell) << ",\n";
        });

        os << indent << indent << "},\n";
    }

    os << indent << "]\n}";

    return os;
}

bool mutation_partition_v2::equal(const schema& s, const mutation_partition_v2& p) const {
    return equal(s, p, s);
}

bool mutation_partition_v2::equal(const schema& this_schema, const mutation_partition_v2& p, const schema& p_schema) const {
#ifdef SEASTAR_DEBUG
    assert(_schema_version == this_schema.version());
    assert(p._schema_version == p_schema.version());
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

    if (!std::equal(_row_tombstones.begin(), _row_tombstones.end(),
        p._row_tombstones.begin(), p._row_tombstones.end(),
        [&] (const auto& rt1, const auto& rt2) { return rt1.tombstone().equal(this_schema, rt2.tombstone()); }
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
    sum += row_tombstones().external_memory_usage(s);

    return sum;
}

// Returns true if the mutation_partition_v2 represents no writes.
bool mutation_partition_v2::empty() const
{
    if (_tombstone.timestamp != api::missing_timestamp) {
        return false;
    }
    return !_static_row.size() && _rows.empty() && _row_tombstones.empty();
}

bool
mutation_partition_v2::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    check_schema(s);
    return has_any_live_data(s, column_kind::static_column, static_row().get(), _tombstone, query_time);
}

uint64_t
mutation_partition_v2::live_row_count(const schema& s, gc_clock::time_point query_time) const {
    check_schema(s);
    uint64_t count = 0;

    for (const rows_entry& e : non_dummy_rows()) {
        tombstone base_tombstone = range_tombstone_for_row(s, e.key());
        if (e.row().is_live(s, column_kind::regular_column, base_tombstone, query_time)) {
            ++count;
        }
    }

    if (count == 0 && is_static_row_live(s, query_time)) {
        return 1;
    }

    return count;
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
    for (const auto& rt : _row_tombstones) {
        v.accept_row_tombstone(rt.tombstone());
    }
    for (const rows_entry& e : _rows) {
        const deletable_row& dr = e.row();
        v.accept_row(e.position(), dr.deleted_at(), dr.marker(), e.dummy(), e.continuous());
        dr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& def = s.regular_column_at(id);
            if (def.is_atomic()) {
                v.accept_row_cell(id, cell.as_atomic_cell(def));
            } else {
                v.accept_row_cell(id, cell.as_collection_mutation());
            }
        });
    }
}

void
mutation_partition_v2::upgrade(const schema& old_schema, const schema& new_schema) {
    // We need to copy to provide strong exception guarantees.
    mutation_partition tmp(new_schema.shared_from_this());
    tmp.set_static_row_continuous(_static_row_continuous);
    converting_mutation_partition_applier v(old_schema.get_column_mapping(), new_schema, tmp);
    accept(old_schema, v);
    *this = mutation_partition_v2(new_schema, std::move(tmp));
}

mutation_partition_v2::mutation_partition_v2(mutation_partition_v2::incomplete_tag, const schema& s, tombstone t)
    : _tombstone(t)
    , _static_row_continuous(!s.has_static_columns())
    , _rows()
    , _row_tombstones(s)
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
        if (i->dummy()) {
            i = _rows.erase_and_dispose(i, alloc_strategy_deleter<rows_entry>());
        } else {
            i->set_continuous(true);
            ++i;
        }
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

    assert(i != end);
    ++i;

    while (1) {
        i->set_continuous(cont);
        if (i == end) {
            break;
        }
        if (i->dummy()) {
            i = _rows.erase_and_dispose(i, alloc_strategy_deleter<rows_entry>());
        } else {
            ++i;
        }
    }
}

clustering_interval_set mutation_partition_v2::get_continuity(const schema& s, is_continuous cont) const {
    check_schema(s);
    clustering_interval_set result;
    auto i = _rows.begin();
    auto prev_pos = position_in_partition::before_all_clustered_rows();
    while (i != _rows.end()) {
        if (i->continuous() == cont) {
            result.add(s, position_range(std::move(prev_pos), position_in_partition(i->position())));
        }
        if (i->position().is_clustering_row() && bool(i->dummy()) == !bool(cont)) {
            result.add(s, position_range(position_in_partition(i->position()),
                position_in_partition::after_key(s, i->position().key())));
        }
        prev_pos = i->position().is_clustering_row()
            ? position_in_partition::after_key(s, i->position().key())
            : position_in_partition(i->position());
        ++i;
    }
    if (cont) {
        result.add(s, position_range(std::move(prev_pos), position_in_partition::after_all_clustered_rows()));
    }
    return result;
}

stop_iteration mutation_partition_v2::clear_gently(cache_tracker* tracker) noexcept {
    if (_row_tombstones.clear_gently() == stop_iteration::no) {
        return stop_iteration::no;
    }

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
