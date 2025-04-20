/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "mutation_partition.hh"
#include "clustering_interval_set.hh"
#include "converting_mutation_partition_applier.hh"
#include "partition_builder.hh"
#include "query-result-writer.hh"
#include "mutation_fragment.hh"
#include "mutation_query.hh"
#include "mutation_compactor.hh"
#include "counters.hh"
#include "db/row_cache.hh"
#include "timestamp.hh"
#include "view_info.hh"
#include "mutation_cleaner.hh"
#include <seastar/core/execution_stage.hh>
#include "types/map.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "clustering_key_filter.hh"
#include "mutation_partition_view.hh"
#include "tombstone_gc.hh"
#include "utils/assert.hh"
#include "utils/unconst.hh"
#include "mutation/async_utils.hh"

logging::logger mclog("mutation_compactor");
logging::logger mplog("mutation_partition");

mutation_partition::mutation_partition(const schema& s, const mutation_partition& x)
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
    SCYLLA_ASSERT(x._schema_version == _schema_version);
#endif
    auto cloner = [&s] (const rows_entry* x) -> rows_entry* {
        return current_allocator().construct<rows_entry>(s, *x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
}

mutation_partition::mutation_partition(const mutation_partition& x, const schema& schema,
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
    SCYLLA_ASSERT(x._schema_version == _schema_version);
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

mutation_partition::mutation_partition(mutation_partition&& x, const schema& schema,
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
    SCYLLA_ASSERT(x._schema_version == _schema_version);
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

mutation_partition::~mutation_partition() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
}

mutation_partition&
mutation_partition::operator=(mutation_partition&& x) noexcept {
    if (this != &x) {
        this->~mutation_partition();
        new (this) mutation_partition(std::move(x));
    }
    return *this;
}

void mutation_partition::ensure_last_dummy(const schema& s) {
    check_schema(s);
    if (_rows.empty() || !_rows.rbegin()->is_last_dummy()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, rows_entry::last_dummy_tag(), is_continuous::yes));
        _rows.insert_before(_rows.end(), std::move(e));
    }
}

struct mutation_fragment_applier {
    const schema& _s;
    mutation_partition& _mp;

    void operator()(tombstone t) {
        _mp.apply(t);
    }

    void operator()(range_tombstone rt) {
        _mp.apply_row_tombstone(_s, std::move(rt));
    }

    void operator()(const static_row& sr) {
        _mp.static_row().apply(_s, column_kind::static_column, sr.cells());
    }

    void operator()(partition_start ps) {
        _mp.apply(ps.partition_tombstone());
    }

    void operator()(partition_end ps) {
    }

    void operator()(const clustering_row& cr) {
        auto temp = clustering_row(_s, cr);
        auto& dr = _mp.clustered_row(_s, std::move(temp.key()));
        dr.apply(_s, std::move(temp).as_deletable_row());
    }
};

void
mutation_partition::apply(const schema& s, const mutation_fragment& mf) {
    check_schema(s);
    mutation_fragment_applier applier{s, *this};
    mf.visit(applier);
}

stop_iteration mutation_partition::apply_monotonically(const schema& s, mutation_partition&& p, cache_tracker* tracker,
        mutation_application_stats& app_stats, is_preemptible preemptible, apply_resume& res) {
#ifdef SEASTAR_DEBUG
    SCYLLA_ASSERT(s.version() == _schema_version);
    SCYLLA_ASSERT(p._schema_version == _schema_version);
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

void
mutation_partition::apply(const schema& s, mutation_partition_view p,
        const schema& p_schema, mutation_application_stats& app_stats) {
    // FIXME: Optimize
    mutation_partition p2(*this, copy_comparators_only{});
    partition_builder b(p_schema, p2);
    p.accept(p_schema, b);
    if (s.version() != p_schema.version()) {
        p2.upgrade(p_schema, s);
    }
    apply_resume res;
    apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::no, res);
}

void mutation_partition::apply(const schema& s, const mutation_partition& p,
        const schema& p_schema, mutation_application_stats& app_stats) {
    // FIXME: Optimize
    mutation_partition p2(p_schema, p);
    if (s.version() != p_schema.version()) {
        p2.upgrade(p_schema, s);
    }
    apply_resume res;
    apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::no, res);
}

void mutation_partition::apply(const schema& s, mutation_partition&& p, mutation_application_stats& app_stats) {
    apply_resume res;
    apply_monotonically(s, std::move(p), no_cache_tracker, app_stats, is_preemptible::no, res);
}

tombstone
mutation_partition::range_tombstone_for_row(const schema& schema, const clustering_key& key) const {
    check_schema(schema);
    tombstone t = _tombstone;
    if (!_row_tombstones.empty()) {
        auto found = _row_tombstones.search_tombstone_covering(schema, key);
        t.apply(found);
    }
    return t;
}

row_tombstone
mutation_partition::tombstone_for_row(const schema& schema, const clustering_key& key) const {
    check_schema(schema);
    row_tombstone t = row_tombstone(range_tombstone_for_row(schema, key));

    auto j = _rows.find(key, rows_entry::tri_compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().deleted_at(), j->row().marker());
    }

    return t;
}

row_tombstone
mutation_partition::tombstone_for_row(const schema& schema, const rows_entry& e) const {
    check_schema(schema);
    row_tombstone t = e.row().deleted_at();
    t.apply(range_tombstone_for_row(schema, e.key()));
    return t;
}

void
mutation_partition::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    check_schema(schema);
    SCYLLA_ASSERT(!prefix.is_full(schema));
    auto start = prefix;
    _row_tombstones.apply(schema, {std::move(start), std::move(prefix), std::move(t)});
}

void
mutation_partition::apply_row_tombstone(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    _row_tombstones.apply(schema, std::move(rt));
}

void
mutation_partition::apply_delete(const schema& schema, const clustering_key_prefix& prefix, tombstone t) {
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
mutation_partition::apply_delete(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    if (range_tombstone::is_single_clustering_row_tombstone(schema, rt.start, rt.start_kind, rt.end, rt.end_kind)) {
        apply_delete(schema, std::move(rt.start), std::move(rt.tomb));
        return;
    }
    apply_row_tombstone(schema, std::move(rt));
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key&& prefix, tombstone t) {
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
mutation_partition::apply_delete(const schema& schema, clustering_key_prefix_view prefix, tombstone t) {
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
mutation_partition::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(row_marker(created_at));
}
void mutation_partition::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at,
        gc_clock::duration ttl, gc_clock::time_point expiry) {
    clustered_row(s, key).apply(row_marker(created_at, ttl, expiry));
}
void mutation_partition::insert_row(const schema& s, const clustering_key& key, deletable_row&& row) {
    auto e = alloc_strategy_unique_ptr<rows_entry>(
        current_allocator().construct<rows_entry>(key, std::move(row)));
    _rows.insert_before_hint(_rows.end(), std::move(e), rows_entry::tri_compare(s));
}

void mutation_partition::insert_row(const schema& s, const clustering_key& key, const deletable_row& row) {
    check_schema(s);
    auto e = alloc_strategy_unique_ptr<rows_entry>(
        current_allocator().construct<rows_entry>(s, key, row));
    _rows.insert_before_hint(_rows.end(), std::move(e), rows_entry::tri_compare(s));
}

const row*
mutation_partition::find_row(const schema& s, const clustering_key& key) const {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, clustering_key&& key) {
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
mutation_partition::clustered_row(const schema& s, const clustering_key& key) {
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
mutation_partition::clustered_row(const schema& s, clustering_key_view key) {
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
mutation_partition::clustered_rows_entry(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
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
mutation_partition::clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    return clustered_rows_entry(s, pos, dummy, continuous).row();
}

deletable_row&
mutation_partition::append_clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    check_schema(s);
    const auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.end();
    if (!_rows.empty() && (cmp(*std::prev(i), pos) >= 0)) {
        on_internal_error(mplog, format("mutation_partition::append_clustered_row(): cannot append clustering row with key {} to the partition"
                ", last clustering row is equal or greater: {}", pos, std::prev(i)->position()));
    }
    auto e = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(s, pos, dummy, continuous));
    i = _rows.insert_before_hint(i, std::move(e), cmp).first;

    return i->row();
}

mutation_partition::rows_type::const_iterator
mutation_partition::lower_bound(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    if (!r.start()) {
        return std::cbegin(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_start(r), rows_entry::tri_compare(schema));
}

mutation_partition::rows_type::const_iterator
mutation_partition::upper_bound(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    if (!r.end()) {
        return std::cend(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_end(r), rows_entry::tri_compare(schema));
}

std::ranges::subrange<mutation_partition::rows_type::const_iterator>
mutation_partition::range(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    return std::ranges::subrange(lower_bound(schema, r), upper_bound(schema, r));
}

std::ranges::subrange<mutation_partition::rows_type::iterator>
mutation_partition::range(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->range(schema, r));
}

mutation_partition::rows_type::iterator
mutation_partition::lower_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->lower_bound(schema, r));
}

mutation_partition::rows_type::iterator
mutation_partition::upper_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->upper_bound(schema, r));
}

template<typename Func>
void mutation_partition::for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const
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
        for (const auto& e : r | std::views::reverse) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    }
}

template<typename RowWriter>
void write_cell(RowWriter& w, const query::partition_slice& slice, ::atomic_cell_view c) {
    SCYLLA_ASSERT(c.is_live());
    auto wr = w.add().write();
    auto after_timestamp = [&, wr = std::move(wr)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_timestamp>()) {
            return std::move(wr).write_timestamp(c.timestamp());
        } else {
            return std::move(wr).skip_timestamp();
        }
    }();
    auto after_value = [&, wr = std::move(after_timestamp)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_expiry>() && c.is_live_and_has_ttl()) {
            return std::move(wr).write_expiry(c.expiry());
        } else {
            return std::move(wr).skip_expiry();
        }
    }().write_fragmented_value(fragment_range(c.value()));
    [&, wr = std::move(after_value)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_ttl>() && c.is_live_and_has_ttl()) {
            return std::move(wr).write_ttl(c.ttl());
        } else {
            return std::move(wr).skip_ttl();
        }
    }().end_qr_cell();
}

template<typename RowWriter>
void write_cell(RowWriter& w, const query::partition_slice& slice, data_type type, collection_mutation_view v) {
    if (type->is_collection() && slice.options.contains<query::partition_slice::option::collections_as_maps>()) {
        auto& ctype = static_cast<const collection_type_impl&>(*type);
        type = map_type_impl::get_instance(ctype.name_comparator(), ctype.value_comparator(), true);
    }

    w.add().write().skip_timestamp()
        .skip_expiry()
        .write_fragmented_value(serialize_for_cql(*type, std::move(v)))
        .skip_ttl()
        .end_qr_cell();
}

template<typename RowWriter>
void write_counter_cell(RowWriter& w, const query::partition_slice& slice, ::atomic_cell_view c) {
    SCYLLA_ASSERT(c.is_live());
    auto ccv = counter_cell_view(c);
    auto wr = w.add().write();
    [&, wr = std::move(wr)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_timestamp>()) {
            return std::move(wr).write_timestamp(c.timestamp());
        } else {
            return std::move(wr).skip_timestamp();
        }
    }().skip_expiry()
            .write_value(counter_cell_view::total_value_type()->decompose(ccv.total_value()))
            .skip_ttl()
            .end_qr_cell();
}

template<typename Hasher>
void appending_hash<row>::operator()(Hasher& h, const row& cells, const schema& s, column_kind kind, const query::column_id_vector& columns, max_timestamp& max_ts) const {
    for (auto id : columns) {
        const cell_and_hash* cell_and_hash = cells.find_cell_and_hash(id);
        if (!cell_and_hash) {
            feed_hash(h, appending_hash<row>::null_hash_value);
            continue;
        }
        auto&& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            max_ts.update(cell_and_hash->cell.as_atomic_cell(def).timestamp());
            if constexpr (query::using_hash_of_hash_v<Hasher>) {
                if (cell_and_hash->hash) {
                    feed_hash(h, *cell_and_hash->hash);
                } else {
                    Hasher cellh;
                    feed_hash(cellh, cell_and_hash->cell.as_atomic_cell(def), def);
                    feed_hash(h, cellh.finalize_uint64());
                }
            } else {
                feed_hash(h, cell_and_hash->cell.as_atomic_cell(def), def);
            }
        } else {
            auto cm = cell_and_hash->cell.as_collection_mutation();
            max_ts.update(cm.last_update(*def.type));
            if constexpr (query::using_hash_of_hash_v<Hasher>) {
                if (cell_and_hash->hash) {
                    feed_hash(h, *cell_and_hash->hash);
                } else {
                    Hasher cellh;
                    feed_hash(cellh, cm, def);
                    feed_hash(h, cellh.finalize_uint64());
                }
            } else {
                feed_hash(h, cm, def);
            }
        }
    }
}
// Instantiation for mutation_test.cc
template void appending_hash<row>::operator()<xx_hasher>(xx_hasher& h, const row& cells, const schema& s, column_kind kind, const query::column_id_vector& columns, max_timestamp& max_ts) const;

cell_hash_opt row::cell_hash_for(column_id id) const {
    const cell_and_hash* cah = _cells.get(id);
    return cah != nullptr ? cah->hash : cell_hash_opt();
}

void row::prepare_hash(const schema& s, column_kind kind) const {
    // const to avoid removing const qualifiers on the read path
    for_each_cell([&s, kind] (column_id id, const cell_and_hash& c_a_h) {
        if (!c_a_h.hash) {
            query::default_hasher cellh;
            feed_hash(cellh, c_a_h.cell, s.column_at(kind, id));
            c_a_h.hash = cell_hash{cellh.finalize_uint64()};
        }
    });
}

void row::clear_hash() const {
    for_each_cell([] (column_id, const cell_and_hash& c_a_h) {
        c_a_h.hash = { };
    });
}

template<typename RowWriter>
static void get_compacted_row_slice(const schema& s,
    const query::partition_slice& slice,
    column_kind kind,
    const row& cells,
    const query::column_id_vector& columns,
    RowWriter& writer)
{
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            writer.add().skip();
        } else {
            auto&& def = s.column_at(kind, id);
            if (def.is_atomic()) {
                auto c = cell->as_atomic_cell(def);
                if (!c.is_live()) {
                    writer.add().skip();
                } else if (def.is_counter()) {
                    write_counter_cell(writer, slice, cell->as_atomic_cell(def));
                } else {
                    write_cell(writer, slice, cell->as_atomic_cell(def));
                }
            } else {
                auto mut = cell->as_collection_mutation();
                if (!mut.is_any_live(*def.type)) {
                    writer.add().skip();
                } else {
                    write_cell(writer, slice, def.type, std::move(mut));
                }
            }
        }
    }
}

bool has_any_live_data(const schema& s, column_kind kind, const row& cells, tombstone tomb, gc_clock::time_point now) {
    bool any_live = false;
    cells.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell_or_collection) {
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            auto&& c = cell_or_collection.as_atomic_cell(def);
            if (c.is_live(tomb, now, def.is_counter())) {
                any_live = true;
                return stop_iteration::yes;
            }
        } else {
            auto mut = cell_or_collection.as_collection_mutation();
            if (mut.is_any_live(*def.type, tomb, now)) {
                any_live = true;
                return stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    });
    return any_live;
}

std::ostream&
operator<<(std::ostream& os, const std::pair<column_id, const atomic_cell_or_collection::printer&>& c) {
    fmt::print(os, "{{column: {} {}}}", c.first, c.second);
    return os;
}

// Transforms given range of printable into a range of strings where each element
// in the original range is prefxied with given string.
template<typename RangeOfPrintable>
static auto prefixed(const sstring& prefix, const RangeOfPrintable& r) {
    return r | std::views::transform([&] (auto&& e) { return format("{}{}", prefix, e); });
}

std::ostream&
operator<<(std::ostream& os, const row::printer& p) {
    auto& cells = p._row._cells;

    os << "{{row:";
    cells.walk([&] (column_id id, const cell_and_hash& cah) {
        auto& cdef = p._schema.column_at(p._kind, id);
        fmt::print(os, "\n    {}{}", cdef.name_as_text(), atomic_cell_or_collection::printer(cdef, cah.cell));
        return true;
    });
    return os << "}}";
}

std::ostream&
operator<<(std::ostream& os, const row_marker& rm) {
    if (rm.is_missing()) {
        fmt::print(os, "{{row_marker: }}");
    } else if (rm._ttl == row_marker::dead) {
        fmt::print(os, "{{row_marker: dead {} {}}}", rm._timestamp, rm._expiry.time_since_epoch().count());
    } else {
        fmt::print(os, "{{row_marker: {} {} {}}}", rm._timestamp, rm._ttl.count(),
            rm._ttl != row_marker::no_ttl ? rm._expiry.time_since_epoch().count() : 0);
    }
    return os;
}

std::ostream&
operator<<(std::ostream& os, const deletable_row::printer& p) {
    auto& dr = p._deletable_row;
    os << "{deletable_row: ";
    if (!dr._marker.is_missing()) {
        os << dr._marker << " ";
    }
    if (dr._deleted_at) {
        os << dr._deleted_at << " ";
    }
    return os << row::printer(p._schema, column_kind::regular_column, dr._cells) << "}";
}

std::ostream&
operator<<(std::ostream& os, const rows_entry::printer& p) {
    auto& re = p._rows_entry;
    fmt::print(os, "{{rows_entry: cont={} dummy={} {} {}}}", re.continuous(), re.dummy(),
                  position_in_partition_view::printer(p._schema, re.position()),
                  deletable_row::printer(p._schema, re._row));
    return os;
}

auto fmt::formatter<mutation_partition::printer>::format(const mutation_partition::printer& p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    constexpr auto indent = "  ";

    auto out = ctx.out();
    auto& mp = p._mutation_partition;
    out = fmt::format_to(out, "mutation_partition: {{\n");
    if (mp._tombstone) {
        out = fmt::format_to(out, "{}tombstone: {},\n", indent, mp._tombstone);
    }
    if (!mp._row_tombstones.empty()) {
        out = fmt::format_to(out, "{}range_tombstones: {{{}}},\n", indent, fmt::join(prefixed("\n    ", mp._row_tombstones), ","));
    }

    if (!mp.static_row().empty()) {
        out = fmt::format_to(out, "{}static_row: {{\n", indent);
        const auto& srow = mp.static_row().get();
        srow.for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::static_column, c_id);
            out = fmt::format_to(out, "{}{}'{}':{},\n",
                                 indent, indent, column_def.name_as_text(),
                                 atomic_cell_or_collection::printer(column_def, cell));
        });
        out = fmt::format_to(out, "{}}},\n", indent);
    }

    out = fmt::format_to(out, "{}rows: [\n", indent);

    for (const auto& re : mp.clustered_rows()) {
        out = fmt::format_to(out, "{}{}{{\n", indent, indent);

        const auto& row = re.row();
        out = fmt::format_to(out, "{}{}{}cont: {},\n", indent, indent, indent,
                             re.continuous());
        out = fmt::format_to(out, "{}{}{}dummy: {},\n", indent, indent, indent,
                             re.dummy());
        if (!row.marker().is_missing()) {
            out = fmt::format_to(out, "{}{}{}marker: {},\n", indent, indent, indent,
                                 row.marker());
        }
        if (row.deleted_at()) {
            out = fmt::format_to(out, "{}{}{}tombstone: {},\n", indent, indent, indent,
                                 row.deleted_at());
        }

        position_in_partition pip(re.position());
        if (pip.get_clustering_key_prefix()) {
            out = fmt::format_to(out, "{}{}{}position: {{\n", indent, indent, indent);

            auto ck = *pip.get_clustering_key_prefix();
            auto type_iterator = ck.get_compound_type(p._schema)->types().begin();
            auto column_iterator = p._schema.clustering_key_columns().begin();

            out = fmt::format_to(out, "{}{}{}{}bound_weight: {},\n", indent, indent, indent, indent,
                                 int32_t(pip.get_bound_weight()));

            for (auto&& e : ck.components(p._schema)) {
                out = fmt::format_to(out, "{}{}{}{}'{}': {},\n", indent, indent, indent, indent,
                                     column_iterator->name_as_text(),
                                     (*type_iterator)->to_string(to_bytes(e)));
                ++type_iterator;
                ++column_iterator;
            }

            out = fmt::format_to(out, "{}{}{}}},\n", indent, indent, indent);
        }

        row.cells().for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::regular_column, c_id);
            out = fmt::format_to(out, "{}{}{}'{}': {},\n", indent, indent, indent,
                           column_def.name_as_text(),
                           atomic_cell_or_collection::printer(column_def, cell));
        });

        out = fmt::format_to(out, "{}{}}},\n", indent, indent);
    }

    return fmt::format_to(out, "{}]\n}}", indent);
}

constexpr gc_clock::duration row_marker::no_ttl;
constexpr gc_clock::duration row_marker::dead;

// Note: the ordering algorithm for rows is the same as for cells,
// except that there is no cell value to break a tie in case all other attributes are equal.
// See compare_atomic_cell_for_merge.
int compare_row_marker_for_merge(const row_marker& left, const row_marker& right) noexcept {
    // Largest write timestamp wins.
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    // Tombstones always win reconciliation with live rows of the same timestamp
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        // Prefer expiring rows (which will become tombstones at some future date) over live rows.
        // See https://issues.apache.org/jira/browse/CASSANDRA-14592
        if (left.is_expiring() != right.is_expiring()) {
            // prefer expiring cells.
            return left.is_expiring() ? 1 : -1;
        }
        // If both are expiring, choose the cell with the latest expiry or derived write time.
        if (left.is_expiring()) {
            if (left.expiry() != right.expiry()) {
                return left.expiry() < right.expiry() ? -1 : 1;
            } else if (left.ttl() != right.ttl()) {
                // The cell write time is derived by (expiry - ttl).
                // Prefer row that was written later (and has a smaller ttl).
                return left.ttl() < right.ttl() ? 1 : -1;
            }
        }
    } else {
        // Both are either deleted or missing
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint64_t) left.deletion_time().time_since_epoch().count()
                   < (uint64_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

bool
deletable_row::equal(column_kind kind, const schema& s, const deletable_row& other, const schema& other_schema) const {
    if (_deleted_at != other._deleted_at || _marker != other._marker) {
        return false;
    }
    return _cells.equal(kind, s, other._cells, other_schema);
}

void deletable_row::apply(const schema& s, deletable_row&& src) {
    apply_monotonically(s, std::move(src));
}

void deletable_row::apply(const schema& s, const deletable_row& src) {
    apply_monotonically(s, src);
}

void deletable_row::apply(const schema& our_schema, const schema& their_schema, deletable_row&& src) {
    apply_monotonically(our_schema, their_schema, std::move(src));
}

void deletable_row::apply(const schema& our_schema, const schema& their_schema, const deletable_row& src) {
    apply_monotonically(our_schema, their_schema, src);
}

void deletable_row::apply_monotonically(const schema& s, deletable_row&& src) {
    _cells.apply_monotonically(s, column_kind::regular_column, std::move(src._cells));
    _marker.apply(src._marker);
    _deleted_at.apply(src._deleted_at, _marker);
}

void deletable_row::apply_monotonically(const schema& s, const deletable_row& src) {
    _cells.apply_monotonically(s, column_kind::regular_column, src._cells);
    _marker.apply(src._marker);
    _deleted_at.apply(src._deleted_at, _marker);
}

void deletable_row::apply_monotonically(const schema& our_schema, const schema& their_schema, deletable_row&& src) {
    _cells.apply_monotonically(our_schema, their_schema, column_kind::regular_column, std::move(src._cells));
    _marker.apply(src._marker);
    _deleted_at.apply(src._deleted_at, _marker);
}

void deletable_row::apply_monotonically(const schema& our_schema, const schema& their_schema, const deletable_row& src) {
    _cells.apply_monotonically(our_schema, their_schema, column_kind::regular_column, src._cells);
    _marker.apply(src._marker);
    _deleted_at.apply(src._deleted_at, _marker);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other) const {
    return equal(s, other, s);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other, const schema& other_schema) const {
    position_in_partition::equal_compare eq(s);
    return eq(position(), other.position())
           && _range_tombstone == other._range_tombstone
           && row().equal(column_kind::regular_column, s, other.row(), other_schema);
}

bool mutation_partition::equal(const schema& s, const mutation_partition& p) const {
    return equal(s, p, s);
}

bool mutation_partition::equal(const schema& this_schema, const mutation_partition& p, const schema& p_schema) const {
#ifdef SEASTAR_DEBUG
    SCYLLA_ASSERT(_schema_version == this_schema.version());
    SCYLLA_ASSERT(p._schema_version == p_schema.version());
#endif
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!std::ranges::equal(non_dummy_rows(), p.non_dummy_rows(),
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

bool mutation_partition::equal_continuity(const schema& s, const mutation_partition& p) const {
    return _static_row_continuous == p._static_row_continuous
        && get_continuity(s).equals(s, p.get_continuity(s));
}

mutation_partition mutation_partition::sliced(const schema& s, const query::clustering_row_ranges& ranges) const {
    auto p = mutation_partition(*this, s, ranges);
    p._row_tombstones.trim(s, ranges);
    return p;
}

static
void
apply_monotonically(const column_definition& def, cell_and_hash& dst,
                    atomic_cell_or_collection& src, cell_hash_opt src_hash) {
    if (def.is_atomic()) {
        if (def.is_counter()) {
            counter_cell_view::apply(def, dst.cell, src); // FIXME: Optimize
            dst.hash = { };
        } else if (compare_atomic_cell_for_merge(dst.cell.as_atomic_cell(def), src.as_atomic_cell(def)) < 0) {
            using std::swap;
            swap(dst.cell, src);
            dst.hash = std::move(src_hash);
        }
    } else {
        dst.cell = merge(*def.type, dst.cell.as_collection_mutation(), src.as_collection_mutation());
        dst.hash = { };
    }
}

void
row::apply(const column_definition& column, const atomic_cell_or_collection& value, cell_hash_opt hash) {
    auto tmp = value.copy(*column.type);
    apply_monotonically(column, std::move(tmp), std::move(hash));
}

void
row::apply(const column_definition& column, atomic_cell_or_collection&& value, cell_hash_opt hash) {
    apply_monotonically(column, std::move(value), std::move(hash));
}

template<typename Func>
void row::consume_with(Func&& func) {
    _cells.weed([func, this] (column_id id, cell_and_hash& cah) {
        func(id, cah);
        _size--;
        return true;
    });
}

void
row::apply_monotonically(const column_definition& column, atomic_cell_or_collection&& value, cell_hash_opt hash) {
    static_assert(std::is_nothrow_move_constructible<atomic_cell_or_collection>::value
                  && std::is_nothrow_move_assignable<atomic_cell_or_collection>::value,
                  "noexcept required for atomicity");

    // our mutations are not yet immutable
    auto id = column.id;

    cell_and_hash* cah = _cells.get(id);
    if (cah == nullptr) {
        // FIXME -- add .locate method to radix_tree to find or allocate a spot
        _cells.emplace(id, std::move(value), std::move(hash));
        _size++;
    } else {
        ::apply_monotonically(column, *cah, value, std::move(hash));
    }
}

void
row::append_cell(column_id id, atomic_cell_or_collection value) {
    _cells.emplace(id, std::move(value), cell_hash_opt());
    _size++;
}

const cell_and_hash*
row::find_cell_and_hash(column_id id) const {
    return _cells.get(id);
}

const atomic_cell_or_collection*
row::find_cell(column_id id) const {
    auto c_a_h = find_cell_and_hash(id);
    return c_a_h ? &c_a_h->cell : nullptr;
}

size_t row::external_memory_usage(const schema& s, column_kind kind) const {
    return _cells.memory_usage([&] (column_id id, const cell_and_hash& cah) noexcept {
            auto& cdef = s.column_at(kind, id);
            return cah.cell.external_memory_usage(*cdef.type);
    });
}

size_t rows_entry::memory_usage(const schema& s) const {
    size_t size = 0;
    if (!dummy()) {
        size += key().external_memory_usage();
    }
    return size +
           row().cells().external_memory_usage(s, column_kind::regular_column) +
           sizeof(rows_entry);
}

size_t mutation_partition::external_memory_usage(const schema& s) const {
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

template<typename Func>
requires std::is_invocable_r_v<stop_iteration, Func, rows_entry&>
void mutation_partition::trim_rows(const schema& s,
    const std::vector<query::clustering_range>& row_ranges,
    Func&& func)
{
    check_schema(s);

    stop_iteration stop = stop_iteration::no;
    auto last = _rows.begin();
    auto deleter = current_deleter<rows_entry>();

    auto range_begin = [this, &s] (const query::clustering_range& range) {
        return lower_bound(s, range);
    };

    auto range_end = [this, &s] (const query::clustering_range& range) {
        return upper_bound(s, range);
    };

    for (auto&& row_range : row_ranges) {
        if (stop) {
            break;
        }

        last = _rows.erase_and_dispose(last, range_begin(row_range), deleter);
        auto end = range_end(row_range);
        while (last != end && !stop) {
            rows_entry& e = *last;
            stop = func(e);
            if (e.empty()) {
                last = _rows.erase_and_dispose(last, deleter);
            } else {
                ++last;
            }
        }
    }

    last = _rows.erase_and_dispose(last, _rows.end(), deleter);
}

uint32_t mutation_partition::do_compact(const schema& s,
    const dht::decorated_key& dk,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    bool always_return_static_content,
    uint64_t row_limit,
    can_gc_fn& can_gc,
    bool drop_tombstones_unconditionally,
    const tombstone_gc_state& gc_state)
{
    check_schema(s);
    SCYLLA_ASSERT(row_limit > 0);

    auto gc_before = drop_tombstones_unconditionally ? gc_clock::time_point::max() :
        gc_state.get_gc_before_for_key(s.shared_from_this(), dk, query_time);

    auto should_purge_tombstone = [&] (const tombstone& t, is_shadowable is_shadowable) {
        return t.deletion_time < gc_before && can_gc(t, is_shadowable);
    };

    bool static_row_live = _static_row.compact_and_expire(s, column_kind::static_column, row_tombstone(_tombstone),
        query_time, can_gc, gc_before);

    uint64_t row_count = 0;

    auto row_callback = [&] (rows_entry& e) {
        if (e.dummy()) {
            return stop_iteration::no;
        }
        deletable_row& row = e.row();
        tombstone tomb = range_tombstone_for_row(s, e.key());
        bool is_live = row.compact_and_expire(s, tomb, query_time, can_gc, gc_before, nullptr);
        return stop_iteration(is_live && ++row_count == row_limit);
    };

    trim_rows(s, row_ranges, row_callback);

    // #589 - Do not add extra row for static content unless we did a CK range-less query.
    // See comment in query
    bool return_static_content_on_partition_with_no_rows = always_return_static_content || !has_ck_selector(row_ranges);
    if (row_count == 0 && static_row_live && return_static_content_on_partition_with_no_rows) {
        ++row_count;
    }

    _row_tombstones.erase_where([&] (auto&& rt) {
        // Only row tombstones can be shadowable, range tombstones aren't
        return should_purge_tombstone(rt.tomb, is_shadowable::no) || rt.tomb <= _tombstone;
    });
    // The partition tombstone is never shadowable
    if (should_purge_tombstone(_tombstone, is_shadowable::no)) {
        _tombstone = tombstone();
    }

    // FIXME: purge unneeded prefix tombstones based on row_ranges

    return row_count;
}

uint64_t
mutation_partition::compact_for_query(
    const schema& s,
    const dht::decorated_key& dk,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    bool always_return_static_content,
    uint64_t row_limit)
{
    check_schema(s);
    bool drop_tombstones_unconditionally = false;
    // Replicas should only send non-purgeable tombstones already,
    // so we can expect to not have to actually purge any tombstones here.
    return do_compact(s, dk, query_time, row_ranges, always_return_static_content, row_limit, always_gc, drop_tombstones_unconditionally, tombstone_gc_state(nullptr));
}

void mutation_partition::compact_for_compaction(const schema& s,
    can_gc_fn& can_gc, const dht::decorated_key& dk, gc_clock::time_point compaction_time,
    const tombstone_gc_state& gc_state)
{
    check_schema(s);
    static const std::vector<query::clustering_range> all_rows = {
        query::clustering_range::make_open_ended_both_sides()
    };

    bool drop_tombstones_unconditionally = false;
    do_compact(s, dk, compaction_time, all_rows, true, query::partition_max_rows, can_gc, drop_tombstones_unconditionally, gc_state);
}

void mutation_partition::compact_for_compaction_drop_tombstones_unconditionally(const schema& s, const dht::decorated_key& dk)
{
    check_schema(s);
    static const std::vector<query::clustering_range> all_rows = {
        query::clustering_range::make_open_ended_both_sides()
    };
    bool drop_tombstones_unconditionally = true;
    auto compaction_time = gc_clock::time_point::max();
    do_compact(s, dk, compaction_time, all_rows, true, query::partition_max_rows, always_gc, drop_tombstones_unconditionally, tombstone_gc_state(nullptr));
}

// Returns true if the mutation_partition represents no writes.
bool mutation_partition::empty() const
{
    if (_tombstone.timestamp != api::missing_timestamp) {
        return false;
    }
    return !_static_row.size() && _rows.empty() && _row_tombstones.empty();
}

bool
deletable_row::is_live(const schema& s, column_kind kind, tombstone base_tombstone, gc_clock::time_point query_time) const {
    // _created_at corresponds to the row marker cell, present for rows
    // created with the 'insert' statement. If row marker is live, we know the
    // row is live. Otherwise, a row is considered live if it has any cell
    // which is live.
    base_tombstone.apply(_deleted_at.tomb());
    return _marker.is_live(base_tombstone, query_time) || _cells.is_live(s, kind, base_tombstone, query_time);
}

bool
row::is_live(const schema& s, column_kind kind, tombstone base_tombstone, gc_clock::time_point query_time) const {
    return has_any_live_data(s, kind, *this, base_tombstone, query_time);
}

bool
mutation_partition::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    check_schema(s);
    return has_any_live_data(s, column_kind::static_column, static_row().get(), _tombstone, query_time);
}

uint64_t
mutation_partition::live_row_count(const schema& s, gc_clock::time_point query_time) const {
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
mutation_partition::row_count() const {
    return _rows.calculate_size();
}

rows_entry::rows_entry(rows_entry&& o) noexcept
    : evictable(std::move(o))
    , _link(std::move(o._link))
    , _key(std::move(o._key))
    , _row(std::move(o._row))
    , _range_tombstone(std::move(o._range_tombstone))
    , _flags(std::move(o._flags))
{
}

void rows_entry::compact(const schema& s, tombstone t) {
    row().compact_and_expire(s,
                             t + _range_tombstone,
                             gc_clock::time_point::min(),  // no TTL expiration
                             never_gc,                     // no GC
                             gc_clock::time_point::min()); // no GC
    // FIXME: Purge redundant _range_tombstone
}

void rows_entry::replace_with(rows_entry&& o) noexcept {
    swap(o);
    _range_tombstone = std::move(o._range_tombstone);
    _row = std::move(o._row);
}

row::row(const schema& s, column_kind kind, const row& o) : _size(o._size)
{
    auto clone_cell_and_hash = [&s, &kind] (column_id id, const cell_and_hash& cah) {
        auto& cdef = s.column_at(kind, id);
        return cell_and_hash(cah.cell.copy(*cdef.type), cah.hash);
    };

    _cells.clone_from(o._cells, clone_cell_and_hash);
}

row row::construct(const schema& our_schema, const schema& their_schema, column_kind kind, const row& o) {
    if (our_schema.version() == their_schema.version()) {
        return row(our_schema, kind, o);
    } else {
        row r;
        r.apply(our_schema, their_schema, kind, o);
        return r;
    }
}

row::~row() {
}

const atomic_cell_or_collection& row::cell_at(column_id id) const {
    auto&& cell = find_cell(id);
    if (!cell) {
        throw_with_backtrace<std::out_of_range>(format("Column not found for id = {:d}", id));
    }
    return *cell;
}

bool row::equal(column_kind kind, const schema& this_schema, const row& other, const schema& other_schema) const {
    if (size() != other.size()) {
        return false;
    }

    auto cells_equal = [&] (column_id id1, const atomic_cell_or_collection& c1,
                            column_id id2, const atomic_cell_or_collection& c2) {
        static_assert(schema::row_column_ids_are_ordered_by_name::value, "Relying on column ids being ordered by name");
        auto& at1 = *this_schema.column_at(kind, id1).type;
        auto& at2 = *other_schema.column_at(kind, id2).type;
        return at1 == at2
               && this_schema.column_at(kind, id1).name() == other_schema.column_at(kind, id2).name()
               && c1.equals(at1, c2);
    };

    auto i1 = _cells.begin();
    auto i1_end = _cells.end();
    auto i2 = other._cells.begin();
    auto i2_end = other._cells.end();

    while (true) {
        if (i1 == i1_end) {
            return i2 == i2_end;
        }
        if (i2 == i2_end) {
            return i1 == i1_end;
        }

        if (!cells_equal(i1.key(), i1->cell, i2.key(), i2->cell)) {
            return false;
        }

        i1++;
        i2++;
    }
}

row::row() {
}

row::row(row&& other) noexcept
    : _size(other._size), _cells(std::move(other._cells)) {
    other._size = 0;
}

row& row::operator=(row&& other) noexcept {
    if (this != &other) {
        this->~row();
        new (this) row(std::move(other));
    }
    return *this;
}

void row::apply(const schema& s, column_kind kind, row&& other) {
    apply_monotonically(s, kind, std::move(other));
}

void row::apply(const schema& s, column_kind kind, const row& other) {
    apply_monotonically(s, kind, other);
}

void row::apply(const schema& our_schema, const schema& their_schema, column_kind kind, row&& other) {
    apply_monotonically(our_schema, their_schema, kind, std::move(other));
};

void row::apply(const schema& our_schema, const schema& their_schema, column_kind kind, const row& other) {
    apply_monotonically(our_schema, their_schema, kind, other);
};

void row::apply_monotonically(const schema& s, column_kind kind, row&& other) {
    if (other.empty()) {
        return;
    }
    other.consume_with([&] (column_id id, cell_and_hash& c_a_h) {
        apply_monotonically(s.column_at(kind, id), std::move(c_a_h.cell), std::move(c_a_h.hash));
    });
}

void row::apply_monotonically(const schema& s, column_kind kind, const row& other) {
    if (other.empty()) {
        return;
    }
    other.for_each_cell([&] (column_id id, const cell_and_hash& c_a_h) {
        apply(s.column_at(kind, id), c_a_h.cell, c_a_h.hash);
    });
}

void row::apply_monotonically(const schema& our_schema, const schema& their_schema, column_kind kind, row&& other) {
    if (our_schema.version() == their_schema.version()) {
        return apply_monotonically(our_schema, kind, std::move(other));
    }
    other.consume_with([&] (column_id id, cell_and_hash& c_a_h) {
        const column_definition& their_col = their_schema.column_at(kind, id);
        const column_definition* our_col = our_schema.get_column_definition(their_col.name());
        if (our_col) {
            converting_mutation_partition_applier::append_cell(*this, kind, *our_col, their_col, c_a_h.cell);
        }
    });
}

void row::apply_monotonically(const schema& our_schema, const schema& their_schema, column_kind kind, const row& other) {
    if (our_schema.version() == their_schema.version()) {
        return apply_monotonically(our_schema, kind, other);
    }
    other.for_each_cell([&] (column_id id, const cell_and_hash& c_a_h) {
        const column_definition& their_col = their_schema.column_at(kind, id);
        const column_definition* our_col = our_schema.get_column_definition(their_col.name());
        if (our_col) {
            converting_mutation_partition_applier::append_cell(*this, kind, *our_col, their_col, c_a_h.cell);
        }
    });
}

// When views contain a primary key column that is not part of the base table primary key,
// that column determines whether the row is live or not. We need to ensure that when that
// cell is dead, and thus the derived row marker, either by normal deletion of by TTL, so
// is the rest of the row. To ensure that none of the regular columns keep the row alive,
// we erase the live cells according to the shadowable_tombstone rules.
static bool dead_marker_shadows_row(const schema& s, column_kind kind, const row_marker& marker) {
    return s.is_view()
            && (s.view_info()->has_base_non_pk_columns_in_view_pk() || s.view_info()->has_computed_column_depending_on_base_non_primary_key())
            && !marker.is_live()
            && kind == column_kind::regular_column; // not applicable to static rows
}

compact_and_expire_result row::compact_and_expire(
        const schema& s,
        column_kind kind,
        row_tombstone tomb,
        gc_clock::time_point query_time,
        can_gc_fn& can_gc,
        gc_clock::time_point gc_before,
        const row_marker& marker,
        compaction_garbage_collector* collector)
{
    if (dead_marker_shadows_row(s, kind, marker)) {
        tomb.apply(shadowable_tombstone(api::max_timestamp, gc_clock::time_point::max()), row_marker());
    }
    compact_and_expire_result res{};
    remove_if([&] (column_id id, atomic_cell_or_collection& c) {
        bool erase = false;
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            atomic_cell_view cell = c.as_atomic_cell(def);
            auto can_erase_cell = [&] {
                // Only row tombstones can be shadowable, (collection) cell tombstones aren't
                return cell.deletion_time() < gc_before && can_gc(tombstone(cell.timestamp(), cell.deletion_time()), is_shadowable::no);
            };

            if (cell.is_covered_by(tomb.regular(), def.is_counter())) {
                erase = true;
                res.dead_cells++;
            } else if (cell.is_covered_by(tomb.shadowable().tomb(), def.is_counter())) {
                erase = true;
                res.dead_cells++;
            } else if (cell.has_expired(query_time)) {
                erase = can_erase_cell();
                if (!erase) {
                    c = atomic_cell::make_dead(cell.timestamp(), cell.deletion_time());
                } else if (collector) {
                    collector->collect(id, atomic_cell::make_dead(cell.timestamp(), cell.deletion_time()));
                }
                res.dead_cells++;
            } else if (!cell.is_live()) {
                erase = can_erase_cell();
                if (erase && collector) {
                    collector->collect(id, atomic_cell::make_dead(cell.timestamp(), cell.deletion_time()));
                }
                res.dead_cells++;
            } else {
                res.live_cells++;
            }
        } else {
            c.as_collection_mutation().with_deserialized(*def.type, [&] (collection_mutation_view_description m_view) {
                auto m = m_view.materialize(*def.type);
                res += m.compact_and_expire(id, tomb, query_time, can_gc, gc_before, collector);
                if (m.cells.empty() && m.tomb <= tomb.tomb()) {
                    erase = true;
                } else {
                    c = m.serialize(*def.type);
                }
            });
        }
        return erase;
    });
    return res;
}

compact_and_expire_result row::compact_and_expire(
        const schema& s,
        column_kind kind,
        row_tombstone tomb,
        gc_clock::time_point query_time,
        can_gc_fn& can_gc,
        gc_clock::time_point gc_before,
        compaction_garbage_collector* collector) {
    row_marker m;
    return compact_and_expire(s, kind, tomb, query_time, can_gc, gc_before, m, collector);
}

bool lazy_row::compact_and_expire(
        const schema& s,
        column_kind kind,
        row_tombstone tomb,
        gc_clock::time_point query_time,
        can_gc_fn& can_gc,
        gc_clock::time_point gc_before,
        const row_marker& marker,
        compaction_garbage_collector* collector) {
    if (!_row) {
        return false;
    }
    return _row->compact_and_expire(s, kind, tomb, query_time, can_gc, gc_before, marker, collector).is_live();
}

bool lazy_row::compact_and_expire(
        const schema& s,
        column_kind kind,
        row_tombstone tomb,
        gc_clock::time_point query_time,
        can_gc_fn& can_gc,
        gc_clock::time_point gc_before,
        compaction_garbage_collector* collector) {
    if (!_row) {
        return false;
    }
    return _row->compact_and_expire(s, kind, tomb, query_time, can_gc, gc_before, collector).is_live();
}

std::ostream& operator<<(std::ostream& os, const lazy_row::printer& p) {
    return os << row::printer(p._schema, p._kind, p._row.get());
}

bool deletable_row::compact_and_expire(const schema& s,
                                       tombstone tomb,
                                       gc_clock::time_point query_time,
                                       can_gc_fn& can_gc,
                                       gc_clock::time_point gc_before,
                                       compaction_garbage_collector* collector)
{
    auto should_purge_row_tombstone = [&] (const row_tombstone& t) {
        return t.max_deletion_time() < gc_before && can_gc(t.tomb(), t.is_shadowable());
    };

    apply(tomb);
    bool is_live = marker().compact_and_expire(deleted_at().tomb(), query_time, can_gc, gc_before);
    is_live |= cells().compact_and_expire(s, column_kind::regular_column, deleted_at(), query_time, can_gc, gc_before, marker(), collector).is_live();

    if (deleted_at().tomb() <= tomb || should_purge_row_tombstone(deleted_at())) {
        remove_tombstone();
    }

    return is_live;
}


deletable_row deletable_row::difference(const schema& s, column_kind kind, const deletable_row& other) const
{
    deletable_row dr;
    if (_deleted_at > other._deleted_at) {
        dr.apply(_deleted_at);
    }
    if (compare_row_marker_for_merge(_marker, other._marker) > 0) {
        dr.apply(_marker);
    }
    dr._cells = _cells.difference(s, kind, other._cells);
    return dr;
}

row row::difference(const schema& s, column_kind kind, const row& other) const
{
    row r;

    auto c = _cells.begin();
    auto c_end = _cells.end();
    auto it = other._cells.begin();
    auto it_end = other._cells.end();

    while (c != c_end) {
        while (it != it_end && it.key() < c.key()) {
            ++it;
        }
        auto& cdef = s.column_at(kind, c.key());
        if (it == it_end || it.key() != c.key()) {
            r.append_cell(c.key(), c->cell.copy(*cdef.type));
        } else if (cdef.is_counter()) {
            auto cell = counter_cell_view::difference(c->cell.as_atomic_cell(cdef), it->cell.as_atomic_cell(cdef));
            if (cell) {
                r.append_cell(c.key(), std::move(*cell));
            }
        } else if (cdef.is_atomic()) {
            if (compare_atomic_cell_for_merge(c->cell.as_atomic_cell(cdef), it->cell.as_atomic_cell(cdef)) > 0) {
                r.append_cell(c.key(), c->cell.copy(*cdef.type));
            }
        } else {
            auto diff = ::difference(*cdef.type,
                    c->cell.as_collection_mutation(), it->cell.as_collection_mutation());
            if (!static_cast<collection_mutation_view>(diff).is_empty()) {
                r.append_cell(c.key(), std::move(diff));
            }
        }
        c++;
    }

    return r;
}

bool row_marker::compact_and_expire(tombstone tomb, gc_clock::time_point now,
        can_gc_fn& can_gc, gc_clock::time_point gc_before, compaction_garbage_collector* collector) {
    if (is_missing()) {
        return false;
    }
    if (_timestamp <= tomb.timestamp) {
        _timestamp = api::missing_timestamp;
        return false;
    }
    if (_ttl > no_ttl && _expiry <= now) {
        _expiry -= _ttl;
        _ttl = dead;
    }
    // The row marker itself is not shadowable.
    // Only the deletable_row.row_rombstone may be shadowable
    if (_ttl == dead && _expiry < gc_before && can_gc(tombstone(_timestamp, _expiry), is_shadowable::no)) {
        if (collector) {
            collector->collect(*this);
        }
        _timestamp = api::missing_timestamp;
    }
    return !is_missing() && _ttl != dead;
}

mutation_partition mutation_partition::difference(const schema& s, const mutation_partition& other) const
{
    check_schema(s);
    mutation_partition mp(s);
    if (_tombstone > other._tombstone) {
        mp.apply(_tombstone);
    }
    mp._static_row = _static_row.difference(s, column_kind::static_column, other._static_row);

    mp._row_tombstones = _row_tombstones.difference(s, other._row_tombstones);

    auto it_r = other._rows.begin();
    rows_entry::compare cmp_r(s);
    for (auto&& r : _rows) {
        if (r.dummy()) {
            continue;
        }
        while (it_r != other._rows.end() && (it_r->dummy() || cmp_r(*it_r, r))) {
            ++it_r;
        }
        if (it_r == other._rows.end() || !it_r->key().equal(s, r.key())) {
            mp.insert_row(s, r.key(), r.row());
        } else {
            auto dr = r.row().difference(s, column_kind::regular_column, it_r->row());
            if (!dr.empty()) {
                mp.insert_row(s, r.key(), std::move(dr));
            }
        }
    }
    return mp;
}

void mutation_partition::accept(const schema& s, mutation_partition_visitor& v) const {
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
mutation_partition::upgrade(const schema& old_schema, const schema& new_schema) {
    // We need to copy to provide strong exception guarantees.
    mutation_partition tmp(new_schema);
    tmp.set_static_row_continuous(_static_row_continuous);
    converting_mutation_partition_applier v(old_schema.get_column_mapping(), new_schema, tmp);
    accept(old_schema, v);
    *this = std::move(tmp);
}

mutation_querier::mutation_querier(const schema& s, query::result::partition_writer pw,
                                   query::result_memory_accounter& memory_accounter)
    : _schema(s)
    , _memory_accounter(memory_accounter)
    , _static_cells_wr(pw.start().start_static_row().start_cells())
    , _pw(std::move(pw))
{
}

void mutation_querier::query_static_row(const row& r, tombstone current_tombstone)
{
    const query::partition_slice& slice = _pw.slice();
    if (!slice.static_columns.empty()) {
        if (_pw.requested_result()) {
            auto start = _static_cells_wr._out.size();
            get_compacted_row_slice(_schema, slice, column_kind::static_column,
                                    r, slice.static_columns, _static_cells_wr);
            _memory_accounter.update(_static_cells_wr._out.size() - start);
        } else {
            seastar::measuring_output_stream stream;
            ser::qr_partition__static_row__cells<seastar::measuring_output_stream> out(stream, { });
            auto start = stream.size();
            get_compacted_row_slice(_schema, slice, column_kind::static_column,
                                    r, slice.static_columns, out);
            _memory_accounter.update(stream.size() - start);
        }
        if (_pw.requested_digest()) {
            max_timestamp max_ts{_pw.last_modified()};
            _pw.digest().feed_hash(current_tombstone);
            max_ts.update(current_tombstone.timestamp);
            _pw.digest().feed_hash(r, _schema, column_kind::static_column, slice.static_columns, max_ts);
            _pw.last_modified() = max_ts.max;
        }
    }
    _rows_wr.emplace(std::move(_static_cells_wr).end_cells().end_static_row().start_rows());
}

stop_iteration mutation_querier::consume(static_row&& sr, tombstone current_tombstone) {
    query_static_row(sr.cells(), current_tombstone);
    _live_data_in_static_row = true;
    return stop_iteration::no;
}

void mutation_querier::prepare_writers() {
    if (!_rows_wr) {
        row empty_row;
        query_static_row(empty_row, { });
        _live_data_in_static_row = false;
    }
}

stop_iteration mutation_querier::consume(clustering_row&& cr, row_tombstone current_tombstone) {
    prepare_writers();

    const query::partition_slice& slice = _pw.slice();

    if (_pw.requested_digest()) {
        _pw.digest().feed_hash(cr.key(), _schema);
        _pw.digest().feed_hash(current_tombstone);
        max_timestamp max_ts{_pw.last_modified()};
        max_ts.update(current_tombstone.tomb().timestamp);
        _pw.digest().feed_hash(cr.cells(), _schema, column_kind::regular_column, slice.regular_columns, max_ts);
        _pw.last_modified() = max_ts.max;
    }

    auto write_row = [&] (auto& rows_writer) {
        auto cells_wr = [&] {
            if (slice.options.contains(query::partition_slice::option::send_clustering_key)) {
                return rows_writer.add().write_key(cr.key()).start_cells().start_cells();
            } else {
                return rows_writer.add().skip_key().start_cells().start_cells();
            }
        }();
        get_compacted_row_slice(_schema, slice, column_kind::regular_column, cr.cells(), slice.regular_columns, cells_wr);
        std::move(cells_wr).end_cells().end_cells().end_qr_clustered_row();
    };

    auto stop = stop_iteration::no;
    if (_pw.requested_result()) {
        auto start = _rows_wr->_out.size();
        write_row(*_rows_wr);
        stop = _memory_accounter.update_and_check(_rows_wr->_out.size() - start);
    } else {
        seastar::measuring_output_stream stream;
        ser::qr_partition__rows<seastar::measuring_output_stream> out(stream, { });
        auto start = stream.size();
        write_row(out);
        stop = _memory_accounter.update_and_check(stream.size() - start);
    }

    _live_clustering_rows++;
    return stop;
}

uint64_t mutation_querier::consume_end_of_stream() {
    prepare_writers();

    // If we got no rows, but have live static columns, we should only
    // give them back IFF we did not have any CK restrictions.
    // #589
    // If ck:s exist, and we do a restriction on them, we either have matching
    // rows, or return nothing, since cql does not allow "is null".
    bool return_static_content_on_partition_with_no_rows =
        _pw.slice().options.contains(query::partition_slice::option::always_return_static_content) ||
        !has_ck_selector(_pw.ranges());
    if (!_live_clustering_rows && (!return_static_content_on_partition_with_no_rows || !_live_data_in_static_row)) {
        _pw.retract();
        return 0;
    } else {
        auto live_rows = std::max(_live_clustering_rows, uint64_t(1));
        _pw.row_count() += live_rows;
        _pw.partition_count() += 1;
        std::move(*_rows_wr).end_rows().end_qr_partition();
        return live_rows;
    }
}

query_result_builder::query_result_builder(const schema& s, query::result::builder& rb) noexcept
    : _schema(s), _rb(rb)
{ }

void query_result_builder::consume_new_partition(const dht::decorated_key& dk) {
    _mutation_consumer.emplace(mutation_querier(_schema, _rb.add_partition(_schema, dk.key()), _rb.memory_accounter()));
}

void query_result_builder::consume(tombstone t) {
    _mutation_consumer->consume(t);
    _stop = _rb.bump_and_check_tombstone_limit();
}
stop_iteration query_result_builder::consume(static_row&& sr, tombstone t, bool is_live) {
    if (!is_live) {
        _stop = _rb.bump_and_check_tombstone_limit();
        return _stop;
    }
    _stop = _mutation_consumer->consume(std::move(sr), t);
    return _stop;
}
stop_iteration query_result_builder::consume(clustering_row&& cr, row_tombstone t,  bool is_live) {
    if (!is_live) {
        _stop = _rb.bump_and_check_tombstone_limit();
        return _stop;
    }
    _stop = _mutation_consumer->consume(std::move(cr), t);
    return _stop;
}
stop_iteration query_result_builder::consume(range_tombstone_change&& rtc) {
    _stop = _rb.bump_and_check_tombstone_limit();
    return _stop;
}

stop_iteration query_result_builder::consume_end_of_partition() {
    auto live_rows_in_partition = _mutation_consumer->consume_end_of_stream();
    if (live_rows_in_partition > 0 && !_stop) {
        _stop = _rb.memory_accounter().check();
    }
    if (_stop) {
        _rb.mark_as_short_read();
    }
    return _stop;
}

void query_result_builder::consume_end_of_stream() {
}

stop_iteration query::result_memory_accounter::check_local_limit() const {
    if (_short_read_allowed) {
        return stop_iteration(_total_used_memory > _maximum_result_size.get_page_size());
    } else {
        if (_total_used_memory > _maximum_result_size.hard_limit) {
            throw std::runtime_error(fmt::format(
                    "Memory usage of unpaged query exceeds hard limit of {} (configured via max_memory_for_unlimited_query_hard_limit)",
                    _maximum_result_size.hard_limit));
        }
        if (_below_soft_limit && _total_used_memory > _maximum_result_size.soft_limit) {
            mplog.warn(
                    "Memory usage of unpaged query exceeds soft limit of {} (configured via max_memory_for_unlimited_query_soft_limit)",
                    _maximum_result_size.soft_limit);
            _below_soft_limit = false;
        }
    }
    return stop_iteration::no;
}

void reconcilable_result_builder::consume_new_partition(const dht::decorated_key& dk) {
    _rt_assembler.reset();
    _return_static_content_on_partition_with_no_rows =
        _slice.options.contains(query::partition_slice::option::always_return_static_content) ||
        !has_ck_selector(_slice.row_ranges(*_query_schema, dk.key()));
    _static_row_is_alive = false;
    _live_rows = 0;
    _mutation_consumer.emplace(streamed_mutation_freezer(*_query_schema, dk.key()));
    _used_at_entry = _memory_accounter.used_memory();
}

void reconcilable_result_builder::consume(tombstone t) {
    _mutation_consumer->consume(t);
}

stop_iteration reconcilable_result_builder::consume(static_row&& sr, tombstone, bool is_alive) {
    _static_row_is_alive = is_alive;
    _memory_accounter.update(sr.memory_usage(*_query_schema));
    return _mutation_consumer->consume(std::move(sr));
}

stop_iteration reconcilable_result_builder::consume(clustering_row&& cr, row_tombstone, bool is_alive) {
    if (_rt_assembler.needs_flush()) {
        if (auto rt_opt = _rt_assembler.flush(*_query_schema, position_in_partition::after_key(*_query_schema, cr.key()))) {
            consume(std::move(*rt_opt));
        }
    }
    _live_rows += is_alive;
    auto stop = _memory_accounter.update_and_check(cr.memory_usage(*_query_schema));
    if (is_alive || _slice.options.contains<query::partition_slice::option::allow_mutation_read_page_without_live_row>()) {
        // We are considering finishing current read only after consuming a
        // live clustering row. While sending a single live row is enough to
        // guarantee progress, not ending the result on a live row would
        // mean that the next page fetch will read all tombstones after the
        // last live row again.
        _stop = stop;
    }
    return _mutation_consumer->consume(std::move(cr)) || _stop;
}

stop_iteration reconcilable_result_builder::consume(range_tombstone&& rt) {
    _memory_accounter.update(rt.memory_usage(*_query_schema));
    return _mutation_consumer->consume(std::move(rt));
}

stop_iteration reconcilable_result_builder::consume(range_tombstone_change&& rtc) {
    if (auto rt_opt = _rt_assembler.consume(*_query_schema, std::move(rtc))) {
        return consume(std::move(*rt_opt));
    }
    return stop_iteration::no;
}

stop_iteration reconcilable_result_builder::consume_end_of_partition() {
    _rt_assembler.on_end_of_stream();
    if (_live_rows == 0 && _static_row_is_alive && _return_static_content_on_partition_with_no_rows) {
        ++_live_rows;
        // Normally we count only live clustering rows, to guarantee that
        // the next page fetch won't ask for the same range. However,
        // if we return just a single static row we can stop the result as
        // well. Next page fetch will ask for the next partition and if we
        // don't do that we could end up with an unbounded number of
        // partitions with only a static row.
        _stop = _stop || _memory_accounter.check();
    }
    _total_live_rows += _live_rows;
    _result.emplace_back(partition { _live_rows, _mutation_consumer->consume_end_of_stream() });

    auto accounted = _memory_accounter.used_memory() - _used_at_entry;
    auto actually_used = sizeof(partition) + _result.back().mut().representation().size();
    if (actually_used > accounted) {
        _memory_accounter.update(actually_used - accounted);
    }

    if (_slice.options.contains<query::partition_slice::option::allow_mutation_read_page_without_live_row>()) {
        _stop = _stop || _memory_accounter.check();
    }

    return _stop;
}

reconcilable_result reconcilable_result_builder::consume_end_of_stream() {
    return reconcilable_result(_total_live_rows, std::move(_result),
                               query::short_read(bool(_stop)),
                               std::move(_memory_accounter).done());
}

future<query::result>
to_data_query_result(const reconcilable_result& r, schema_ptr s, const query::partition_slice& slice, uint64_t max_rows, uint32_t max_partitions,
        query::result_options opts) {
    // This result was already built with a limit, don't apply another one.
    query::result::builder builder(slice, opts, query::result_memory_accounter{ query::result_memory_limiter::unlimited_result_size }, query::max_tombstones);
    auto consumer = compact_for_query_v2<query_result_builder>(*s, gc_clock::time_point::min(), slice, max_rows,
            max_partitions, query_result_builder(*s, builder));
    auto compaction_state = consumer.get_state();
    frozen_mutation_consumer_adaptor adaptor(s, consumer);
    for (const partition& p : r.partitions()) {
        const auto res = co_await p.mut().consume_gently(s, adaptor);
        if (res.stop == stop_iteration::yes) {
            break;
        }
    }
    if (r.is_short_read()) {
        builder.mark_as_short_read();
    }
    co_return builder.build(compaction_state->current_full_position());
}

query::result
query_mutation(mutation&& m, const query::partition_slice& slice, uint64_t row_limit, gc_clock::time_point now, query::result_options opts) {
    query::result::builder builder(slice, opts, query::result_memory_accounter{ query::result_memory_limiter::unlimited_result_size }, query::max_tombstones);
    auto consumer = compact_for_query_v2<query_result_builder>(*m.schema(), now, slice, row_limit,
            query::max_partitions, query_result_builder(*m.schema(), builder));
    auto compaction_state = consumer.get_state();
    std::move(m).consume(consumer, consume_in_reverse::no);
    return builder.build(compaction_state->current_full_position());
}

class counter_write_query_result_builder {
    const schema& _schema;
    mutation_opt _mutation;
public:
    counter_write_query_result_builder(const schema& s) : _schema(s) { }
    void consume_new_partition(const dht::decorated_key& dk) {
        _mutation = mutation(_schema.shared_from_this(), dk);
    }
    void consume(tombstone) { }
    stop_iteration consume(static_row&& sr, tombstone, bool is_live) {
        if (!is_live) {
            return stop_iteration::no;
        }
        _mutation->partition().static_row().maybe_create() = std::move(sr.cells());
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone,  bool is_live) {
        if (!is_live) {
            return stop_iteration::no;
        }
        _mutation->partition().insert_row(_schema, cr.key(), std::move(cr).as_deletable_row());
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }
    mutation_opt consume_end_of_stream() {
        return std::move(_mutation);
    }
};

mutation_partition::mutation_partition(mutation_partition::incomplete_tag, const schema& s, tombstone t)
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

bool mutation_partition::is_fully_continuous() const {
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

void mutation_partition::make_fully_continuous() {
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

void mutation_partition::set_continuity(const schema& s, const position_range& pr, is_continuous cont) {
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
        if (i->dummy()) {
            i = _rows.erase_and_dispose(i, alloc_strategy_deleter<rows_entry>());
        } else {
            ++i;
        }
    }
}

clustering_interval_set mutation_partition::get_continuity(const schema& s, is_continuous cont) const {
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

stop_iteration mutation_partition::clear_gently(cache_tracker* tracker) noexcept {
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
        // mutation_partition objects. Not doing this would cause eviction to leave garbage
        // versions behind unnecessarily.
        if (need_preempt() && i != end) {
            return stop_iteration::no;
        }
    }

    return stop_iteration::yes;
}

bool
mutation_partition::check_continuity(const schema& s, const position_range& r, is_continuous cont) const {
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
mutation_partition::fully_continuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::yes);
}

bool
mutation_partition::fully_discontinuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::no);
}

future<mutation_opt> counter_write_query(schema_ptr s, const mutation_source& source, reader_permit permit,
                                         const dht::decorated_key& dk,
                                         const query::partition_slice& slice,
                                         tracing::trace_state_ptr trace_ptr)
{
    struct range_and_reader {
        dht::partition_range range;
        mutation_reader reader;

        range_and_reader(range_and_reader&&) = delete;
        range_and_reader(const range_and_reader&) = delete;

        range_and_reader(schema_ptr s, const mutation_source& source, reader_permit permit,
                         const dht::decorated_key& dk,
                         const query::partition_slice& slice,
                         tracing::trace_state_ptr trace_ptr)
            : range(dht::partition_range::make_singular(dk))
            , reader(source.make_reader_v2(s, std::move(permit), range, slice,
                                                      std::move(trace_ptr), streamed_mutation::forwarding::no,
                                                      mutation_reader::forwarding::no))
        { }
    };

    // do_with() doesn't support immovable objects
    auto r_a_r = std::make_unique<range_and_reader>(s, source, std::move(permit), dk, slice, std::move(trace_ptr));
    auto cwqrb = counter_write_query_result_builder(*s);
    auto cfq = compact_for_query_v2<counter_write_query_result_builder>(
            *s, gc_clock::now(), slice, query::max_rows, query::max_partitions, std::move(cwqrb));
    auto f = r_a_r->reader.consume(std::move(cfq));
    return f.finally([r_a_r = std::move(r_a_r)] {
        return r_a_r->reader.close();
    });
}

mutation_cleaner_impl::~mutation_cleaner_impl() {
    _worker_state->done = true;
    _worker_state->cv.signal();
    _worker_state->snapshots.clear_and_dispose(typename lw_shared_ptr<partition_snapshot>::disposer());
    with_allocator(_region.allocator(), [this] {
        clear();
    });
}

void mutation_cleaner_impl::clear() noexcept {
    while (clear_gently() == stop_iteration::no) ;
}

stop_iteration mutation_cleaner_impl::clear_gently() noexcept {
    while (clear_some() == memory::reclaiming_result::reclaimed_something) {
        if (need_preempt()) {
            return stop_iteration::no;
        }
    }
    return stop_iteration::yes;
}

memory::reclaiming_result mutation_cleaner_impl::clear_some() noexcept {
    if (_versions.empty()) {
        return memory::reclaiming_result::reclaimed_nothing;
    }
    auto&& alloc = current_allocator();
    partition_version& pv = _versions.front();
    if (pv.clear_gently(_tracker) == stop_iteration::yes) {
        _versions.pop_front();
        alloc.destroy(&pv);
    }
    return memory::reclaiming_result::reclaimed_something;
}

void mutation_cleaner_impl::merge(mutation_cleaner_impl& r) noexcept {
    _versions.splice(r._versions);
    for (partition_snapshot& snp : r._worker_state->snapshots) {
        snp.migrate(&_region, _cleaner);
    }
    _worker_state->snapshots.splice(_worker_state->snapshots.end(), r._worker_state->snapshots);
    if (!_worker_state->snapshots.empty()) {
        _worker_state->cv.signal();
    }
}

void mutation_cleaner_impl::start_worker() {
    auto f = repeat([w = _worker_state, this] () mutable noexcept {
      if (w->done) {
          return make_ready_future<stop_iteration>(stop_iteration::yes);
      }
      return with_scheduling_group(_scheduling_group, [w, this] {
        return w->cv.wait([w] {
            return (w->done || !w->snapshots.empty()) && !w->merging_paused;
        }).then([this, w] () noexcept {
            if (w->done) {
                return stop_iteration::yes;
            }
            if (w->merging_paused) {
                return stop_iteration::no;
            }
            merge_some();
            return stop_iteration::no;
        });
      });
    });
    if (f.failed()) {
        f.get();
    }
}

stop_iteration mutation_cleaner_impl::merge_some(partition_snapshot& snp) noexcept {
    auto&& region = snp.region();
    return with_allocator(region.allocator(), [&] {
        {
            // Allocating sections require the region to be reclaimable
            // which means that they cannot be nested.
            // It is, however, possible, that if the snapshot is taken
            // inside an allocating section and then an exception is thrown
            // this function will be called to clean up even though we
            // still will be in the context of the allocating section.
            if (!region.reclaiming_enabled()) {
                return stop_iteration::no;
            }
            try {
                auto dirty_guard = make_region_space_guard();
                return _worker_state->alloc_section(region, [&] {
                    return snp.merge_partition_versions(_app_stats);
                });
            } catch (...) {
                // Merging failed, give up as there is no guarantee of forward progress.
                return stop_iteration::yes;
            }
        }
    });
}

stop_iteration mutation_cleaner_impl::merge_some() noexcept {
    if (_worker_state->snapshots.empty()) {
        return stop_iteration::yes;
    }
    partition_snapshot& snp = _worker_state->snapshots.front();
    if (merge_some(snp) == stop_iteration::yes) {
        _worker_state->snapshots.pop_front();
        lw_shared_ptr<partition_snapshot>::dispose(&snp);
    }
    return stop_iteration::no;
}

future<> mutation_cleaner_impl::drain() {
    return repeat([this] {
        return merge_some();
    }).then([this] {
        return repeat([this] {
            return with_allocator(_region.allocator(), [this] {
                return clear_gently();
            });
        });
    });
}

can_gc_fn always_gc = [] (tombstone, is_shadowable) { return true; };
can_gc_fn never_gc = [] (tombstone, is_shadowable) { return false; };

max_purgeable_fn can_always_purge = [] (const dht::decorated_key&, is_shadowable) { return api::max_timestamp; };
max_purgeable_fn can_never_purge = [] (const dht::decorated_key&, is_shadowable) { return api::min_timestamp; };

logging::logger compound_logger("compound");
