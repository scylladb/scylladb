/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/heap_algorithm.hpp>

#include "partition_version.hh"
#include "row_cache.hh"
#include "partition_snapshot_row_cursor.hh"
#include "utils/assert.hh"
#include "utils/coroutine.hh"
#include "real_dirty_memory_accounter.hh"

static void remove_or_mark_as_unique_owner(partition_version* current, mutation_cleaner* cleaner)
{
    while (current && !current->is_referenced()) {
        auto next = current->next();
        current->erase();
        if (cleaner) {
            cleaner->destroy_gently(*current);
        } else {
            current_allocator().destroy(current);
        }
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

partition_version::partition_version(partition_version&& pv) noexcept
    : anchorless_list_base_hook(std::move(static_cast<anchorless_list_base_hook&>(pv)))
    , _backref(pv._backref)
    , _schema(std::move(pv._schema))
    , _is_being_upgraded(pv._is_being_upgraded)
    , _partition(std::move(pv._partition))
{
    if (_backref) {
        _backref->_version = this;
    }
    pv._backref = nullptr;
}

partition_version& partition_version::operator=(partition_version&& pv) noexcept
{
    if (this != &pv) {
        this->~partition_version();
        new (this) partition_version(std::move(pv));
    }
    return *this;
}

partition_version::~partition_version()
{
    if (_backref) {
        _backref->_version = nullptr;
    }
    with_allocator(standard_allocator(), [&] {
        // Destroying the schema_ptr can cause a destruction of the schema,
        // so it has to happen in the allocator which schemas are allocated in.
        _schema = nullptr;
    });
}

stop_iteration partition_version::clear_gently(cache_tracker* tracker) noexcept {
    return _partition.clear_gently(tracker);
}

size_t partition_version::size_in_allocator(allocation_strategy& allocator) const {
    return allocator.object_memory_size_in_allocator(this) +
           partition().external_memory_usage(*_schema);
}

namespace {

// A functor which transforms objects from Domain into objects from CoDomain
template<typename U, typename Domain, typename CoDomain>
concept Mapper =
    requires(U obj, const Domain& src) {
        { obj(src) } -> std::convertible_to<const CoDomain&>;
    };

// A functor which merges two objects from Domain into one. The result is stored in the first argument.
template<typename U, typename Domain>
concept Reducer =
    requires(U obj, Domain& dst, const Domain& src) {
        { obj(dst, src) } -> std::same_as<void>;
    };

// Calculates the value of particular part of mutation_partition represented by
// the version chain starting from v.
// |map| extracts the part from each version.
// |reduce| Combines parts from the two versions.
template <typename Result, typename Map, typename Initial, typename Reduce>
requires Mapper<Map, mutation_partition_v2, Result> && Reducer<Reduce, Result>
inline Result squashed(const partition_version_ref& v, Map&& map, Initial&& initial, Reduce&& reduce) {
    const partition_version* this_v = &*v;
    partition_version* it = v->last();
    Result r = initial(map(it->partition()));
    while (it != this_v) {
        it = it->prev();
        reduce(r, map(it->partition()));
    }
    return r;
}

template <typename Result, typename Map, typename Reduce>
requires Mapper<Map, mutation_partition_v2, Result> && Reducer<Reduce, Result>
inline Result squashed(const partition_version_ref& v, Map&& map, Reduce&& reduce) {
    return squashed<Result>(v, map,
                            [] (auto&& o) -> decltype(auto) { return std::forward<decltype(o)>(o); },
                            reduce);
}

}

::static_row partition_snapshot::static_row(bool digest_requested) const {
    const partition_version* this_v = &*version();
    partition_version* it = this_v->last();
    if (digest_requested) {
        it->partition().static_row().prepare_hash(*it->get_schema(), column_kind::static_column);
    }
    row r = row::construct(*this_v->get_schema(), *it->get_schema(), column_kind::static_column, it->partition().static_row().get());
    while (it != this_v) {
        it = it->prev();
        if (digest_requested) {
            it->partition().static_row().prepare_hash(*it->get_schema(), column_kind::static_column);
        }
        r.apply(*this_v->get_schema(), *it->get_schema(), column_kind::static_column, it->partition().static_row().get());
    }
    return ::static_row(std::move(r));
}

bool partition_snapshot::static_row_continuous() const {
    return version()->partition().static_row_continuous();
}

tombstone partition_snapshot::partition_tombstone() const {
    return ::squashed<tombstone>(version(),
                               [] (const mutation_partition_v2& mp) { return mp.partition_tombstone(); },
                               [] (tombstone& a, tombstone b) { a.apply(b); });
}

mutation_partition partition_snapshot::squashed() const {
    const partition_version* this_v = &*version();
    mutation_partition mp(*this_v->get_schema());
    for (auto it = this_v->last();; it = it->prev()) {
       mutation_application_stats app_stats;
       mp.apply(*this_v->get_schema(), it->partition().as_mutation_partition(*it->get_schema()), *it->get_schema(), app_stats);
       if (it == this_v) {
           break;
       }
    }
    return mp;
}

tombstone partition_entry::partition_tombstone() const {
    return ::squashed<tombstone>(_version,
        [] (const mutation_partition_v2& mp) { return mp.partition_tombstone(); },
        [] (tombstone& a, tombstone b) { a.apply(b); });
}

partition_snapshot::~partition_snapshot() {
    with_allocator(region().allocator(), [this] {
        if (_locked) {
            touch();
        }
        if (_version && _version.is_unique_owner()) {
            auto v = &*_version;
            _version = {};
            remove_or_mark_as_unique_owner(v, _cleaner);
        } else if (_entry) {
            _entry->_snapshot = nullptr;
        }
    });
}

void merge_versions(const schema& s, mutation_partition_v2& newer, mutation_partition_v2&& older, cache_tracker* tracker, is_evictable evictable) {
    older.apply(s, std::move(newer), tracker, evictable);
    newer = std::move(older);
}

// Inserts a new version after pv.
// Used only when upgrading the schema of pv.
static partition_version& append_version(partition_version& pv, const schema& s, cache_tracker* tracker) {
    // Every evictable version must have a dummy entry at the end so that
    // it can be tracked in the LRU. It is also needed to allow old versions
    // to stay around (with tombstones and static rows) after fully evicted.
    // Such versions must be fully discontinuous, and thus have a dummy at the end.
    auto new_version = tracker
                       ? current_allocator().construct<partition_version>(mutation_partition_v2::make_incomplete(s), s.shared_from_this())
                       : current_allocator().construct<partition_version>(mutation_partition_v2(s), s.shared_from_this());
    new_version->partition().set_static_row_continuous(pv.partition().static_row_continuous());
    new_version->insert_after(pv);
    if (tracker) {
        tracker->insert(*new_version);
    }
    return *new_version;
}

stop_iteration partition_snapshot::merge_partition_versions(mutation_application_stats& app_stats) {
    partition_version_ref& v = version();
    if (!v.is_unique_owner()) {
        // Shift _version to the oldest unreferenced version and then keep merging left hand side into it.
        // This is good for performance because in case we were at the latest version
        // we leave it for incoming writes and they don't have to create a new one.
        //
        // If `current->next()` has a different schema than `current`, it will have
        // to be upgraded before being merged with `current`.
        // If its upgrade is already in progress, it would be wasteful (though legal)
        // to initiate its upgrade again, so we stop shifting.
        //
        // See the documentation in partition_version.hh for additional info about upgrades.
        partition_version* current = &*v;
        while (current->next() && !current->next()->is_referenced() && !current->next()->_is_being_upgraded) {
            current = current->next();
            _version = partition_version_ref(*current);
            _version_merging_state.reset();
        }
        while (auto prev = current->prev()) {
            region().allocator().invalidate_references();
            // Here we count writes that overwrote rows from a previous version. Total number of writes does not change.
            mutation_application_stats local_app_stats;
            if (!_version_merging_state) {
                _version_merging_state = apply_resume();
            }
            if (prev->prev() && prev->prev()->_is_being_upgraded) [[unlikely]] {
                // Give up.
                //
                // While `prev->prev()` is being upgraded into `prev`,
                // `prev`'s last dummy violates the usual eviction order.
                // Merging it into `current` could break the "older versions are evicted first".
                //
                // There is no harm in giving up here. After the upgrade finishes,
                // `prev`'s snapshot will slide to `current` and pick up where we left.
                return stop_iteration::yes;
            }
            if (!prev->_is_being_upgraded && prev->get_schema()->version() != current->get_schema()->version()) {
                // The versions we are attempting to merge have different schemas.
                // In this scenario the older version has to be upgraded before
                // being merged with the newer one.
                //
                // This is done by adding a fresh empty version (with the newer
                // schema) after `current` and merging `current` into the new
                // version.
                //
                // While the upgrade is happening, `_is_being_upgraded` is set
                // in the version which is being upgraded, to mark it as having
                // older schema than its `next()` (and therefore violating the
                // normal chronological schema order).  This is necessary
                // precisely for the above `if`, so that after resuming a
                // preempted upgrade we can simply continue, instead of
                // (illegally) initiating an upgrade of the special fresh
                // version back to the old schema.
                //
                // See the documentation in partition_version.hh for additional info about upgrades.
                current = &append_version(*current, *prev->get_schema(), _tracker);
                _version = partition_version_ref(*current);
                prev = current->prev();
                prev->_is_being_upgraded = true;
            }
            const auto do_stop_iteration = current->partition().apply_monotonically(*current->get_schema(),
                *prev->get_schema(), std::move(prev->partition()), _tracker, local_app_stats, default_preemption_check(), *_version_merging_state,
                is_evictable(bool(_tracker)));
            app_stats.row_hits += local_app_stats.row_hits;
            if (do_stop_iteration == stop_iteration::no) {
                return stop_iteration::no;
            }
            // If do_stop_iteration is yes, we have to remove the previous version.
            // It now appears as fully continuous because it is empty.
            _version_merging_state.reset();
            prev->_is_being_upgraded = false;
            if (prev->is_referenced()) {
                _version.release();
                prev->back_reference() = partition_version_ref(*current, prev->back_reference().is_unique_owner());
                current_allocator().destroy(prev);
                return stop_iteration::yes;
            }
            current_allocator().destroy(prev);
        }
    }
    return stop_iteration::yes;
}

stop_iteration partition_snapshot::slide_to_oldest() noexcept {
    partition_version_ref& v = version();
    if (v.is_unique_owner()) {
        return stop_iteration::yes;
    }
    if (_entry) {
        _entry->_snapshot = nullptr;
        _entry = nullptr;
    }
    partition_version* current = &*v;
    while (current->next() && !current->next()->is_referenced() && !current->next()->_is_being_upgraded) {
        current = current->next();
        _version = partition_version_ref(*current);
    }
    return current->prev() ? stop_iteration::no : stop_iteration::yes;
}

unsigned partition_snapshot::version_count()
{
    unsigned count = 0;
    for (auto&& v : versions()) {
        (void)v;
        count++;
    }
    return count;
}

partition_entry::partition_entry(const schema& s, mutation_partition_v2 mp)
{
    auto new_version = current_allocator().construct<partition_version>(std::move(mp), s.shared_from_this());
    _version = partition_version_ref(*new_version);
}

partition_entry::partition_entry(const schema& s, mutation_partition mp)
    : partition_entry(s, mutation_partition_v2(s, std::move(mp)))
{ }

partition_entry::partition_entry(partition_entry::evictable_tag, const schema& s, mutation_partition&& mp)
    : partition_entry(s, [&] {
        mp.ensure_last_dummy(s);
        return mutation_partition_v2(s, std::move(mp));
    }())
{ }

partition_entry partition_entry::make_evictable(const schema& s, mutation_partition&& mp) {
    return {evictable_tag(), s, std::move(mp)};
}

partition_entry partition_entry::make_evictable(const schema& s, const mutation_partition& mp) {
    return make_evictable(s, mutation_partition(s, mp));
}

partition_entry::~partition_entry() {
    if (!_version) {
        return;
    }
    if (_snapshot) {
        SCYLLA_ASSERT(!_snapshot->is_locked());
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
    } else {
        auto v = &*_version;
        _version = { };
        remove_or_mark_as_unique_owner(v, no_cleaner);
    }
}

stop_iteration partition_entry::clear_gently(cache_tracker* tracker) noexcept {
    if (!_version) {
        return stop_iteration::yes;
    }

    if (_snapshot) {
        SCYLLA_ASSERT(!_snapshot->is_locked());
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
        return stop_iteration::yes;
    }

    partition_version* v = &*_version;
    _version = {};
    while (v) {
        if (v->is_referenced()) {
            v->back_reference().mark_as_unique_owner();
            break;
        }
        auto next = v->next();
        if (v->clear_gently(tracker) == stop_iteration::no) {
            _version = partition_version_ref(*v);
            return stop_iteration::no;
        }
        current_allocator().destroy(&*v);
        v = next;
    }
    return stop_iteration::yes;
}

void partition_entry::set_version(partition_version* new_version)
{
    if (_snapshot) {
        SCYLLA_ASSERT(!_snapshot->is_locked());
        _snapshot->_version = std::move(_version);
        _snapshot->_entry = nullptr;
    }

    _snapshot = nullptr;
    _version = partition_version_ref(*new_version);
}

partition_version& partition_entry::add_version(const schema& s, cache_tracker* tracker) {
    // Every evictable version must have a dummy entry at the end so that
    // it can be tracked in the LRU. It is also needed to allow old versions
    // to stay around (with tombstones and static rows) after fully evicted.
    // Such versions must be fully discontinuous, and thus have a dummy at the end.
    auto new_version = tracker
                       ? current_allocator().construct<partition_version>(mutation_partition_v2::make_incomplete(s), s.shared_from_this())
                       : current_allocator().construct<partition_version>(mutation_partition_v2(s), s.shared_from_this());
    new_version->partition().set_static_row_continuous(_version->partition().static_row_continuous());
    new_version->insert_before(*_version);
    set_version(new_version);
    if (tracker) {
        tracker->insert(*new_version);
    }
    return *new_version;
}

void partition_entry::apply(logalloc::region& r, mutation_cleaner& cleaner, const schema& s, const mutation_partition_v2& mp, const schema& mp_schema,
        mutation_application_stats& app_stats) {
    apply(r, cleaner, s, mutation_partition_v2(mp_schema, mp), mp_schema, app_stats);
}

void partition_entry::apply(logalloc::region& r,
           mutation_cleaner& c,
           const schema& s,
           const mutation_partition& mp,
           const schema& mp_schema,
           mutation_application_stats& app_stats) {
    auto mp_v1 = mutation_partition(mp_schema, mp);
    mp_v1.make_fully_continuous();
    apply(r, c, s, mutation_partition_v2(mp_schema, std::move(mp_v1)), mp_schema, app_stats);
}

void partition_entry::apply(logalloc::region& r, mutation_cleaner& cleaner, const schema& s, mutation_partition_v2&& mp, const schema& mp_schema,
        mutation_application_stats& app_stats) {
    // A note about app_stats: it may happen that mp has rows that overwrite other rows
    // in older partition_version. Those overwrites will be counted when their versions get merged.
    if (s.version() != mp_schema.version()) {
        mp.upgrade(mp_schema, s);
    }
    auto new_version = current_allocator().construct<partition_version>(std::move(mp), s.shared_from_this());
    partition_snapshot_ptr snp; // Should die after new_version is inserted
    if (!_snapshot) {
        try {
            apply_resume res;
            auto notify = cleaner.make_region_space_guard();
            if (_version->partition().apply_monotonically(s, s,
                      std::move(new_version->partition()),
                      no_cache_tracker,
                      app_stats,
                      default_preemption_check(),
                      res,
                      is_evictable::no) == stop_iteration::yes) {
                current_allocator().destroy(new_version);
                return;
            } else {
                // Apply was preempted. Let the cleaner finish the job when snapshot dies
                snp = read(r, cleaner, no_cache_tracker);
                // FIXME: Store res in the snapshot as an optimization to resume from where we left off.
            }
        } catch (...) {
            // fall through
        }
    }
    new_version->insert_before(*_version);
    set_version(new_version);
    app_stats.row_writes += new_version->partition().row_count();
}

utils::coroutine partition_entry::apply_to_incomplete(const schema& s,
    partition_entry&& pe,
    mutation_cleaner& pe_cleaner,
    logalloc::allocating_section& alloc,
    logalloc::region& reg,
    cache_tracker& tracker,
    partition_snapshot::phase_type phase,
    real_dirty_memory_accounter& acc,
    preemption_source& preempt_src)
{
    // This flag controls whether this operation may defer. It is more
    // expensive to apply with deferring due to construction of snapshots and
    // two-pass application, with the first pass filtering and moving data to
    // the new version and the second pass merging it back once all is done.
    // We cannot merge into current version because if we defer in the middle
    // that may publish partial writes. Also, snapshot construction results in
    // creation of garbage objects, partition_version and rows_entry. Garbage
    // will yield sparse segments and add overhead due to increased LSA
    // segment compaction. This becomes especially significant for small
    // partitions where I saw 40% slow down.
    const bool preemptible = s.clustering_key_size() > 0;

    // When preemptible, later memtable reads could start using the snapshot before
    // snapshot's writes are made visible in cache, which would cause them to miss those writes.
    // So we cannot allow erasing when preemptible.
    bool can_move = !preemptible && !pe._snapshot;

    auto src_snp = pe.read(reg, pe_cleaner, no_cache_tracker);
    partition_snapshot_ptr prev_snp;
    if (preemptible) {
        // Reads must see prev_snp until whole update completes so that writes
        // are not partially visible.
        prev_snp = read(reg, tracker.cleaner(), &tracker, phase - 1);
    }
    auto dst_snp = read(reg, tracker.cleaner(), &tracker, phase);
    dst_snp->lock();

    // Once we start updating the partition, we must keep all snapshots until the update completes,
    // otherwise partial writes would be published. So the scope of snapshots must enclose the scope
    // of allocating sections, so we return here to get out of the current allocating section and
    // give the caller a chance to store the coroutine object. The code inside coroutine below
    // runs outside allocating section.
    auto& src_snp_ref = *src_snp;
    auto& dst_snp_ref = *dst_snp;
    return utils::coroutine([&tracker, &s, &alloc, &reg, &acc, can_move, preemptible, &preempt_src,
            cur = partition_snapshot_row_cursor(s, dst_snp_ref),
            src_cur = partition_snapshot_row_cursor(s, src_snp_ref, can_move),
            dst_snp = std::move(dst_snp),
            prev_snp = std::move(prev_snp),
            src_snp = std::move(src_snp),
            lb = position_in_partition::before_all_clustered_rows(),
            static_done = false] () mutable {
        auto&& allocator = reg.allocator();
        return alloc(reg, [&] {
            size_t dirty_size = 0;

            if (!static_done) {
                partition_version& dst = *dst_snp->version();
                bool static_row_continuous = dst_snp->static_row_continuous();
                auto current = &*src_snp->version();
                while (current) {
                    dirty_size += allocator.object_memory_size_in_allocator(current)
                        + current->partition().static_row().external_memory_usage(s, column_kind::static_column);
                    dst.partition().apply(current->partition().partition_tombstone());
                    if (static_row_continuous) {
                        lazy_row& static_row = dst.partition().static_row();
                        if (can_move) {
                            static_row.apply(s, column_kind::static_column,
                                std::move(current->partition().static_row()));
                        } else {
                            static_row.apply(s, column_kind::static_column, current->partition().static_row());
                        }
                    }
                    current = current->next();
                    can_move &= current && !current->is_referenced();
                }
                acc.unpin_memory(dirty_size);
                static_done = true;
            }

            if (!src_cur.maybe_refresh_static()) {
                return stop_iteration::yes;
            }

            do {
                auto size = src_cur.memory_usage();
                // Range tombstones in memtables are bounded by dummy entries on both sides.
                SCYLLA_ASSERT(src_cur.range_tombstone_for_row() == src_cur.range_tombstone());
                if (src_cur.range_tombstone()) {
                    // Apply the tombstone to (lb, src_cur.position())
                    // FIXME: Avoid if before all rows
                    auto ropt = cur.ensure_entry_if_complete(lb);
                    cur.advance_to(lb); // ensure_entry_if_complete() leaves the cursor invalid. Bring back to valid.
                    // If !ropt, it means there is no entry at lb, so cur is guaranteed to be at a position
                    // greater than lb. No need to advance it.
                    if (ropt) {
                        cur.next();
                    }
                    position_in_partition::less_compare less(s);
                    SCYLLA_ASSERT(less(lb, cur.position()));
                    while (less(cur.position(), src_cur.position())) {
                        auto res = cur.ensure_entry_in_latest();
                        if (cur.continuous()) {
                            SCYLLA_ASSERT(cur.dummy() || cur.range_tombstone_for_row() == cur.range_tombstone());
                            res.row.set_continuous(is_continuous::yes);
                        }
                        res.row.set_range_tombstone(cur.range_tombstone_for_row() + src_cur.range_tombstone());

                        // FIXME: Compact the row
                        ++tracker.get_stats().rows_covered_by_range_tombstones_from_memtable;
                        cur.next();
                        // FIXME: preempt
                    }
                }
                {
                    if (src_cur.dummy()) {
                        ++tracker.get_stats().dummy_processed_from_memtable;
                    } else {
                        tracker.on_row_processed_from_memtable();
                    }
                    auto ropt = cur.ensure_entry_if_complete(src_cur.position());
                    if (ropt) {
                        if (!ropt->inserted) {
                            tracker.on_row_merged_from_memtable();
                        }
                        rows_entry& e = ropt->row;
                        if (!src_cur.dummy()) {
                            src_cur.consume_row([&](deletable_row&& row) {
                                e.row().apply_monotonically(s, std::move(row));
                            });
                        }
                        // We can set cont=1 only if there is a range tombstone because
                        // only then the lower bound of the range is ensured in the latest version earlier.
                        if (src_cur.range_tombstone()) {
                            if (cur.continuous()) {
                                SCYLLA_ASSERT(cur.dummy() || cur.range_tombstone_for_row() == cur.range_tombstone());
                                e.set_continuous(is_continuous::yes);
                            }
                            e.set_range_tombstone(cur.range_tombstone_for_row() + src_cur.range_tombstone());
                        }
                    } else {
                        tracker.on_row_dropped_from_memtable();
                    }
                }
                // FIXME: Avoid storing lb if no range tombstones
                lb = position_in_partition(src_cur.position());
                auto has_next = src_cur.erase_and_advance();
                acc.unpin_memory(size);
                if (!has_next) {
                    dst_snp->unlock();
                    return stop_iteration::yes;
                }
            } while (!preemptible || !preempt_src.should_preempt());
            return stop_iteration::no;
        });
    });
}

mutation_partition_v2 partition_entry::squashed_v2(const schema& to, is_evictable evictable)
{
    mutation_partition_v2 mp(to);
    mp.set_static_row_continuous(_version->partition().static_row_continuous());
    for (auto&& v : _version->all_elements()) {
        auto older = mutation_partition_v2(*v.get_schema(), v.partition());
        if (v.get_schema()->version() != to.version()) {
            older.upgrade(*v.get_schema(), to);
        }
        merge_versions(to, mp, std::move(older), no_cache_tracker, evictable);
    }
    return mp;
}

mutation_partition partition_entry::squashed(const schema& s, is_evictable evictable)
{
    return squashed_v2(s, evictable).as_mutation_partition(s);
}

void partition_entry::upgrade(logalloc::region& r, schema_ptr to, mutation_cleaner& cleaner, cache_tracker* tracker)
{
    with_allocator(r.allocator(), [&] {
        auto phase = partition_snapshot::max_phase;
        if (_snapshot) {
            phase = _snapshot->_phase;
        }
        // The destruction of this snapshot pointer will trigger a background merge
        // of the old version into the new version.
        partition_snapshot_ptr snp = read(r, cleaner, tracker, phase);
        add_version(*to, tracker);
    });
}

partition_snapshot_ptr partition_entry::read(logalloc::region& r,
    mutation_cleaner& cleaner, cache_tracker* tracker, partition_snapshot::phase_type phase)
{
    if (_snapshot) {
        if (_snapshot->_phase == phase) {
            return _snapshot->shared_from_this();
        } else if (phase < _snapshot->_phase) {
            // If entry is being updated, we will get reads for non-latest phase, and
            // they must attach to the non-current version.
            partition_version* second = _version->next();
            SCYLLA_ASSERT(second && second->is_referenced());
            auto snp = partition_snapshot::container_of(second->_backref).shared_from_this();
            SCYLLA_ASSERT(phase == snp->_phase);
            return snp;
        } else { // phase > _snapshot->_phase
            with_allocator(r.allocator(), [&] {
                add_version(*get_schema(), tracker);
            });
        }
    }

    auto snp = make_lw_shared<partition_snapshot>(r, cleaner, this, tracker, phase);
    _snapshot = snp.get();
    return partition_snapshot_ptr(std::move(snp));
}

void partition_snapshot::touch() noexcept {
    // Eviction assumes that older versions are evicted before newer so only the latest snapshot
    // can be touched.
    if (_tracker && at_latest_version()) {
        auto&& rows = version()->partition().clustered_rows();
        SCYLLA_ASSERT(!rows.empty());
        rows_entry& last_dummy = *rows.rbegin();
        SCYLLA_ASSERT(last_dummy.is_last_dummy());
        _tracker->touch(last_dummy);
    }
}

auto fmt::formatter<partition_entry::printer>::format(const partition_entry::printer& p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto& e = p._partition_entry;
    auto out = fmt::format_to(ctx.out(), "{{");
    bool first = true;
    if (e._version) {
        const partition_version* v = &*e._version;
        while (v) {
            if (!first) {
                out = fmt::format_to(out, ", ");
            }
            if (v->is_referenced()) {
                partition_snapshot* snp = nullptr;
                if (first) {
                    snp = e._snapshot;
                } else {
                    snp = &partition_snapshot::container_of(&v->back_reference());
                }
                out = fmt::format_to(out, "(*");
                if (snp) {
                    out = fmt::format_to(out, " snp={}, phase={}", fmt::ptr(snp), snp->phase());
                }
                out = fmt::format_to(out, ") ");
            }
            out = fmt::format_to(out, "{}: {}",
                       fmt::ptr(v), mutation_partition_v2::printer(*v->get_schema(), v->partition()));
            v = v->next();
            first = false;
        }
    }
    return fmt::format_to(out, "}}");
}

void partition_entry::evict(mutation_cleaner& cleaner) noexcept {
    if (!_version) {
        return;
    }
    if (_snapshot) {
        SCYLLA_ASSERT(!_snapshot->is_locked());
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
    } else {
        auto v = &*_version;
        _version = { };
        remove_or_mark_as_unique_owner(v, &cleaner);
    }
}

partition_snapshot_ptr::~partition_snapshot_ptr() {
    if (_snp) {
        auto&& cleaner = _snp->cleaner();
        auto snp = _snp.release();
        if (snp) {
            cleaner.merge_and_destroy(*snp.release());
        }
    }
}

void partition_snapshot::lock() noexcept {
    // partition_entry::is_locked() assumes that if there is a locked snapshot,
    // it can be found attached directly to it.
    SCYLLA_ASSERT(at_latest_version());
    _locked = true;
}

void partition_snapshot::unlock() noexcept {
    // Locked snapshots must always be latest, is_locked() assumes that.
    // Also, touch() is only effective when this snapshot is latest. 
    SCYLLA_ASSERT(at_latest_version());
    _locked = false;
    touch(); // Make the entry evictable again in case it was fully unlinked by eviction attempt.
}
