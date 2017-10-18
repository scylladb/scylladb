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

#pragma once

#include <vector>
#include "row_cache.hh"
#include "mutation_reader.hh"
#include "streamed_mutation.hh"
#include "partition_version.hh"
#include "utils/logalloc.hh"
#include "query-request.hh"
#include "partition_snapshot_reader.hh"
#include "partition_snapshot_row_cursor.hh"
#include "read_context.hh"

namespace cache {

extern logging::logger clogger;

class lsa_manager {
    row_cache& _cache;
public:
    lsa_manager(row_cache& cache) : _cache(cache) { }
    template<typename Func>
    decltype(auto) run_in_read_section(const Func& func) {
        return _cache._read_section(_cache._tracker.region(), [&func] () {
            return with_linearized_managed_bytes([&func] () {
                return func();
            });
        });
    }
    template<typename Func>
    decltype(auto) run_in_update_section(const Func& func) {
        return _cache._update_section(_cache._tracker.region(), [&func] () {
            return with_linearized_managed_bytes([&func] () {
                return func();
            });
        });
    }
    template<typename Func>
    void run_in_update_section_with_allocator(Func&& func) {
        return _cache._update_section(_cache._tracker.region(), [this, &func] () {
            return with_linearized_managed_bytes([this, &func] () {
                return with_allocator(_cache._tracker.region().allocator(), [this, &func] () mutable {
                    return func();
                });
            });
        });
    }
    logalloc::region& region() { return _cache._tracker.region(); }
    logalloc::allocating_section& read_section() { return _cache._read_section; }
};

class cache_streamed_mutation final : public streamed_mutation::impl {
    enum class state {
        before_static_row,

        // Invariants:
        //  - position_range(_lower_bound, _upper_bound) covers all not yet emitted positions from current range
        //  - if _next_row has valid iterators:
        //    - _next_row points to the nearest row in cache >= _lower_bound
        //    - _next_row_in_range = _next.position() < _upper_bound
        //  - if _next_row doesn't have valid iterators, it has no meaning.
        reading_from_cache,

        // Starts reading from underlying reader.
        // The range to read is position_range(_lower_bound, min(_next_row.position(), _upper_bound)).
        // Invariants:
        //  - _next_row_in_range = _next.position() < _upper_bound
        move_to_underlying,

        // Invariants:
        // - Upper bound of the read is min(_next_row.position(), _upper_bound)
        // - _next_row_in_range = _next.position() < _upper_bound
        // - _last_row points at a direct predecessor of the next row which is going to be read.
        //   Used for populating continuity.
        reading_from_underlying,

        end_of_stream
    };
    lw_shared_ptr<partition_snapshot> _snp;
    position_in_partition::tri_compare _position_cmp;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _ck_ranges_curr;
    query::clustering_row_ranges::const_iterator _ck_ranges_end;

    lsa_manager _lsa_manager;

    partition_snapshot_row_weakref _last_row;

    // We need to be prepared that we may get overlapping and out of order
    // range tombstones. We must emit fragments with strictly monotonic positions,
    // so we can't just trim such tombstones to the position of the last fragment.
    // To solve that, range tombstones are accumulated first in a range_tombstone_stream
    // and emitted once we have a fragment with a larger position.
    range_tombstone_stream _tombstones;

    // Holds the lower bound of a position range which hasn't been processed yet.
    // Only fragments with positions < _lower_bound have been emitted.
    //
    // It is assumed that !_lower_bound.is_clustering_row(). We depend on this when
    // calling range_tombstone::trim_front() and when inserting dummy entries. Dummy
    // entries are assumed to be only at !is_clustering_row() positions.
    position_in_partition _lower_bound;
    position_in_partition_view _upper_bound;

    state _state = state::before_static_row;
    lw_shared_ptr<read_context> _read_context;
    partition_snapshot_row_cursor _next_row;
    bool _next_row_in_range = false;

    future<> do_fill_buffer();
    void copy_from_cache_to_buffer();
    future<> process_static_row();
    void move_to_end();
    void move_to_next_range();
    void move_to_range(query::clustering_row_ranges::const_iterator);
    void move_to_next_entry();
    // Emits all delayed range tombstones with positions smaller than upper_bound.
    void drain_tombstones(position_in_partition_view upper_bound);
    // Emits all delayed range tombstones.
    void drain_tombstones();
    void add_to_buffer(const partition_snapshot_row_cursor&);
    void add_clustering_row_to_buffer(mutation_fragment&&);
    void add_to_buffer(range_tombstone&&);
    void add_to_buffer(mutation_fragment&&);
    future<> read_from_underlying();
    void start_reading_from_underlying();
    bool after_current_range(position_in_partition_view position);
    bool can_populate() const;
    void maybe_update_continuity();
    void maybe_add_to_cache(const mutation_fragment& mf);
    void maybe_add_to_cache(const clustering_row& cr);
    void maybe_add_to_cache(const range_tombstone& rt);
    void maybe_add_to_cache(const static_row& sr);
    void maybe_set_static_row_continuous();
public:
    cache_streamed_mutation(schema_ptr s,
                            dht::decorated_key dk,
                            query::clustering_key_filter_ranges&& crr,
                            lw_shared_ptr<read_context> ctx,
                            lw_shared_ptr<partition_snapshot> snp,
                            row_cache& cache)
        : streamed_mutation::impl(std::move(s), std::move(dk), snp->partition_tombstone())
        , _snp(std::move(snp))
        , _position_cmp(*_schema)
        , _ck_ranges(std::move(crr))
        , _ck_ranges_curr(_ck_ranges.begin())
        , _ck_ranges_end(_ck_ranges.end())
        , _lsa_manager(cache)
        , _tombstones(*_schema)
        , _lower_bound(position_in_partition::before_all_clustered_rows())
        , _upper_bound(position_in_partition_view::before_all_clustered_rows())
        , _read_context(std::move(ctx))
        , _next_row(*_schema, *_snp)
    {
        clogger.trace("csm {}: table={}.{}", this, _schema->ks_name(), _schema->cf_name());
    }
    cache_streamed_mutation(const cache_streamed_mutation&) = delete;
    cache_streamed_mutation(cache_streamed_mutation&&) = delete;
    virtual future<> fill_buffer() override;
    virtual ~cache_streamed_mutation() {
        maybe_merge_versions(_snp, _lsa_manager.region(), _lsa_manager.read_section());
    }
};

inline
future<> cache_streamed_mutation::process_static_row() {
    if (_snp->version()->partition().static_row_continuous()) {
        _read_context->cache().on_row_hit();
        row sr = _lsa_manager.run_in_read_section([this] {
            return _snp->static_row();
        });
        if (!sr.empty()) {
            push_mutation_fragment(mutation_fragment(static_row(std::move(sr))));
        }
        return make_ready_future<>();
    } else {
        _read_context->cache().on_row_miss();
        return _read_context->get_next_fragment().then([this] (mutation_fragment_opt&& sr) {
            if (sr) {
                assert(sr->is_static_row());
                maybe_add_to_cache(sr->as_static_row());
                push_mutation_fragment(std::move(*sr));
            }
            maybe_set_static_row_continuous();
        });
    }
}

inline
future<> cache_streamed_mutation::fill_buffer() {
    if (_state == state::before_static_row) {
        auto after_static_row = [this] {
            if (_ck_ranges_curr == _ck_ranges_end) {
                _end_of_stream = true;
                _state = state::end_of_stream;
                return make_ready_future<>();
            }
            _state = state::reading_from_cache;
            _lsa_manager.run_in_read_section([this] {
                move_to_range(_ck_ranges_curr);
            });
            return fill_buffer();
        };
        if (_schema->has_static_columns()) {
            return process_static_row().then(std::move(after_static_row));
        } else {
            return after_static_row();
        }
    }
    clogger.trace("csm {}: fill_buffer(), range={}, lb={}", this, *_ck_ranges_curr, _lower_bound);
    return do_until([this] { return _end_of_stream || is_buffer_full(); }, [this] {
        return do_fill_buffer();
    });
}

inline
future<> cache_streamed_mutation::do_fill_buffer() {
    if (_state == state::move_to_underlying) {
        _state = state::reading_from_underlying;
        auto end = _next_row_in_range ? position_in_partition(_next_row.position())
                                      : position_in_partition(_upper_bound);
        return _read_context->fast_forward_to(position_range{_lower_bound, std::move(end)}).then([this] {
            return read_from_underlying();
        });
    }
    if (_state == state::reading_from_underlying) {
        return read_from_underlying();
    }
    // assert(_state == state::reading_from_cache)
    return _lsa_manager.run_in_read_section([this] {
        auto next_valid = _next_row.iterators_valid();
        clogger.trace("csm {}: reading_from_cache, range=[{}, {}), next={}, valid={}", this, _lower_bound,
            _upper_bound, _next_row.position(), next_valid);
        // We assume that if there was eviction, and thus the range may
        // no longer be continuous, the cursor was invalidated.
        if (!next_valid) {
            auto adjacent = _next_row.advance_to(_lower_bound);
            _next_row_in_range = !after_current_range(_next_row.position());
            if (!adjacent && !_next_row.continuous()) {
                _last_row = nullptr; // We could insert a dummy here, but this path is unlikely.
                start_reading_from_underlying();
                return make_ready_future<>();
            }
        }
        _next_row.maybe_refresh();
        clogger.trace("csm {}: next={}, cont={}", this, _next_row.position(), _next_row.continuous());
        while (!is_buffer_full() && _state == state::reading_from_cache) {
            copy_from_cache_to_buffer();
            if (need_preempt()) {
                break;
            }
        }
        return make_ready_future<>();
    });
}

inline
future<> cache_streamed_mutation::read_from_underlying() {
    return consume_mutation_fragments_until(_read_context->get_streamed_mutation(),
        [this] { return _state != state::reading_from_underlying || is_buffer_full(); },
        [this] (mutation_fragment mf) {
            _read_context->cache().on_row_miss();
            maybe_add_to_cache(mf);
            add_to_buffer(std::move(mf));
        },
        [this] {
            _state = state::reading_from_cache;
            _lsa_manager.run_in_update_section([this] {
                auto same_pos = _next_row.maybe_refresh();
                if (!same_pos) {
                    _read_context->cache().on_mispopulate(); // FIXME: Insert dummy entry at _upper_bound.
                    _next_row_in_range = !after_current_range(_next_row.position());
                    if (!_next_row.continuous()) {
                        start_reading_from_underlying();
                    }
                    return;
                }
                if (_next_row_in_range) {
                    maybe_update_continuity();
                    _last_row = _next_row;
                    add_to_buffer(_next_row);
                    try {
                        move_to_next_entry();
                    } catch (const std::bad_alloc&) {
                        // We cannot reenter the section, since we may have moved to the new range, and
                        // because add_to_buffer() should not be repeated.
                        _snp->region().allocator().invalidate_references(); // Invalidates _next_row
                    }
                } else {
                    if (no_clustering_row_between(*_schema, _upper_bound, _next_row.position())) {
                        this->maybe_update_continuity();
                    } else if (can_populate()) {
                        rows_entry::compare less(*_schema);
                        auto& rows = _snp->version()->partition().clustered_rows();
                        if (query::is_single_row(*_schema, *_ck_ranges_curr)) {
                            with_allocator(_snp->region().allocator(), [&] {
                                auto e = alloc_strategy_unique_ptr<rows_entry>(
                                    current_allocator().construct<rows_entry>(_ck_ranges_curr->start()->value()));
                                // Use _next_row iterator only as a hint, because there could be insertions after _upper_bound.
                                auto insert_result = rows.insert_check(_next_row.get_iterator_in_latest_version(), *e, less);
                                auto inserted = insert_result.second;
                                auto it = insert_result.first;
                                if (inserted) {
                                    e.release();
                                    auto next = std::next(it);
                                    it->set_continuous(next->continuous());
                                    clogger.trace("csm {}: inserted dummy at {}, cont={}", this, it->position(), it->continuous());
                                }
                            });
                        } else if (!_ck_ranges_curr->start() || _last_row.refresh(*_snp)) {
                            with_allocator(_snp->region().allocator(), [&] {
                                auto e = alloc_strategy_unique_ptr<rows_entry>(
                                    current_allocator().construct<rows_entry>(*_schema, _upper_bound, is_dummy::yes, is_continuous::yes));
                                // Use _next_row iterator only as a hint, because there could be insertions after _upper_bound.
                                auto insert_result = rows.insert_check(_next_row.get_iterator_in_latest_version(), *e, less);
                                auto inserted = insert_result.second;
                                if (inserted) {
                                    clogger.trace("csm {}: inserted dummy at {}", this, _upper_bound);
                                    e.release();
                                } else {
                                    clogger.trace("csm {}: mark {} as continuous", this, insert_result.first->position());
                                    insert_result.first->set_continuous(true);
                                }
                            });
                        }
                    } else {
                        _read_context->cache().on_mispopulate();
                    }
                    try {
                        move_to_next_range();
                    } catch (const std::bad_alloc&) {
                        // We cannot reenter the section, since we may have moved to the new range
                        _snp->region().allocator().invalidate_references(); // Invalidates _next_row
                    }
                }
            });
            return make_ready_future<>();
        });
}

inline
void cache_streamed_mutation::maybe_update_continuity() {
    if (can_populate() && (!_ck_ranges_curr->start() || _last_row.refresh(*_snp))) {
            if (_next_row.is_in_latest_version()) {
                clogger.trace("csm {}: mark {} continuous", this, _next_row.get_iterator_in_latest_version()->position());
                _next_row.get_iterator_in_latest_version()->set_continuous(true);
            } else {
                // Cover entry from older version
                with_allocator(_snp->region().allocator(), [&] {
                    auto& rows = _snp->version()->partition().clustered_rows();
                    rows_entry::compare less(*_schema);
                    auto e = alloc_strategy_unique_ptr<rows_entry>(
                        current_allocator().construct<rows_entry>(*_schema, _next_row.position(), is_dummy(_next_row.dummy()), is_continuous::yes));
                    auto insert_result = rows.insert_check(_next_row.get_iterator_in_latest_version(), *e, less);
                    auto inserted = insert_result.second;
                    if (inserted) {
                        clogger.trace("csm {}: inserted dummy at {}", this, e->position());
                        e.release();
                    }
                });
            }
    } else {
        _read_context->cache().on_mispopulate();
    }
}

inline
void cache_streamed_mutation::maybe_add_to_cache(const mutation_fragment& mf) {
    if (mf.is_range_tombstone()) {
        maybe_add_to_cache(mf.as_range_tombstone());
    } else {
        assert(mf.is_clustering_row());
        const clustering_row& cr = mf.as_clustering_row();
        maybe_add_to_cache(cr);
    }
}

inline
void cache_streamed_mutation::maybe_add_to_cache(const clustering_row& cr) {
    if (!can_populate()) {
        _last_row = nullptr;
        _read_context->cache().on_mispopulate();
        return;
    }
    clogger.trace("csm {}: populate({})", this, cr);
    _lsa_manager.run_in_update_section_with_allocator([this, &cr] {
        mutation_partition& mp = _snp->version()->partition();
        rows_entry::compare less(*_schema);

        auto new_entry = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(cr.key(), cr.tomb(), cr.marker(), cr.cells()));
        new_entry->set_continuous(false);
        auto it = _next_row.iterators_valid() ? _next_row.get_iterator_in_latest_version()
                                              : mp.clustered_rows().lower_bound(cr.key(), less);
        auto insert_result = mp.clustered_rows().insert_check(it, *new_entry, less);
        if (insert_result.second) {
            _read_context->cache().on_row_insert();
            new_entry.release();
        }
        it = insert_result.first;

        rows_entry& e = *it;
        if (!_ck_ranges_curr->start() || _last_row.refresh(*_snp)) {
            clogger.trace("csm {}: set_continuous({})", this, e.position());
            e.set_continuous(true);
        } else {
            _read_context->cache().on_mispopulate();
        }
        with_allocator(standard_allocator(), [&] {
            _last_row = partition_snapshot_row_weakref(*_snp, it);
        });
    });
}

inline
bool cache_streamed_mutation::after_current_range(position_in_partition_view p) {
    return _position_cmp(p, _upper_bound) >= 0;
}

inline
void cache_streamed_mutation::start_reading_from_underlying() {
    clogger.trace("csm {}: start_reading_from_underlying(), range=[{}, {})", this, _lower_bound, _next_row_in_range ? _next_row.position() : _upper_bound);
    _state = state::move_to_underlying;
}

inline
void cache_streamed_mutation::copy_from_cache_to_buffer() {
    clogger.trace("csm {}: copy_from_cache, next={}, next_row_in_range={}", this, _next_row.position(), _next_row_in_range);
    position_in_partition_view next_lower_bound = _next_row.dummy() ? _next_row.position() : position_in_partition_view::after_key(_next_row.key());
    for (auto&& rts : _snp->range_tombstones(*_schema, _lower_bound, _next_row_in_range ? next_lower_bound : _upper_bound)) {
        add_to_buffer(std::move(rts));
        if (is_buffer_full()) {
            return;
        }
    }
    if (_next_row_in_range) {
        _last_row = _next_row;
        add_to_buffer(_next_row);
        move_to_next_entry();
    } else {
        move_to_next_range();
    }
}

inline
void cache_streamed_mutation::move_to_end() {
    drain_tombstones();
    _end_of_stream = true;
    _state = state::end_of_stream;
    clogger.trace("csm {}: eos", this);
}

inline
void cache_streamed_mutation::move_to_next_range() {
    auto next_it = std::next(_ck_ranges_curr);
    if (next_it == _ck_ranges_end) {
        move_to_end();
        _ck_ranges_curr = next_it;
    } else {
        move_to_range(next_it);
    }
}

inline
void cache_streamed_mutation::move_to_range(query::clustering_row_ranges::const_iterator next_it) {
    auto lb = position_in_partition::for_range_start(*next_it);
    auto ub = position_in_partition_view::for_range_end(*next_it);
    _last_row = nullptr;
    _lower_bound = std::move(lb);
    _upper_bound = std::move(ub);
    _ck_ranges_curr = next_it;
    auto adjacent = _next_row.advance_to(_lower_bound);
    _next_row_in_range = !after_current_range(_next_row.position());
    clogger.trace("csm {}: move_to_range(), range={}, lb={}, ub={}, next={}", this, *_ck_ranges_curr, _lower_bound, _upper_bound, _next_row.position());
    if (!adjacent && !_next_row.continuous()) {
        // FIXME: We don't insert a dummy for singular range to avoid allocating 3 entries
        // for a hit (before, at and after). If we supported the concept of an incomplete row,
        // we could insert such a row for the lower bound if it's full instead, for both singular and
        // non-singular ranges.
        if (_ck_ranges_curr->start() && !query::is_single_row(*_schema, *_ck_ranges_curr)) {
            // Insert dummy for lower bound
            if (can_populate()) {
                // FIXME: _lower_bound could be adjacent to the previous row, in which case we could skip this
                clogger.trace("csm {}: insert dummy at {}", this, _lower_bound);
                auto it = with_allocator(_lsa_manager.region().allocator(), [&] {
                    auto& rows = _snp->version()->partition().clustered_rows();
                    auto new_entry = current_allocator().construct<rows_entry>(*_schema, _lower_bound, is_dummy::yes, is_continuous::no);
                    return rows.insert_before(_next_row.get_iterator_in_latest_version(), *new_entry);
                });
                _last_row = partition_snapshot_row_weakref(*_snp, it);
            } else {
                _read_context->cache().on_mispopulate();
            }
        }
        start_reading_from_underlying();
    }
}

// _next_row must be inside the range.
inline
void cache_streamed_mutation::move_to_next_entry() {
    clogger.trace("csm {}: move_to_next_entry(), curr={}", this, _next_row.position());
    if (no_clustering_row_between(*_schema, _next_row.position(), _upper_bound)) {
        move_to_next_range();
    } else {
        if (!_next_row.next()) {
            move_to_end();
            return;
        }
        _next_row_in_range = !after_current_range(_next_row.position());
        clogger.trace("csm {}: next={}, cont={}, in_range={}", this, _next_row.position(), _next_row.continuous(), _next_row_in_range);
        if (!_next_row.continuous()) {
            start_reading_from_underlying();
        }
    }
}

inline
void cache_streamed_mutation::drain_tombstones(position_in_partition_view pos) {
    while (true) {
        reserve_one();
        auto mfo = _tombstones.get_next(pos);
        if (!mfo) {
            break;
        }
        push_mutation_fragment(std::move(*mfo));
    }
}

inline
void cache_streamed_mutation::drain_tombstones() {
    while (true) {
        reserve_one();
        auto mfo = _tombstones.get_next();
        if (!mfo) {
            break;
        }
        push_mutation_fragment(std::move(*mfo));
    }
}

inline
void cache_streamed_mutation::add_to_buffer(mutation_fragment&& mf) {
    clogger.trace("csm {}: add_to_buffer({})", this, mf);
    if (mf.is_clustering_row()) {
        add_clustering_row_to_buffer(std::move(mf));
    } else {
        assert(mf.is_range_tombstone());
        add_to_buffer(std::move(mf).as_range_tombstone());
    }
}

inline
void cache_streamed_mutation::add_to_buffer(const partition_snapshot_row_cursor& row) {
    if (!row.dummy()) {
        _read_context->cache().on_row_hit();
        add_clustering_row_to_buffer(row.row());
    }
}

// Maintains the following invariants, also in case of exception:
//   (1) no fragment with position >= _lower_bound was pushed yet
//   (2) If _lower_bound > mf.position(), mf was emitted
inline
void cache_streamed_mutation::add_clustering_row_to_buffer(mutation_fragment&& mf) {
    clogger.trace("csm {}: add_clustering_row_to_buffer({})", this, mf);
    auto& row = mf.as_clustering_row();
    auto key = row.key();
    try {
        drain_tombstones(row.position());
        push_mutation_fragment(std::move(mf));
        _lower_bound = position_in_partition::after_key(std::move(key));
    } catch (...) {
        // We may have emitted some of the range tombstones which start after the old _lower_bound
        _lower_bound = position_in_partition::for_key(std::move(key));
        throw;
    }
}

inline
void cache_streamed_mutation::add_to_buffer(range_tombstone&& rt) {
    clogger.trace("csm {}: add_to_buffer({})", this, rt);
    // This guarantees that rt starts after any emitted clustering_row
    if (!rt.trim_front(*_schema, _lower_bound)) {
        return;
    }
    _lower_bound = position_in_partition(rt.position());
    _tombstones.apply(std::move(rt));
    drain_tombstones(_lower_bound);
}

inline
void cache_streamed_mutation::maybe_add_to_cache(const range_tombstone& rt) {
    if (can_populate()) {
        clogger.trace("csm {}: maybe_add_to_cache({})", this, rt);
        _lsa_manager.run_in_update_section_with_allocator([&] {
            _snp->version()->partition().row_tombstones().apply_monotonically(*_schema, rt);
        });
    } else {
        _read_context->cache().on_mispopulate();
    }
}

inline
void cache_streamed_mutation::maybe_add_to_cache(const static_row& sr) {
    if (can_populate()) {
        clogger.trace("csm {}: populate({})", this, sr);
        _read_context->cache().on_row_insert();
        _lsa_manager.run_in_update_section_with_allocator([&] {
            _snp->version()->partition().static_row().apply(*_schema, column_kind::static_column, sr.cells());
        });
    } else {
        _read_context->cache().on_mispopulate();
    }
}

inline
void cache_streamed_mutation::maybe_set_static_row_continuous() {
    if (can_populate()) {
        clogger.trace("csm {}: set static row continuous", this);
        _snp->version()->partition().set_static_row_continuous(true);
    } else {
        _read_context->cache().on_mispopulate();
    }
}

inline
bool cache_streamed_mutation::can_populate() const {
    return _snp->at_latest_version() && _read_context->cache().phase_of(_read_context->key()) == _read_context->phase();
}

} // namespace cache

inline streamed_mutation make_cache_streamed_mutation(schema_ptr s,
                                                      dht::decorated_key dk,
                                                      query::clustering_key_filter_ranges crr,
                                                      row_cache& cache,
                                                      lw_shared_ptr<cache::read_context> ctx,
                                                      lw_shared_ptr<partition_snapshot> snp)
{
    return make_streamed_mutation<cache::cache_streamed_mutation>(
        std::move(s), std::move(dk), std::move(crr), std::move(ctx), std::move(snp), cache);
}
