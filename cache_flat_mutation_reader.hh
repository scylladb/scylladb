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

#pragma once

#include <vector>
#include "row_cache.hh"
#include "mutation_reader.hh"
#include "mutation_fragment.hh"
#include "partition_version.hh"
#include "utils/logalloc.hh"
#include "query-request.hh"
#include "partition_snapshot_row_cursor.hh"
#include "read_context.hh"
#include "flat_mutation_reader.hh"

namespace cache {

extern logging::logger clogger;

class cache_flat_mutation_reader final : public flat_mutation_reader::impl {
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
        // - _population_range_starts_before_all_rows is set accordingly
        // - _underlying is engaged and fast-forwarded
        reading_from_underlying,

        end_of_stream
    };
    partition_snapshot_ptr _snp;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _ck_ranges_curr;
    query::clustering_row_ranges::const_iterator _ck_ranges_end;

    lsa_manager _lsa_manager;

    partition_snapshot_row_weakref _last_row;

    // Holds the lower bound of a position range which hasn't been processed yet.
    // Only rows with positions < _lower_bound have been emitted, and only
    // range_tombstones with positions <= _lower_bound.
    position_in_partition _lower_bound;
    position_in_partition_view _upper_bound;

    // cache_flat_mutation_reader may be constructed either
    // with a read_context&, where it knows that the read_context
    // is owned externally, by the caller.  In this case
    // _read_context_holder would be disengaged.
    // Or, it could be constructed with a std::unique_ptr<read_context>,
    // in which case it assumes ownership of the read_context
    // and it is now responsible for closing it.
    // In this case, _read_context_holder would be engaged
    // and _read_context will reference its content.
    std::unique_ptr<read_context> _read_context_holder;
    read_context& _read_context;
    partition_snapshot_row_cursor _next_row;

    state _state = state::before_static_row;

    bool _next_row_in_range = false;

    // True iff current population interval, since the previous clustering row, starts before all clustered rows.
    // We cannot just look at _lower_bound, because emission of range tombstones changes _lower_bound and
    // because we mark clustering intervals as continuous when consuming a clustering_row, it would prevent
    // us from marking the interval as continuous.
    // Valid when _state == reading_from_underlying.
    bool _population_range_starts_before_all_rows;

    // Whether _lower_bound was changed within current fill_buffer().
    // If it did not then we cannot break out of it (e.g. on preemption) because
    // forward progress is not guaranteed in case iterators are getting constantly invalidated.
    bool _lower_bound_changed = false;

    // Points to the underlying reader conforming to _schema,
    // either to *_underlying_holder or _read_context.underlying().underlying().
    flat_mutation_reader* _underlying = nullptr;
    flat_mutation_reader_opt _underlying_holder;

    future<> do_fill_buffer(db::timeout_clock::time_point);
    future<> ensure_underlying(db::timeout_clock::time_point);
    void copy_from_cache_to_buffer();
    future<> process_static_row(db::timeout_clock::time_point);
    void move_to_end();
    void move_to_next_range();
    void move_to_range(query::clustering_row_ranges::const_iterator);
    void move_to_next_entry();
    void maybe_drop_last_entry() noexcept;
    void add_to_buffer(const partition_snapshot_row_cursor&);
    void add_clustering_row_to_buffer(mutation_fragment&&);
    void add_to_buffer(range_tombstone&&);
    void add_range_tombstone_to_buffer(range_tombstone&&);
    void add_to_buffer(mutation_fragment&&);
    future<> read_from_underlying(db::timeout_clock::time_point);
    void start_reading_from_underlying();
    bool after_current_range(position_in_partition_view position);
    bool can_populate() const;
    // Marks the range between _last_row (exclusive) and _next_row (exclusive) as continuous,
    // provided that the underlying reader still matches the latest version of the partition.
    // Invalidates _last_row.
    void maybe_update_continuity();
    // Tries to ensure that the lower bound of the current population range exists.
    // Returns false if it failed and range cannot be populated.
    // Assumes can_populate().
    bool ensure_population_lower_bound();
    void maybe_add_to_cache(const mutation_fragment& mf);
    void maybe_add_to_cache(const clustering_row& cr);
    void maybe_add_to_cache(const range_tombstone& rt);
    void maybe_add_to_cache(const static_row& sr);
    void maybe_set_static_row_continuous();
    void finish_reader() {
        push_mutation_fragment(*_schema, _permit, partition_end());
        _end_of_stream = true;
        _state = state::end_of_stream;
    }
    void touch_partition();
public:
    cache_flat_mutation_reader(schema_ptr s,
                               dht::decorated_key dk,
                               query::clustering_key_filter_ranges&& crr,
                               read_context& ctx,
                               partition_snapshot_ptr snp,
                               row_cache& cache)
        : flat_mutation_reader::impl(std::move(s), ctx.permit())
        , _snp(std::move(snp))
        , _ck_ranges(std::move(crr))
        , _ck_ranges_curr(_ck_ranges.begin())
        , _ck_ranges_end(_ck_ranges.end())
        , _lsa_manager(cache)
        , _lower_bound(position_in_partition::before_all_clustered_rows())
        , _upper_bound(position_in_partition_view::before_all_clustered_rows())
        , _read_context_holder()
        , _read_context(ctx)    // ctx is owned by the caller, who's responsible for closing it.
        , _next_row(*_schema, *_snp)
    {
        clogger.trace("csm {}: table={}.{}", fmt::ptr(this), _schema->ks_name(), _schema->cf_name());
        push_mutation_fragment(*_schema, _permit, partition_start(std::move(dk), _snp->partition_tombstone()));
    }
    cache_flat_mutation_reader(schema_ptr s,
                               dht::decorated_key dk,
                               query::clustering_key_filter_ranges&& crr,
                               std::unique_ptr<read_context> unique_ctx,
                               partition_snapshot_ptr snp,
                               row_cache& cache)
        : cache_flat_mutation_reader(s, std::move(dk), std::move(crr), *unique_ctx, std::move(snp), cache)
    {
        // Assume ownership of the read_context.
        // It is our responsibility to close it now.
        _read_context_holder = std::move(unique_ctx);
    }
    cache_flat_mutation_reader(const cache_flat_mutation_reader&) = delete;
    cache_flat_mutation_reader(cache_flat_mutation_reader&&) = delete;
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = true;
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _end_of_stream = true;
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept {
        auto close_read_context = _read_context_holder ?  _read_context_holder->close() : make_ready_future<>();
        auto close_underlying = _underlying_holder ? _underlying_holder->close() : make_ready_future<>();
        return when_all_succeed(std::move(close_read_context), std::move(close_underlying)).discard_result();
    }
};

inline
future<> cache_flat_mutation_reader::process_static_row(db::timeout_clock::time_point timeout) {
    if (_snp->static_row_continuous()) {
        _read_context.cache().on_row_hit();
        static_row sr = _lsa_manager.run_in_read_section([this] {
            return _snp->static_row(_read_context.digest_requested());
        });
        if (!sr.empty()) {
            push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(sr)));
        }
        return make_ready_future<>();
    } else {
        _read_context.cache().on_row_miss();
        return ensure_underlying(timeout).then([this, timeout] {
            return (*_underlying)(timeout).then([this] (mutation_fragment_opt&& sr) {
                if (sr) {
                    assert(sr->is_static_row());
                    maybe_add_to_cache(sr->as_static_row());
                    push_mutation_fragment(std::move(*sr));
                }
                maybe_set_static_row_continuous();
            });
        });
    }
}

inline
void cache_flat_mutation_reader::touch_partition() {
    _snp->touch();
}

inline
future<> cache_flat_mutation_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    if (_state == state::before_static_row) {
        touch_partition();
        auto after_static_row = [this, timeout] {
            if (_ck_ranges_curr == _ck_ranges_end) {
                finish_reader();
                return make_ready_future<>();
            }
            _state = state::reading_from_cache;
            _lsa_manager.run_in_read_section([this] {
                move_to_range(_ck_ranges_curr);
            });
            return fill_buffer(timeout);
        };
        if (_schema->has_static_columns()) {
            return process_static_row(timeout).then(std::move(after_static_row));
        } else {
            return after_static_row();
        }
    }
    clogger.trace("csm {}: fill_buffer(), range={}, lb={}", fmt::ptr(this), *_ck_ranges_curr, _lower_bound);
    return do_until([this] { return _end_of_stream || is_buffer_full(); }, [this, timeout] {
        return do_fill_buffer(timeout);
    });
}

inline
future<> cache_flat_mutation_reader::ensure_underlying(db::timeout_clock::time_point timeout) {
    if (_underlying) {
        return make_ready_future<>();
    }
    return _read_context.ensure_underlying(timeout).then([this] {
        flat_mutation_reader& ctx_underlying = _read_context.underlying().underlying();
        if (ctx_underlying.schema() != _schema) {
            _underlying_holder = make_delegating_reader(ctx_underlying);
            _underlying_holder->upgrade_schema(_schema);
            _underlying = &*_underlying_holder;
        } else {
            _underlying = &ctx_underlying;
        }
    });
}

inline
future<> cache_flat_mutation_reader::do_fill_buffer(db::timeout_clock::time_point timeout) {
    if (_state == state::move_to_underlying) {
        if (!_underlying) {
            return ensure_underlying(timeout).then([this, timeout] {
                return do_fill_buffer(timeout);
            });
        }
        _state = state::reading_from_underlying;
        _population_range_starts_before_all_rows = _lower_bound.is_before_all_clustered_rows(*_schema);
        if (!_read_context.partition_exists()) {
            return read_from_underlying(timeout);
        }
        auto end = _next_row_in_range ? position_in_partition(_next_row.position())
                                      : position_in_partition(_upper_bound);
        return _underlying->fast_forward_to(position_range{_lower_bound, std::move(end)}, timeout).then([this, timeout] {
            return read_from_underlying(timeout);
        });
    }
    if (_state == state::reading_from_underlying) {
        return read_from_underlying(timeout);
    }
    // assert(_state == state::reading_from_cache)
    return _lsa_manager.run_in_read_section([this] {
        auto next_valid = _next_row.iterators_valid();
        clogger.trace("csm {}: reading_from_cache, range=[{}, {}), next={}, valid={}", fmt::ptr(this), _lower_bound,
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
        clogger.trace("csm {}: next={}, cont={}", fmt::ptr(this), _next_row.position(), _next_row.continuous());
        _lower_bound_changed = false;
        while (_state == state::reading_from_cache) {
            copy_from_cache_to_buffer();
            // We need to check _lower_bound_changed even if is_buffer_full() because
            // we may have emitted only a range tombstone which overlapped with _lower_bound
            // and thus didn't cause _lower_bound to change.
            if ((need_preempt() || is_buffer_full()) && _lower_bound_changed) {
                break;
            }
        }
        return make_ready_future<>();
    });
}

inline
future<> cache_flat_mutation_reader::read_from_underlying(db::timeout_clock::time_point timeout) {
    return consume_mutation_fragments_until(*_underlying,
        [this] { return _state != state::reading_from_underlying || is_buffer_full(); },
        [this] (mutation_fragment mf) {
            _read_context.cache().on_row_miss();
            maybe_add_to_cache(mf);
            add_to_buffer(std::move(mf));
        },
        [this] {
            _state = state::reading_from_cache;
            _lsa_manager.run_in_update_section([this] {
                auto same_pos = _next_row.maybe_refresh();
                if (!same_pos) {
                    _read_context.cache().on_mispopulate(); // FIXME: Insert dummy entry at _upper_bound.
                    _next_row_in_range = !after_current_range(_next_row.position());
                    if (!_next_row.continuous()) {
                        start_reading_from_underlying();
                    }
                    return;
                }
                if (_next_row_in_range) {
                    maybe_update_continuity();
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
                        rows_entry::tri_compare cmp(*_schema);
                        auto& rows = _snp->version()->partition().clustered_rows();
                        if (query::is_single_row(*_schema, *_ck_ranges_curr)) {
                            with_allocator(_snp->region().allocator(), [&] {
                                auto e = alloc_strategy_unique_ptr<rows_entry>(
                                    current_allocator().construct<rows_entry>(_ck_ranges_curr->start()->value()));
                                // Use _next_row iterator only as a hint, because there could be insertions after _upper_bound.
                                auto insert_result = rows.insert_before_hint(_next_row.get_iterator_in_latest_version(), std::move(e), cmp);
                                if (insert_result.second) {
                                    auto it = insert_result.first;
                                    _snp->tracker()->insert(*it);
                                    auto next = std::next(it);
                                    it->set_continuous(next->continuous());
                                    clogger.trace("csm {}: inserted empty row at {}, cont={}", fmt::ptr(this), it->position(), it->continuous());
                                }
                            });
                        } else if (ensure_population_lower_bound()) {
                            with_allocator(_snp->region().allocator(), [&] {
                                auto e = alloc_strategy_unique_ptr<rows_entry>(
                                    current_allocator().construct<rows_entry>(*_schema, _upper_bound, is_dummy::yes, is_continuous::yes));
                                // Use _next_row iterator only as a hint, because there could be insertions after _upper_bound.
                                auto insert_result = rows.insert_before_hint(_next_row.get_iterator_in_latest_version(), std::move(e), cmp);
                                if (insert_result.second) {
                                    clogger.trace("csm {}: inserted dummy at {}", fmt::ptr(this), _upper_bound);
                                    _snp->tracker()->insert(*insert_result.first);
                                } else {
                                    clogger.trace("csm {}: mark {} as continuous", fmt::ptr(this), insert_result.first->position());
                                    insert_result.first->set_continuous(true);
                                }
                                maybe_drop_last_entry();
                            });
                        }
                    } else {
                        _read_context.cache().on_mispopulate();
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
        }, timeout);
}

inline
bool cache_flat_mutation_reader::ensure_population_lower_bound() {
    if (_population_range_starts_before_all_rows) {
        return true;
    }
    if (!_last_row.refresh(*_snp)) {
        return false;
    }
    // Continuity flag we will later set for the upper bound extends to the previous row in the same version,
    // so we need to ensure we have an entry in the latest version.
    if (!_last_row.is_in_latest_version()) {
        with_allocator(_snp->region().allocator(), [&] {
            auto& rows = _snp->version()->partition().clustered_rows();
            rows_entry::tri_compare cmp(*_schema);
            // FIXME: Avoid the copy by inserting an incomplete clustering row
            auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(*_schema, *_last_row));
            e->set_continuous(false);
            auto insert_result = rows.insert_before_hint(rows.end(), std::move(e), cmp);
            if (insert_result.second) {
                auto it = insert_result.first;
                clogger.trace("csm {}: inserted lower bound dummy at {}", fmt::ptr(this), it->position());
                _snp->tracker()->insert(*it);
            }
        });
    }
    return true;
}

inline
void cache_flat_mutation_reader::maybe_update_continuity() {
    if (can_populate() && ensure_population_lower_bound()) {
        with_allocator(_snp->region().allocator(), [&] {
            rows_entry& e = _next_row.ensure_entry_in_latest().row;
            e.set_continuous(true);
            maybe_drop_last_entry();
        });
    } else {
        _read_context.cache().on_mispopulate();
    }
}

inline
void cache_flat_mutation_reader::maybe_add_to_cache(const mutation_fragment& mf) {
    if (mf.is_range_tombstone()) {
        maybe_add_to_cache(mf.as_range_tombstone());
    } else {
        assert(mf.is_clustering_row());
        const clustering_row& cr = mf.as_clustering_row();
        maybe_add_to_cache(cr);
    }
}

inline
void cache_flat_mutation_reader::maybe_add_to_cache(const clustering_row& cr) {
    if (!can_populate()) {
        _last_row = nullptr;
        _population_range_starts_before_all_rows = false;
        _read_context.cache().on_mispopulate();
        return;
    }
    clogger.trace("csm {}: populate({})", fmt::ptr(this), clustering_row::printer(*_schema, cr));
    _lsa_manager.run_in_update_section_with_allocator([this, &cr] {
        mutation_partition& mp = _snp->version()->partition();
        rows_entry::tri_compare cmp(*_schema);

        if (_read_context.digest_requested()) {
            cr.cells().prepare_hash(*_schema, column_kind::regular_column);
        }
        auto new_entry = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(*_schema, cr.key(), cr.as_deletable_row()));
        new_entry->set_continuous(false);
        auto it = _next_row.iterators_valid() ? _next_row.get_iterator_in_latest_version()
                                              : mp.clustered_rows().lower_bound(cr.key(), cmp);
        auto insert_result = mp.clustered_rows().insert_before_hint(it, std::move(new_entry), cmp);
        it = insert_result.first;
        if (insert_result.second) {
            _snp->tracker()->insert(*it);
        }

        rows_entry& e = *it;
        if (ensure_population_lower_bound()) {
            clogger.trace("csm {}: set_continuous({})", fmt::ptr(this), e.position());
            e.set_continuous(true);
        } else {
            _read_context.cache().on_mispopulate();
        }
        with_allocator(standard_allocator(), [&] {
            _last_row = partition_snapshot_row_weakref(*_snp, it, true);
        });
        _population_range_starts_before_all_rows = false;
    });
}

inline
bool cache_flat_mutation_reader::after_current_range(position_in_partition_view p) {
    position_in_partition::tri_compare cmp(*_schema);
    return cmp(p, _upper_bound) >= 0;
}

inline
void cache_flat_mutation_reader::start_reading_from_underlying() {
    clogger.trace("csm {}: start_reading_from_underlying(), range=[{}, {})", fmt::ptr(this), _lower_bound, _next_row_in_range ? _next_row.position() : _upper_bound);
    _state = state::move_to_underlying;
    _next_row.touch();
}

inline
void cache_flat_mutation_reader::copy_from_cache_to_buffer() {
    clogger.trace("csm {}: copy_from_cache, next={}, next_row_in_range={}", fmt::ptr(this), _next_row.position(), _next_row_in_range);
    _next_row.touch();
    position_in_partition_view next_lower_bound = _next_row.dummy() ? _next_row.position() : position_in_partition_view::after_key(_next_row.key());
    auto upper_bound = _next_row_in_range ? next_lower_bound : _upper_bound;
    for (auto &&rts : _snp->range_tombstones(_lower_bound, upper_bound)) {
        position_in_partition::less_compare less(*_schema);
        // Avoid emitting overlapping range tombstones for performance reasons.
        if (less(upper_bound, rts.end_position())) {
            rts.set_end(*_schema, upper_bound);
        }
        add_range_tombstone_to_buffer(std::move(rts));
        if (_lower_bound_changed && is_buffer_full()) {
            return;
        }
    }
    // We add the row to the buffer even when it's full.
    // This simplifies the code. For more info see #3139.
    if (_next_row_in_range) {
        add_to_buffer(_next_row);
        move_to_next_entry();
    } else {
        move_to_next_range();
    }
}

inline
void cache_flat_mutation_reader::move_to_end() {
    finish_reader();
    clogger.trace("csm {}: eos", fmt::ptr(this));
}

inline
void cache_flat_mutation_reader::move_to_next_range() {
    auto next_it = std::next(_ck_ranges_curr);
    if (next_it == _ck_ranges_end) {
        move_to_end();
        _ck_ranges_curr = next_it;
    } else {
        move_to_range(next_it);
    }
}

inline
void cache_flat_mutation_reader::move_to_range(query::clustering_row_ranges::const_iterator next_it) {
    auto lb = position_in_partition::for_range_start(*next_it);
    auto ub = position_in_partition_view::for_range_end(*next_it);
    _last_row = nullptr;
    _lower_bound = std::move(lb);
    _upper_bound = std::move(ub);
    _lower_bound_changed = true;
    _ck_ranges_curr = next_it;
    auto adjacent = _next_row.advance_to(_lower_bound);
    _next_row_in_range = !after_current_range(_next_row.position());
    clogger.trace("csm {}: move_to_range(), range={}, lb={}, ub={}, next={}", fmt::ptr(this), *_ck_ranges_curr, _lower_bound, _upper_bound, _next_row.position());
    if (!adjacent && !_next_row.continuous()) {
        // FIXME: We don't insert a dummy for singular range to avoid allocating 3 entries
        // for a hit (before, at and after). If we supported the concept of an incomplete row,
        // we could insert such a row for the lower bound if it's full instead, for both singular and
        // non-singular ranges.
        if (_ck_ranges_curr->start() && !query::is_single_row(*_schema, *_ck_ranges_curr)) {
            // Insert dummy for lower bound
            if (can_populate()) {
                // FIXME: _lower_bound could be adjacent to the previous row, in which case we could skip this
                clogger.trace("csm {}: insert dummy at {}", fmt::ptr(this), _lower_bound);
                auto it = with_allocator(_lsa_manager.region().allocator(), [&] {
                    auto& rows = _snp->version()->partition().clustered_rows();
                    auto new_entry = current_allocator().construct<rows_entry>(*_schema, _lower_bound, is_dummy::yes, is_continuous::no);
                    return rows.insert_before(_next_row.get_iterator_in_latest_version(), *new_entry);
                });
                _snp->tracker()->insert(*it);
                _last_row = partition_snapshot_row_weakref(*_snp, it, true);
            } else {
                _read_context.cache().on_mispopulate();
            }
        }
        start_reading_from_underlying();
    }
}

// Drops _last_row entry when possible without changing logical contents of the partition.
// Call only when _last_row and _next_row are valid.
// Calling after ensure_population_lower_bound() is ok.
// _next_row must have a greater position than _last_row.
// Invalidates references but keeps the _next_row valid.
inline
void cache_flat_mutation_reader::maybe_drop_last_entry() noexcept {
    // Drop dummy entry if it falls inside a continuous range.
    // This prevents unnecessary dummy entries from accumulating in cache and slowing down scans.
    //
    // Eviction can happen only from oldest versions to preserve the continuity non-overlapping rule
    // (See docs/design-notes/row_cache.md)
    //
    if (_last_row
            && _last_row->dummy()
            && _last_row->continuous()
            && _snp->at_latest_version()
            && _snp->at_oldest_version()) {

        with_allocator(_snp->region().allocator(), [&] {
            _last_row->on_evicted(_read_context.cache()._tracker);
        });
        _last_row = nullptr;

        // There could be iterators pointing to _last_row, invalidate them
        _snp->region().allocator().invalidate_references();

        // Don't invalidate _next_row, move_to_next_entry() expects it to be still valid.
        _next_row.force_valid();
    }
}

// _next_row must be inside the range.
inline
void cache_flat_mutation_reader::move_to_next_entry() {
    clogger.trace("csm {}: move_to_next_entry(), curr={}", fmt::ptr(this), _next_row.position());
    if (no_clustering_row_between(*_schema, _next_row.position(), _upper_bound)) {
        move_to_next_range();
    } else {
        auto new_last_row = partition_snapshot_row_weakref(_next_row);
        if (!_next_row.next()) {
            move_to_end();
            return;
        }
        _last_row = std::move(new_last_row);
        _next_row_in_range = !after_current_range(_next_row.position());
        clogger.trace("csm {}: next={}, cont={}, in_range={}", fmt::ptr(this), _next_row.position(), _next_row.continuous(), _next_row_in_range);
        if (!_next_row.continuous()) {
            start_reading_from_underlying();
        } else {
            maybe_drop_last_entry();
        }
    }
}

inline
void cache_flat_mutation_reader::add_to_buffer(mutation_fragment&& mf) {
    clogger.trace("csm {}: add_to_buffer({})", fmt::ptr(this), mutation_fragment::printer(*_schema, mf));
    if (mf.is_clustering_row()) {
        add_clustering_row_to_buffer(std::move(mf));
    } else {
        assert(mf.is_range_tombstone());
        add_to_buffer(std::move(mf).as_range_tombstone());
    }
}

inline
void cache_flat_mutation_reader::add_to_buffer(const partition_snapshot_row_cursor& row) {
    if (!row.dummy()) {
        _read_context.cache().on_row_hit();
        if (_read_context.digest_requested()) {
            row.latest_row().cells().prepare_hash(*_schema, column_kind::regular_column);
        }
        add_clustering_row_to_buffer(mutation_fragment(*_schema, _permit, row.row()));
    } else {
        position_in_partition::less_compare less(*_schema);
        if (less(_lower_bound, row.position())) {
            _lower_bound = row.position();
            _lower_bound_changed = true;
        }
        _read_context.cache()._tracker.on_dummy_row_hit();
    }
}

// Maintains the following invariants, also in case of exception:
//   (1) no fragment with position >= _lower_bound was pushed yet
//   (2) If _lower_bound > mf.position(), mf was emitted
inline
void cache_flat_mutation_reader::add_clustering_row_to_buffer(mutation_fragment&& mf) {
    clogger.trace("csm {}: add_clustering_row_to_buffer({})", fmt::ptr(this), mutation_fragment::printer(*_schema, mf));
    auto& row = mf.as_clustering_row();
    auto new_lower_bound = position_in_partition::after_key(row.key());
    push_mutation_fragment(std::move(mf));
    _lower_bound = std::move(new_lower_bound);
    _lower_bound_changed = true;
}

inline
void cache_flat_mutation_reader::add_to_buffer(range_tombstone&& rt) {
    clogger.trace("csm {}: add_to_buffer({})", fmt::ptr(this), rt);
    // This guarantees that rt starts after any emitted clustering_row
    // and not before any emitted range tombstone.
    position_in_partition::less_compare less(*_schema);
    if (less(_lower_bound, rt.end_position())) {
        add_range_tombstone_to_buffer(std::move(rt));
    }
}

inline
void cache_flat_mutation_reader::add_range_tombstone_to_buffer(range_tombstone&& rt) {
    position_in_partition::less_compare less(*_schema);
    if (!less(_lower_bound, rt.position())) {
        rt.set_start(_lower_bound);
    } else {
        _lower_bound = position_in_partition(rt.position());
        _lower_bound_changed = true;
    }
    push_mutation_fragment(*_schema, _permit, std::move(rt));
}

inline
void cache_flat_mutation_reader::maybe_add_to_cache(const range_tombstone& rt) {
    if (can_populate()) {
        clogger.trace("csm {}: maybe_add_to_cache({})", fmt::ptr(this), rt);
        _lsa_manager.run_in_update_section_with_allocator([&] {
            _snp->version()->partition().row_tombstones().apply_monotonically(*_schema, rt);
        });
    } else {
        _read_context.cache().on_mispopulate();
    }
}

inline
void cache_flat_mutation_reader::maybe_add_to_cache(const static_row& sr) {
    if (can_populate()) {
        clogger.trace("csm {}: populate({})", fmt::ptr(this), static_row::printer(*_schema, sr));
        _read_context.cache().on_static_row_insert();
        _lsa_manager.run_in_update_section_with_allocator([&] {
            if (_read_context.digest_requested()) {
                sr.cells().prepare_hash(*_schema, column_kind::static_column);
            }
            _snp->version()->partition().static_row().apply(*_schema, column_kind::static_column, sr.cells());
        });
    } else {
        _read_context.cache().on_mispopulate();
    }
}

inline
void cache_flat_mutation_reader::maybe_set_static_row_continuous() {
    if (can_populate()) {
        clogger.trace("csm {}: set static row continuous", fmt::ptr(this));
        _snp->version()->partition().set_static_row_continuous(true);
    } else {
        _read_context.cache().on_mispopulate();
    }
}

inline
bool cache_flat_mutation_reader::can_populate() const {
    return _snp->at_latest_version() && _read_context.cache().phase_of(_read_context.key()) == _read_context.phase();
}

} // namespace cache

// pass a reference to ctx to cache_flat_mutation_reader
// keeping its ownership at caller's.
inline flat_mutation_reader make_cache_flat_mutation_reader(schema_ptr s,
                                                            dht::decorated_key dk,
                                                            query::clustering_key_filter_ranges crr,
                                                            row_cache& cache,
                                                            cache::read_context& ctx,
                                                            partition_snapshot_ptr snp)
{
    return make_flat_mutation_reader<cache::cache_flat_mutation_reader>(
        std::move(s), std::move(dk), std::move(crr), ctx, std::move(snp), cache);
}

// transfer ownership of ctx to cache_flat_mutation_reader
inline flat_mutation_reader make_cache_flat_mutation_reader(schema_ptr s,
                                                            dht::decorated_key dk,
                                                            query::clustering_key_filter_ranges crr,
                                                            row_cache& cache,
                                                            std::unique_ptr<cache::read_context> unique_ctx,
                                                            partition_snapshot_ptr snp)
{
    return make_flat_mutation_reader<cache::cache_flat_mutation_reader>(
        std::move(s), std::move(dk), std::move(crr), std::move(unique_ctx), std::move(snp), cache);
}
