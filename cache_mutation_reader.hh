/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <vector>
#include "row_cache.hh"
#include "mutation/mutation_fragment.hh"
#include "query-request.hh"
#include "partition_snapshot_row_cursor.hh"
#include "read_context.hh"
#include "readers/delegating_v2.hh"
#include "clustering_key_filter.hh"

namespace cache {

extern logging::logger clogger;

class cache_mutation_reader final : public mutation_reader::impl {
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
        // - Upper bound of the read is *_underlying_upper_bound
        // - _next_row_in_range = _next.position() < _upper_bound
        // - _last_row points at a direct predecessor of the next row which is going to be read.
        //   Used for populating continuity.
        // - _population_range_starts_before_all_rows is set accordingly
        // - _underlying is engaged and fast-forwarded
        reading_from_underlying,

        end_of_stream
    };
    partition_snapshot_ptr _snp;

    query::clustering_key_filter_ranges _ck_ranges; // Query schema domain, reversed reads use native order
    query::clustering_row_ranges::const_iterator _ck_ranges_curr; // Query schema domain
    query::clustering_row_ranges::const_iterator _ck_ranges_end; // Query schema domain

    lsa_manager _lsa_manager;

    partition_snapshot_row_weakref _last_row; // Table schema domain

    // Holds the lower bound of a position range which hasn't been processed yet.
    // Only rows with positions < _lower_bound have been emitted, and only
    // range_tombstone_changes with positions <= _lower_bound.
    //
    // Invariant: !_lower_bound.is_clustering_row()
    position_in_partition _lower_bound; // Query schema domain
    // Invariant: !_upper_bound.is_clustering_row()
    position_in_partition_view _upper_bound; // Query schema domain
    std::optional<position_in_partition> _underlying_upper_bound; // Query schema domain

    // cache_mutation_reader may be constructed either
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

    // Holds the currently active range tombstone of the output mutation fragment stream.
    // While producing the stream, at any given time, _current_tombstone applies to the
    // key range which extends at least to _lower_bound. When consuming subsequent interval,
    // which will advance _lower_bound further, be it from underlying or from cache,
    // a decision is made whether the range tombstone in the next interval is the same as
    // the current one or not. If it is different, then range_tombstone_change is emitted
    // with the old _lower_bound value (start of the next interval).
    tombstone _current_tombstone;

    state _state = state::before_static_row;

    bool _next_row_in_range = false;
    bool _has_rt = false;

    // True iff current population interval starts at before_all_clustered_rows
    // and _last_row is unset. (And the read isn't reverse).
    //
    // Rationale: in the "most general" step of cache population,
    // we mark the `(_last_row, ...] `range as continuous, which can involve doing something to `_last_row`.
    // But when populating the range `(before_all_clustered_rows, ...)`,
    // a rows_entry at `before_all_clustered_rows` needn't exist.
    // Thus this case needs a special treatment which doesn't involve `_last_row`.
    // And for that, this case it has to be recognized (via this flag).
    //
    // We cannot just look at _lower_bound, because emission of range tombstones changes _lower_bound and
    // because we mark clustering intervals as continuous when consuming a clustering_row, it would prevent
    // us from marking the interval as continuous.
    // Valid when _state == reading_from_underlying.
    bool _population_range_starts_before_all_rows;

    // Points to the underlying reader conforming to _schema,
    // either to *_underlying_holder or _read_context.underlying().underlying().
    mutation_reader* _underlying = nullptr;
    mutation_reader_opt _underlying_holder;

    gc_clock::time_point _read_time;
    gc_clock::time_point _gc_before;

    future<> do_fill_buffer();
    future<> ensure_underlying();
    void copy_from_cache_to_buffer();
    future<> process_static_row();
    void move_to_end();
    void move_to_next_range();
    void move_to_range(query::clustering_row_ranges::const_iterator);
    void move_to_next_entry();
    void maybe_drop_last_entry(tombstone) noexcept;
    void add_to_buffer(const partition_snapshot_row_cursor&);
    void add_clustering_row_to_buffer(mutation_fragment_v2&&);
    void add_to_buffer(range_tombstone_change&&);
    void offer_from_underlying(mutation_fragment_v2&&);
    future<> read_from_underlying();
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
    // If returns true then _last_row is refreshed and points to the population lower bound.
    // if _read_context.is_reversed() then _last_row is always valid after this.
    // if !_read_context.is_reversed() then _last_row is valid after this or the population lower bound
    // is before all rows (so _last_row doesn't point at any entry).
    bool ensure_population_lower_bound();
    void maybe_add_to_cache(const mutation_fragment_v2& mf);
    void maybe_add_to_cache(const clustering_row& cr);
    bool maybe_add_to_cache(const range_tombstone_change& rtc);
    void maybe_add_to_cache(const static_row& sr);
    void maybe_set_static_row_continuous();
    void set_rows_entry_continuous(rows_entry& e);
    void restore_continuity_after_insertion(const mutation_partition::rows_type::iterator&);
    void finish_reader() {
        push_mutation_fragment(*_schema, _permit, partition_end());
        _end_of_stream = true;
        _state = state::end_of_stream;
    }
    const schema_ptr& snp_schema() const {
        return _snp->schema();
    }
    void touch_partition();

    position_in_partition_view to_table_domain(position_in_partition_view query_domain_pos) {
        if (!_read_context.is_reversed()) [[likely]] {
            return query_domain_pos;
        }
        return query_domain_pos.reversed();
    }

    range_tombstone to_table_domain(range_tombstone query_domain_rt) {
        if (_read_context.is_reversed()) [[unlikely]] {
            query_domain_rt.reverse();
        }
        return query_domain_rt;
    }

    position_in_partition_view to_query_domain(position_in_partition_view table_domain_pos) {
        if (!_read_context.is_reversed()) [[likely]] {
            return table_domain_pos;
        }
        return table_domain_pos.reversed();
    }

    const schema& table_schema() {
        return *_snp->schema();
    }

    gc_clock::time_point get_read_time() {
        return _read_context.tombstone_gc_state() ? gc_clock::now() : gc_clock::time_point::min();
    }

    gc_clock::time_point get_gc_before(const schema& schema, dht::decorated_key dk, const gc_clock::time_point query_time) {
        auto gc_state = _read_context.tombstone_gc_state();
        if (gc_state) {
            return gc_state->get_gc_before_for_key(schema.shared_from_this(), dk, query_time);
        }

        return gc_clock::time_point::min();
    }

public:
    cache_mutation_reader(schema_ptr s,
                               dht::decorated_key dk,
                               query::clustering_key_filter_ranges&& crr,
                               read_context& ctx,
                               partition_snapshot_ptr snp,
                               row_cache& cache)
        : mutation_reader::impl(std::move(s), ctx.permit())
        , _snp(std::move(snp))
        , _ck_ranges(std::move(crr))
        , _ck_ranges_curr(_ck_ranges.begin())
        , _ck_ranges_end(_ck_ranges.end())
        , _lsa_manager(cache)
        , _lower_bound(position_in_partition::before_all_clustered_rows())
        , _upper_bound(position_in_partition_view::before_all_clustered_rows())
        , _read_context_holder()
        , _read_context(ctx)    // ctx is owned by the caller, who's responsible for closing it.
        , _next_row(*_schema, *_snp, false, _read_context.is_reversed())
        , _read_time(get_read_time())
        , _gc_before(get_gc_before(*_schema, dk, _read_time))
    {
        clogger.trace("csm {}: table={}.{}, reversed={}, snap={}", fmt::ptr(this), _schema->ks_name(), _schema->cf_name(), _read_context.is_reversed(),
                      fmt::ptr(&*_snp));
        push_mutation_fragment(*_schema, _permit, partition_start(std::move(dk), _snp->partition_tombstone()));
    }
    cache_mutation_reader(schema_ptr s,
                               dht::decorated_key dk,
                               query::clustering_key_filter_ranges&& crr,
                               std::unique_ptr<read_context> unique_ctx,
                               partition_snapshot_ptr snp,
                               row_cache& cache)
        : cache_mutation_reader(s, std::move(dk), std::move(crr), *unique_ctx, std::move(snp), cache)
    {
        // Assume ownership of the read_context.
        // It is our responsibility to close it now.
        _read_context_holder = std::move(unique_ctx);
    }
    cache_mutation_reader(const cache_mutation_reader&) = delete;
    cache_mutation_reader(cache_mutation_reader&&) = delete;
    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = true;
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        clear_buffer();
        _end_of_stream = true;
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(position_range pr) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        auto close_read_context = _read_context_holder ?  _read_context_holder->close() : make_ready_future<>();
        auto close_underlying = _underlying_holder ? _underlying_holder->close() : make_ready_future<>();
        return when_all_succeed(std::move(close_read_context), std::move(close_underlying)).discard_result();
    }
};

inline
future<> cache_mutation_reader::process_static_row() {
    if (_snp->static_row_continuous()) {
        _read_context.cache().on_row_hit();
        static_row sr = _lsa_manager.run_in_read_section([this] {
            return _snp->static_row(_read_context.digest_requested());
        });
        if (!sr.empty()) {
            push_mutation_fragment(*_schema, _permit, std::move(sr));
        }
        return make_ready_future<>();
    } else {
        _read_context.cache().on_row_miss();
        return ensure_underlying().then([this] {
            return (*_underlying)().then([this] (mutation_fragment_v2_opt&& sr) {
                if (sr) {
                    SCYLLA_ASSERT(sr->is_static_row());
                    maybe_add_to_cache(sr->as_static_row());
                    push_mutation_fragment(std::move(*sr));
                }
                maybe_set_static_row_continuous();
            });
        });
    }
}

inline
void cache_mutation_reader::touch_partition() {
    _snp->touch();
}

inline
future<> cache_mutation_reader::fill_buffer() {
    if (_state == state::before_static_row) {
        touch_partition();
        auto after_static_row = [this] {
            if (_ck_ranges_curr == _ck_ranges_end) {
                finish_reader();
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
    clogger.trace("csm {}: fill_buffer(), range={}, lb={}", fmt::ptr(this), *_ck_ranges_curr, _lower_bound);
    return do_until([this] { return _end_of_stream || is_buffer_full(); }, [this] {
        return do_fill_buffer();
    });
}

inline
future<> cache_mutation_reader::ensure_underlying() {
    if (_underlying) {
        return make_ready_future<>();
    }
    return _read_context.ensure_underlying().then([this] {
        mutation_reader& ctx_underlying = _read_context.underlying().underlying();
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
future<> cache_mutation_reader::do_fill_buffer() {
    if (_state == state::move_to_underlying) {
        if (!_underlying) {
            return ensure_underlying().then([this] {
                return do_fill_buffer();
            });
        }
        _state = state::reading_from_underlying;
        _population_range_starts_before_all_rows = _lower_bound.is_before_all_clustered_rows(*_schema) && !_read_context.is_reversed() && !_last_row;
        _underlying_upper_bound = _next_row_in_range ? position_in_partition::before_key(_next_row.position())
                                                     : position_in_partition(_upper_bound);
        if (!_read_context.partition_exists()) {
            clogger.trace("csm {}: partition does not exist", fmt::ptr(this));
            if (_current_tombstone) {
                clogger.trace("csm {}: move_to_underlying: emit rtc({}, null)", fmt::ptr(this), _lower_bound);
                push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, range_tombstone_change(_lower_bound, {})));
                _current_tombstone = {};
            }
            return read_from_underlying();
        }
        return _underlying->fast_forward_to(position_range{_lower_bound, *_underlying_upper_bound}).then([this] {
            if (!_current_tombstone) {
                return read_from_underlying();
            }
            return _underlying->peek().then([this] (mutation_fragment_v2* mf) {
                position_in_partition::equal_compare eq(*_schema);
                if (!mf || !mf->is_range_tombstone_change()
                        || !eq(mf->as_range_tombstone_change().position(), _lower_bound)) {
                    clogger.trace("csm {}: move_to_underlying: emit rtc({}, null)", fmt::ptr(this), _lower_bound);
                    push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, range_tombstone_change(_lower_bound, {})));
                    _current_tombstone = {};
                }
                return read_from_underlying();
            });
        });
    }
    if (_state == state::reading_from_underlying) {
        return read_from_underlying();
    }
    // SCYLLA_ASSERT(_state == state::reading_from_cache)
    return _lsa_manager.run_in_read_section([this] {
        auto next_valid = _next_row.iterators_valid();
        clogger.trace("csm {}: reading_from_cache, range=[{}, {}), next={}, valid={}, rt={}", fmt::ptr(this), _lower_bound,
            _upper_bound, _next_row.position(), next_valid, _current_tombstone);
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
        clogger.trace("csm {}: next={}", fmt::ptr(this), _next_row);
        while (_state == state::reading_from_cache) {
            copy_from_cache_to_buffer();
            if (need_preempt() || is_buffer_full()) {
                break;
            }
        }
        return make_ready_future<>();
    });
}

inline
future<> cache_mutation_reader::read_from_underlying() {
    return consume_mutation_fragments_until(*_underlying,
        [this] { return _state != state::reading_from_underlying || is_buffer_full(); },
        [this] (mutation_fragment_v2 mf) {
            _read_context.cache().on_row_miss();
            offer_from_underlying(std::move(mf));
        },
        [this] {
            _lower_bound = std::move(*_underlying_upper_bound);
            _underlying_upper_bound.reset();
            _state = state::reading_from_cache;
            _lsa_manager.run_in_update_section([this] {
                auto same_pos = _next_row.maybe_refresh();
                clogger.trace("csm {}: underlying done, in_range={}, same={}, next={}", fmt::ptr(this), _next_row_in_range, same_pos, _next_row);
                if (!same_pos) {
                    _read_context.cache().on_mispopulate(); // FIXME: Insert dummy entry at _lower_bound.
                    _next_row_in_range = !after_current_range(_next_row.position());
                    if (!_next_row.continuous()) {
                        _last_row = nullptr; // We did not populate the full range up to _lower_bound, break continuity
                        start_reading_from_underlying();
                    }
                    return;
                }
                if (_next_row_in_range) {
                    maybe_update_continuity();
                } else {
                    if (can_populate()) {
                        const schema& table_s = table_schema();
                        rows_entry::tri_compare cmp(table_s);
                        auto& rows = _snp->version()->partition().mutable_clustered_rows();
                        if (query::is_single_row(*_schema, *_ck_ranges_curr)) {
                            // If there are range tombstones which apply to the row then
                            // we cannot insert an empty entry here because if those range
                            // tombstones got evicted by now, we will insert an entry
                            // with missing range tombstone information.
                            // FIXME: try to set the range tombstone when possible.
                            if (!_has_rt) {
                            with_allocator(_snp->region().allocator(), [&] {
                                auto e = alloc_strategy_unique_ptr<rows_entry>(
                                    current_allocator().construct<rows_entry>(_ck_ranges_curr->start()->value()));
                                // Use _next_row iterator only as a hint, because there could be insertions after _upper_bound.
                                auto insert_result = rows.insert_before_hint(
                                        _next_row.at_a_row() ? _next_row.get_iterator_in_latest_version() : rows.begin(),
                                        std::move(e),
                                        cmp);
                                if (insert_result.second) {
                                    auto it = insert_result.first;
                                    _snp->tracker()->insert(*it);
                                    auto next = std::next(it);
                                    // Also works in reverse read mode.
                                    // It preserves the continuity of the range the entry falls into.
                                    it->set_continuous(next->continuous());
                                    clogger.trace("csm {}: inserted empty row at {}, cont={}, rt={}", fmt::ptr(this), it->position(), it->continuous(), it->range_tombstone());
                                }
                            });
                            }
                        } else if (ensure_population_lower_bound()) {
                            with_allocator(_snp->region().allocator(), [&] {
                                auto e = alloc_strategy_unique_ptr<rows_entry>(
                                    current_allocator().construct<rows_entry>(table_s, to_table_domain(_upper_bound), is_dummy::yes, is_continuous::no));
                                // Use _next_row iterator only as a hint, because there could be insertions after _upper_bound.
                                auto insert_result = rows.insert_before_hint(
                                        _next_row.at_a_row() ? _next_row.get_iterator_in_latest_version() : rows.begin(),
                                        std::move(e),
                                        cmp);
                                if (insert_result.second) {
                                    clogger.trace("csm {}: L{}: inserted dummy at {}", fmt::ptr(this), __LINE__, _upper_bound);
                                    _snp->tracker()->insert(*insert_result.first);
                                    restore_continuity_after_insertion(insert_result.first);
                                }
                                if (_read_context.is_reversed()) [[unlikely]] {
                                    clogger.trace("csm {}: set_continuous({}), prev={}, rt={}", fmt::ptr(this), _last_row.position(), insert_result.first->position(), _current_tombstone);
                                    set_rows_entry_continuous(*_last_row);
                                    _last_row->set_range_tombstone(_current_tombstone);
                                } else {
                                    clogger.trace("csm {}: set_continuous({}), prev={}, rt={}", fmt::ptr(this), insert_result.first->position(), _last_row.position(), _current_tombstone);
                                    set_rows_entry_continuous(*insert_result.first);
                                    insert_result.first->set_range_tombstone(_current_tombstone);
                                }
                                maybe_drop_last_entry(_current_tombstone);
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
        });
}

inline
bool cache_mutation_reader::ensure_population_lower_bound() {
    if (_population_range_starts_before_all_rows) {
        return true;
    }
    if (!_last_row.refresh(*_snp)) {
        return false;
    }
    // Continuity flag we will later set for the upper bound extends to the previous row in the same version,
    // so we need to ensure we have an entry in the latest version.
    if (!_last_row.is_in_latest_version()) {
        rows_entry::tri_compare cmp(*_schema);
        partition_snapshot_row_cursor cur(*_schema, *_snp, false, _read_context.is_reversed());

        if (!cur.advance_to(to_query_domain(_last_row.position()))) {
            return false;
        }

        if (cmp(cur.table_position(), _last_row.position()) != 0) {
            return false;
        }

        auto res = with_allocator(_snp->region().allocator(), [&] {
            return cur.ensure_entry_in_latest();
        });

        _last_row.set_latest(res.it);
        if (res.inserted) {
            clogger.trace("csm {}: inserted lower bound dummy at {}", fmt::ptr(this), _last_row.position());
        }
    }

    return true;
}

inline
void cache_mutation_reader::maybe_update_continuity() {
    position_in_partition::equal_compare eq(*_schema);
    if (can_populate()
            && ensure_population_lower_bound()
            && !eq(_last_row.position(), _next_row.table_position())) {
        with_allocator(_snp->region().allocator(), [&] {
            rows_entry& e = _next_row.ensure_entry_in_latest().row;
            auto& rows = _snp->version()->partition().mutable_clustered_rows();
            const schema& table_s = table_schema();
            rows_entry::tri_compare table_cmp(table_s);

            if (_read_context.is_reversed()) [[unlikely]] {
                if (_current_tombstone != _last_row->range_tombstone() && !_last_row->dummy()) {
                    with_allocator(_snp->region().allocator(), [&] {
                        auto e2 = alloc_strategy_unique_ptr<rows_entry>(
                                current_allocator().construct<rows_entry>(table_s,
                                                                          position_in_partition_view::before_key(_last_row->position()),
                                                                          is_dummy::yes,
                                                                          is_continuous::yes));
                        auto insert_result = rows.insert(std::move(e2), table_cmp);
                        if (insert_result.second) {
                            clogger.trace("csm {}: L{}: inserted dummy at {}", fmt::ptr(this), __LINE__, insert_result.first->position());
                            _snp->tracker()->insert(*insert_result.first);
                        }
                        clogger.trace("csm {}: set_continuous({}), prev={}, rt={}", fmt::ptr(this), insert_result.first->position(),
                                      _last_row.position(), _current_tombstone);
                        set_rows_entry_continuous(*insert_result.first);
                        insert_result.first->set_range_tombstone(_current_tombstone);
                        clogger.trace("csm {}: set_continuous({})", fmt::ptr(this), _last_row.position());
                        set_rows_entry_continuous(*_last_row);
                    });
                } else {
                    clogger.trace("csm {}: set_continuous({}), rt={}", fmt::ptr(this), _last_row.position(), _current_tombstone);
                    set_rows_entry_continuous(*_last_row);
                    _last_row->set_range_tombstone(_current_tombstone);
                }
            } else {
                if (_current_tombstone != e.range_tombstone() && !e.dummy()) {
                    with_allocator(_snp->region().allocator(), [&] {
                        auto e2 = alloc_strategy_unique_ptr<rows_entry>(
                                current_allocator().construct<rows_entry>(table_s,
                                                                          position_in_partition_view::before_key(e.position()),
                                                                          is_dummy::yes,
                                                                          is_continuous::yes));
                        // Use _next_row iterator only as a hint because there could be insertions before
                        // _next_row.get_iterator_in_latest_version(), either from concurrent reads,
                        // from _next_row.ensure_entry_in_latest().
                        auto insert_result = rows.insert_before_hint(_next_row.get_iterator_in_latest_version(), std::move(e2), table_cmp);
                        if (insert_result.second) {
                            clogger.trace("csm {}: L{}: inserted dummy at {}", fmt::ptr(this), __LINE__, insert_result.first->position());
                            _snp->tracker()->insert(*insert_result.first);
                            clogger.trace("csm {}: set_continuous({}), prev={}, rt={}", fmt::ptr(this), insert_result.first->position(),
                                          _last_row.position(), _current_tombstone);
                            set_rows_entry_continuous(*insert_result.first);
                            insert_result.first->set_range_tombstone(_current_tombstone);
                        }
                        clogger.trace("csm {}: set_continuous({})", fmt::ptr(this), e.position());
                        set_rows_entry_continuous(e);
                    });
                } else {
                    clogger.trace("csm {}: set_continuous({}), rt={}", fmt::ptr(this), e.position(), _current_tombstone);
                    e.set_range_tombstone(_current_tombstone);
                    set_rows_entry_continuous(e);
                }
            }
            maybe_drop_last_entry(_current_tombstone);
        });
    } else {
        _read_context.cache().on_mispopulate();
    }
}

inline
void cache_mutation_reader::maybe_add_to_cache(const clustering_row& cr) {
    if (!can_populate()) {
        _last_row = nullptr;
        _population_range_starts_before_all_rows = false;
        _read_context.cache().on_mispopulate();
        return;
    }
    clogger.trace("csm {}: populate({}), rt={}", fmt::ptr(this), clustering_row::printer(*_schema, cr), _current_tombstone);
    _lsa_manager.run_in_update_section_with_allocator([this, &cr] {
        mutation_partition_v2& mp = _snp->version()->partition();
        rows_entry::tri_compare cmp(table_schema());

        if (_read_context.digest_requested()) {
            cr.cells().prepare_hash(*_schema, column_kind::regular_column);
        }
        auto new_entry = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(table_schema(), cr.key(), cr.as_deletable_row()));
        new_entry->set_continuous(false);
        new_entry->set_range_tombstone(_current_tombstone);
        auto it = _next_row.iterators_valid() && _next_row.at_a_row() ? _next_row.get_iterator_in_latest_version()
                                              : mp.clustered_rows().lower_bound(cr.key(), cmp);
        auto insert_result = mp.mutable_clustered_rows().insert_before_hint(it, std::move(new_entry), cmp);
        it = insert_result.first;
        if (insert_result.second) {
            _snp->tracker()->insert(*it);
            restore_continuity_after_insertion(it);
        }

        rows_entry& e = *it;
        if (ensure_population_lower_bound()) {
            if (_read_context.is_reversed()) [[unlikely]] {
                clogger.trace("csm {}: set_continuous({})", fmt::ptr(this), _last_row.position());
                set_rows_entry_continuous(*_last_row);
                // _current_tombstone must also apply to _last_row itself (if it's non-dummy)
                // because otherwise there would be a rtc after it, either creating a different entry,
                // or clearing _last_row if population did not happen.
                _last_row->set_range_tombstone(_current_tombstone);
            } else {
                clogger.trace("csm {}: set_continuous({})", fmt::ptr(this), e.position());
                set_rows_entry_continuous(e);
                e.set_range_tombstone(_current_tombstone);
            }
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
bool cache_mutation_reader::maybe_add_to_cache(const range_tombstone_change& rtc) {
    rows_entry::tri_compare q_cmp(*_schema);

    clogger.trace("csm {}: maybe_add_to_cache({})", fmt::ptr(this), rtc);

    // Don't emit the closing range tombstone change, we may continue from cache with the same tombstone.
    // The following relies on !_underlying_upper_bound->is_clustering_row()
    if (q_cmp(rtc.position(), *_underlying_upper_bound) == 0) {
        _lower_bound = rtc.position();
        return false;
    }

    auto prev = std::exchange(_current_tombstone, rtc.tombstone());
    if (_current_tombstone == prev) {
        return false;
    }

    if (!can_populate()) {
        // _current_tombstone is now invalid and remains so for this reader. No need to change it.
        _last_row = nullptr;
        _population_range_starts_before_all_rows = false;
        _read_context.cache().on_mispopulate();
        return true;
    }

    _lsa_manager.run_in_update_section_with_allocator([&] {
        mutation_partition_v2& mp = _snp->version()->partition();
        rows_entry::tri_compare cmp(table_schema());

        auto new_entry = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(table_schema(), to_table_domain(rtc.position()), is_dummy::yes, is_continuous::no));
        auto it = _next_row.iterators_valid() && _next_row.at_a_row() ? _next_row.get_iterator_in_latest_version()
                                              : mp.clustered_rows().lower_bound(to_table_domain(rtc.position()), cmp);
        auto insert_result = mp.mutable_clustered_rows().insert_before_hint(it, std::move(new_entry), cmp);
        it = insert_result.first;
        if (insert_result.second) {
            _snp->tracker()->insert(*it);
            restore_continuity_after_insertion(it);
        }

        rows_entry& e = *it;
        if (ensure_population_lower_bound()) {
            // underlying may emit range_tombstone_change fragments with the same position.
            // In such case, the range to which the tombstone from the first fragment applies is empty and should be ignored.
            //
            // Note: we are using a query schema comparator to compare table schema positions here,
            // but this is okay because we are only checking for equality,
            // which is preserved by schema reversals.
            if (q_cmp(_last_row.position(), it->position()) != 0) {
                if (_read_context.is_reversed()) [[unlikely]] {
                    clogger.trace("csm {}: set_continuous({}), rt={}", fmt::ptr(this), _last_row.position(), prev);
                    set_rows_entry_continuous(*_last_row);
                    _last_row->set_range_tombstone(prev);
                } else {
                    clogger.trace("csm {}: set_continuous({}), rt={}", fmt::ptr(this), e.position(), prev);
                    set_rows_entry_continuous(e);
                    e.set_range_tombstone(prev);
                }
            }
        } else {
            _read_context.cache().on_mispopulate();
        }
        with_allocator(standard_allocator(), [&] {
            _last_row = partition_snapshot_row_weakref(*_snp, it, true);
        });
        _population_range_starts_before_all_rows = false;
    });
    return true;
}

inline
bool cache_mutation_reader::after_current_range(position_in_partition_view p) {
    position_in_partition::tri_compare cmp(*_schema);
    return cmp(p, _upper_bound) >= 0;
}

inline
void cache_mutation_reader::start_reading_from_underlying() {
    clogger.trace("csm {}: start_reading_from_underlying(), range=[{}, {})", fmt::ptr(this), _lower_bound, _next_row_in_range ? _next_row.position() : _upper_bound);
    _state = state::move_to_underlying;
    _next_row.touch();
}

inline
void cache_mutation_reader::copy_from_cache_to_buffer() {
    clogger.trace("csm {}: copy_from_cache, next_row_in_range={}, next={}", fmt::ptr(this), _next_row_in_range, _next_row);
    _next_row.touch();

    if (_next_row.range_tombstone() != _current_tombstone) {
        position_in_partition::equal_compare eq(*_schema);
        auto upper_bound = _next_row_in_range ? position_in_partition_view::before_key(_next_row.position()) : _upper_bound;
        if (!eq(_lower_bound, upper_bound)) {
            position_in_partition new_lower_bound(upper_bound);
            auto tomb = _next_row.range_tombstone();
            clogger.trace("csm {}: rtc({}, {}) ...{}", fmt::ptr(this), _lower_bound, tomb, new_lower_bound);
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, range_tombstone_change(_lower_bound, tomb)));
            _current_tombstone = tomb;
            _lower_bound = std::move(new_lower_bound);
            _read_context.cache()._tracker.on_range_tombstone_read();
        }
    }

    if (_next_row_in_range) {
        bool remove_row = false;

        if (_read_context.tombstone_gc_state() // do not compact rows when tombstone_gc_state is not set (used in some unit tests)
            && !_next_row.dummy()
            && _snp->at_latest_version()
            && _snp->at_oldest_version()) {
            deletable_row& row = _next_row.latest_row();
            tombstone range_tomb = _next_row.range_tombstone_for_row();
            auto t = row.deleted_at();
            t.apply(range_tomb);

            auto row_tomb_expired = [&](row_tombstone tomb) {
                return (tomb && tomb.max_deletion_time() < _gc_before);
            };

            auto is_row_dead = [&](const deletable_row& row) {
                auto& m = row.marker();
                return (!m.is_missing() && m.is_dead(_read_time) && m.deletion_time() < _gc_before);
            };

            if (row_tomb_expired(t) || is_row_dead(row)) {
                can_gc_fn always_gc = [&](tombstone) { return true; };
                const schema& row_schema = _next_row.latest_row_schema();

                _read_context.cache()._tracker.on_row_compacted();

                with_allocator(_snp->region().allocator(), [&] {
                    deletable_row row_copy(row_schema, row);
                    row_copy.compact_and_expire(row_schema, t.tomb(), _read_time, always_gc, _gc_before, nullptr);
                    std::swap(row, row_copy);
                });
                remove_row = row.empty();

                auto tomb_expired = [&](tombstone tomb) {
                    return (tomb && tomb.deletion_time < _gc_before);
                };

                auto latests_range_tomb = _next_row.get_iterator_in_latest_version()->range_tombstone();
                if (tomb_expired(latests_range_tomb)) {
                    _next_row.get_iterator_in_latest_version()->set_range_tombstone({});
                }
            }
        }

        if (_next_row.range_tombstone_for_row() != _current_tombstone) [[unlikely]] {
            auto tomb = _next_row.range_tombstone_for_row();
            auto new_lower_bound = position_in_partition::before_key(_next_row.position());
            clogger.trace("csm {}: rtc({}, {})", fmt::ptr(this), new_lower_bound, tomb);
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, range_tombstone_change(new_lower_bound, tomb)));
            _lower_bound = std::move(new_lower_bound);
            _current_tombstone = tomb;
            _read_context.cache()._tracker.on_range_tombstone_read();
        }

        if (remove_row) {
            _read_context.cache()._tracker.on_row_compacted_away();

            _lower_bound = position_in_partition::after_key(*_schema, _next_row.position());

            partition_snapshot_row_weakref row_ref(_next_row);
            move_to_next_entry();

            with_allocator(_snp->region().allocator(), [&] {
                cache_tracker& tracker = _read_context.cache()._tracker;
                if (row_ref->is_linked()) {
                    tracker.get_lru().remove(*row_ref);
                }
                row_ref->on_evicted(tracker);
            });

            _snp->region().allocator().invalidate_references();
            _next_row.force_valid();
        } else {
            // We add the row to the buffer even when it's full.
            // This simplifies the code. For more info see #3139.
            add_to_buffer(_next_row);
            move_to_next_entry();
        }
    } else {
        move_to_next_range();
    }
}

inline
void cache_mutation_reader::move_to_end() {
    finish_reader();
    clogger.trace("csm {}: eos", fmt::ptr(this));
}

inline
void cache_mutation_reader::move_to_next_range() {
    if (_current_tombstone) {
        clogger.trace("csm {}: move_to_next_range: emit rtc({}, null)", fmt::ptr(this), _upper_bound);
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, range_tombstone_change(_upper_bound, {})));
        _current_tombstone = {};
    }
    auto next_it = std::next(_ck_ranges_curr);
    if (next_it == _ck_ranges_end) {
        move_to_end();
        _ck_ranges_curr = next_it;
    } else {
        move_to_range(next_it);
    }
}

inline
void cache_mutation_reader::move_to_range(query::clustering_row_ranges::const_iterator next_it) {
    auto lb = position_in_partition::for_range_start(*next_it);
    auto ub = position_in_partition_view::for_range_end(*next_it);
    _last_row = nullptr;
    _lower_bound = std::move(lb);
    _upper_bound = std::move(ub);
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
                auto insert_result = with_allocator(_lsa_manager.region().allocator(), [&] {
                    rows_entry::tri_compare cmp(table_schema());
                    auto& rows = _snp->version()->partition().mutable_clustered_rows();
                    auto new_entry = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(table_schema(),
                            to_table_domain(_lower_bound), is_dummy::yes, is_continuous::no));
                    return rows.insert_before_hint(
                            _next_row.at_a_row() ? _next_row.get_iterator_in_latest_version() : rows.begin(),
                            std::move(new_entry),
                            cmp);
                });
                auto it = insert_result.first;
                if (insert_result.second) {
                    _snp->tracker()->insert(*it);
                }
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
void cache_mutation_reader::maybe_drop_last_entry(tombstone rt) noexcept {
    // Drop dummy entry if it falls inside a continuous range.
    // This prevents unnecessary dummy entries from accumulating in cache and slowing down scans.
    //
    // Eviction can happen only from oldest versions to preserve the continuity non-overlapping rule
    // (See docs/dev/row_cache.md)
    //
    if (_last_row
            && !_read_context.is_reversed() // FIXME
            && _last_row->dummy()
            && _last_row->continuous()
            && _last_row->range_tombstone() == rt
            && _snp->at_latest_version()
            && _snp->at_oldest_version()) {

        clogger.trace("csm {}: dropping unnecessary dummy at {}", fmt::ptr(this), _last_row->position());

        with_allocator(_snp->region().allocator(), [&] {
            cache_tracker& tracker = _read_context.cache()._tracker;
            tracker.get_lru().remove(*_last_row);
            _last_row->on_evicted(tracker);
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
void cache_mutation_reader::move_to_next_entry() {
    clogger.trace("csm {}: move_to_next_entry(), curr={}", fmt::ptr(this), _next_row.position());
    if (no_clustering_row_between(*_schema, _next_row.position(), _upper_bound)) {
        move_to_next_range();
    } else {
        auto new_last_row = partition_snapshot_row_weakref(_next_row);
        // In reverse mode, the cursor may fall out of the entries because there is no dummy before all rows.
        // Hence !next() doesn't mean we can end the read. The cursor will be positioned before all rows and
        // not point at any row. continuous() is still correctly set.
        _next_row.next();
        _last_row = std::move(new_last_row);
        _next_row_in_range = !after_current_range(_next_row.position());
        clogger.trace("csm {}: next={}, cont={}, in_range={}", fmt::ptr(this), _next_row.position(), _next_row.continuous(), _next_row_in_range);
        if (!_next_row.continuous()) {
            start_reading_from_underlying();
        } else {
            maybe_drop_last_entry(_next_row.range_tombstone());
        }
    }
}

inline
void cache_mutation_reader::offer_from_underlying(mutation_fragment_v2&& mf) {
    clogger.trace("csm {}: offer_from_underlying({})", fmt::ptr(this), mutation_fragment_v2::printer(*_schema, mf));
    if (mf.is_clustering_row()) {
        maybe_add_to_cache(mf.as_clustering_row());
        add_clustering_row_to_buffer(std::move(mf));
    } else {
        SCYLLA_ASSERT(mf.is_range_tombstone_change());
        auto& chg = mf.as_range_tombstone_change();
        if (maybe_add_to_cache(chg)) {
            add_to_buffer(std::move(mf).as_range_tombstone_change());
        }
    }
}

inline
void cache_mutation_reader::add_to_buffer(const partition_snapshot_row_cursor& row) {
    position_in_partition::less_compare less(*_schema);
    if (!row.dummy()) {
        _read_context.cache().on_row_hit();
        if (_read_context.digest_requested()) {
            row.latest_row_prepare_hash();
        }
        add_clustering_row_to_buffer(mutation_fragment_v2(*_schema, _permit, row.row()));
    } else {
        if (less(_lower_bound, row.position())) {
            _lower_bound = row.position();
        }
        _read_context.cache()._tracker.on_dummy_row_hit();
    }
}

// Maintains the following invariants, also in case of exception:
//   (1) no fragment with position >= _lower_bound was pushed yet
//   (2) If _lower_bound > mf.position(), mf was emitted
inline
void cache_mutation_reader::add_clustering_row_to_buffer(mutation_fragment_v2&& mf) {
    clogger.trace("csm {}: add_clustering_row_to_buffer({})", fmt::ptr(this), mutation_fragment_v2::printer(*_schema, mf));
    auto& row = mf.as_clustering_row();
    auto new_lower_bound = position_in_partition::after_key(*_schema, row.key());
    push_mutation_fragment(std::move(mf));
    _lower_bound = std::move(new_lower_bound);
    if (row.tomb()) {
        _read_context.cache()._tracker.on_row_tombstone_read();
    }
}

inline
void cache_mutation_reader::add_to_buffer(range_tombstone_change&& rtc) {
    clogger.trace("csm {}: add_to_buffer({})", fmt::ptr(this), rtc);
    _has_rt = true;
    position_in_partition::less_compare less(*_schema);
    _lower_bound = position_in_partition(rtc.position());
    push_mutation_fragment(*_schema, _permit, std::move(rtc));
    _read_context.cache()._tracker.on_range_tombstone_read();
}

inline
void cache_mutation_reader::maybe_add_to_cache(const static_row& sr) {
    if (can_populate()) {
        clogger.trace("csm {}: populate({})", fmt::ptr(this), static_row::printer(*_schema, sr));
        _read_context.cache().on_static_row_insert();
        _lsa_manager.run_in_update_section_with_allocator([&] {
            if (_read_context.digest_requested()) {
                sr.cells().prepare_hash(*_schema, column_kind::static_column);
            }
            // Static row is the same under table and query schema
            _snp->version()->partition().static_row().apply(table_schema(), column_kind::static_column, sr.cells());
        });
    } else {
        _read_context.cache().on_mispopulate();
    }
}

inline
void cache_mutation_reader::maybe_set_static_row_continuous() {
    if (can_populate()) {
        clogger.trace("csm {}: set static row continuous", fmt::ptr(this));
        _snp->version()->partition().set_static_row_continuous(true);
    } else {
        _read_context.cache().on_mispopulate();
    }
}

// Last dummies can exist in a quasi-evicted state, where they are unlinked from LRU,
// but still alive.
// But while in this state, they mustn't carry any information (i.e. continuity),
// due to the "older versions are evicted first" rule of MVCC.
// Thus, when we make an entry continuous, we must ensure that it isn't an
// unlinked last dummy.
inline
void cache_mutation_reader::set_rows_entry_continuous(rows_entry& e) {
    e.set_continuous(true);
    if (!e.is_linked()) [[unlikely]] {
        _snp->tracker()->touch(e);
    }
}

inline
void cache_mutation_reader::restore_continuity_after_insertion(const mutation_partition::rows_type::iterator& it) {
    if (auto x = std::next(it); x->continuous()) {
        it->set_continuous(true);
        it->set_range_tombstone(x->range_tombstone());
    }
}

inline
bool cache_mutation_reader::can_populate() const {
    return _snp->at_latest_version() && _read_context.cache().phase_of(_read_context.key()) == _read_context.phase();
}

} // namespace cache

// pass a reference to ctx to cache_mutation_reader
// keeping its ownership at caller's.
inline mutation_reader make_cache_mutation_reader(schema_ptr s,
                                                            dht::decorated_key dk,
                                                            query::clustering_key_filter_ranges crr,
                                                            row_cache& cache,
                                                            cache::read_context& ctx,
                                                            partition_snapshot_ptr snp)
{
    return make_mutation_reader<cache::cache_mutation_reader>(
        std::move(s), std::move(dk), std::move(crr), ctx, std::move(snp), cache);
}

// transfer ownership of ctx to cache_mutation_reader
inline mutation_reader make_cache_mutation_reader(schema_ptr s,
                                                            dht::decorated_key dk,
                                                            query::clustering_key_filter_ranges crr,
                                                            row_cache& cache,
                                                            std::unique_ptr<cache::read_context> unique_ctx,
                                                            partition_snapshot_ptr snp)
{
    return make_mutation_reader<cache::cache_mutation_reader>(
        std::move(s), std::move(dk), std::move(crr), std::move(unique_ctx), std::move(snp), cache);
}
