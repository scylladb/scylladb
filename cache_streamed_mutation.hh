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
    lw_shared_ptr<partition_snapshot> _snp;
    position_in_partition::tri_compare _position_cmp;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _ck_ranges_curr;
    query::clustering_row_ranges::const_iterator _ck_ranges_end;

    lsa_manager _lsa_manager;

    stdx::optional<clustering_key> _last_row_key;

    // We need to be prepared that we may get overlapping and out of order
    // range tombstones. We must emit fragments with strictly monotonic positions,
    // so we can't just trim such tombstones to the position of the last fragment.
    // To solve that, range tombstones are accumulated first in a range_tombstone_stream
    // and emitted once we have a fragment with a larger position.
    range_tombstone_stream _tombstones;

    // Holds the lower bound of a position range which hasn't been processed yet.
    // Only fragments with positions < _lower_bound have been emitted.
    position_in_partition _lower_bound;
    position_in_partition_view _upper_bound;

    bool _static_row_done = false;
    bool _reading_underlying = false;
    lw_shared_ptr<read_context> _read_context;
    partition_snapshot_row_cursor _next_row;
    bool _next_row_in_range = false;

    future<> do_fill_buffer();
    future<> copy_from_cache_to_buffer();
    future<> process_static_row();
    void move_to_end();
    future<> move_to_next_range();
    future<> move_to_current_range();
    future<> move_to_next_entry();
    // Emits all delayed range tombstones with positions smaller than upper_bound.
    void drain_tombstones(position_in_partition_view upper_bound);
    // Emits all delayed range tombstones.
    void drain_tombstones();
    void add_to_buffer(const partition_snapshot_row_cursor&);
    void add_to_buffer(clustering_row&&);
    void add_to_buffer(range_tombstone&&);
    void add_to_buffer(mutation_fragment&&);
    future<> read_from_underlying();
    future<> start_reading_from_underlying();
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
        : streamed_mutation::impl(std::move(s), dk, snp->partition_tombstone())
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
        , _next_row(*_schema, cache._tracker.region(), *_snp)
    { }
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
        row sr = _snp->static_row();
        if (!sr.empty()) {
            push_mutation_fragment(mutation_fragment(static_row(std::move(sr))));
        }
        return make_ready_future<>();
    } else {
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
    if (!_static_row_done) {
        _static_row_done = true;
        return process_static_row().then([this] {
            return _lsa_manager.run_in_read_section([this] {
                return move_to_current_range();
            }).then([this] {
                return fill_buffer();
            });
        });
    }
    return do_until([this] { return _end_of_stream || is_buffer_full(); }, [this] {
        return do_fill_buffer();
    });
}

inline
future<> cache_streamed_mutation::do_fill_buffer() {
    if (_reading_underlying) {
        return read_from_underlying();
    }
    return _lsa_manager.run_in_read_section([this] {
        auto same_pos = _next_row.maybe_refresh();
        // FIXME: If continuity changed anywhere between _lower_bound and _next_row.position()
        // we need to redo the lookup with _lower_bound. There is no eviction yet, so not yet a problem.
        assert(same_pos);
        while (!is_buffer_full() && !_end_of_stream && !_reading_underlying) {
            future<> f = copy_from_cache_to_buffer();
            if (!f.available() || need_preempt()) {
                return f;
            }
        }
        return make_ready_future<>();
    });
}

inline
future<> cache_streamed_mutation::read_from_underlying() {
    return do_until([this] { return !_reading_underlying || is_buffer_full(); }, [this] {
        return _read_context->get_next_fragment().then([this] (auto&& mfopt) {
            if (!mfopt) {
                _reading_underlying = false;
                return _lsa_manager.run_in_update_section([this] {
                    auto same_pos = _next_row.maybe_refresh();
                    assert(same_pos); // FIXME: handle eviction
                    if (_next_row_in_range) {
                        this->maybe_update_continuity();
                        this->add_to_buffer(_next_row);
                        return this->move_to_next_entry();
                    } else {
                        if (no_clustering_row_between(*_schema, _upper_bound, _next_row.position())) {
                            this->maybe_update_continuity();
                        } else {
                            // FIXME: Insert dummy entry at _upper_bound.
                        }
                        return this->move_to_next_range();
                    }
                });
            } else {
                this->maybe_add_to_cache(*mfopt);
                this->add_to_buffer(std::move(*mfopt));
                return make_ready_future<>();
            }
        });
    });
}

inline
void cache_streamed_mutation::maybe_update_continuity() {
    if (can_populate() && _next_row.is_in_latest_version()) {
        if (_last_row_key) {
            if (_next_row.previous_row_in_latest_version_has_key(*_last_row_key)) {
                _next_row.set_continuous(true);
            }
        } else if (!_ck_ranges_curr->start()) {
            _next_row.set_continuous(true);
        }
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
        return;
    }
    _lsa_manager.run_in_update_section_with_allocator([this, &cr] {
        mutation_partition& mp = _snp->version()->partition();
        rows_entry::compare less(*_schema);

        // FIXME: If _next_row is up to date, but latest version doesn't have iterator in
        // current row (could be far away, so we'd do this often), then this will do
        // the lookup in mp. This is not necessary, because _next_row has iterators for
        // next rows in each version, even if they're not part of the current row.
        // They're currently buried in the heap, but you could keep a vector of
        // iterators per each version in addition to the heap.
        auto new_entry = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(cr.key(), cr.tomb(), cr.marker(), cr.cells()));
        new_entry->set_continuous(false);
        auto it = _next_row.has_up_to_date_row_from_latest_version()
                  ? _next_row.get_iterator_in_latest_version() : mp.clustered_rows().lower_bound(cr.key(), less);
        auto insert_result = mp.clustered_rows().insert_check(it, *new_entry, less);
        if (insert_result.second) {
            new_entry.release();
        }
        it = insert_result.first;

        rows_entry& e = *it;
        if (_last_row_key) {
            if (it == mp.clustered_rows().begin()) {
                // FIXME: check whether entry for _last_row_key is in older versions and if so set
                // continuity to true.
            } else {
                auto prev_it = it;
                --prev_it;
                clustering_key_prefix::tri_compare tri_comp(*_schema);
                if (tri_comp(*_last_row_key, prev_it->key()) == 0) {
                    e.set_continuous(true);
                }
            }
        } else if (!_ck_ranges_curr->start()) {
            e.set_continuous(true);
        } else {
            // FIXME: Insert dummy entry at _ck_ranges_curr->start()
        }
    });
}

inline
bool cache_streamed_mutation::after_current_range(position_in_partition_view p) {
    return _position_cmp(p, _upper_bound) >= 0;
}

inline
future<> cache_streamed_mutation::start_reading_from_underlying() {
    _reading_underlying = true;
    auto end = _next_row_in_range ? position_in_partition(_next_row.position())
                                  : position_in_partition(_upper_bound);
    return _read_context->fast_forward_to(position_range{_lower_bound, std::move(end)});
}

inline
future<> cache_streamed_mutation::copy_from_cache_to_buffer() {
    position_in_partition_view next_lower_bound = _next_row.dummy() ? _next_row.position() : position_in_partition_view::after_key(_next_row.key());
    for (auto&& rts : _snp->range_tombstones(*_schema, _lower_bound, _next_row_in_range ? next_lower_bound : _upper_bound)) {
        add_to_buffer(std::move(rts));
        if (is_buffer_full()) {
            return make_ready_future<>();
        }
    }
    if (_next_row_in_range) {
        add_to_buffer(_next_row);
        return move_to_next_entry();
    } else {
        return move_to_next_range();
    }
}

inline
void cache_streamed_mutation::move_to_end() {
    drain_tombstones();
    _end_of_stream = true;
}

inline
future<> cache_streamed_mutation::move_to_next_range() {
    ++_ck_ranges_curr;
    if (_ck_ranges_curr == _ck_ranges_end) {
        move_to_end();
        return make_ready_future<>();
    } else {
        return move_to_current_range();
    }
}

inline
future<> cache_streamed_mutation::move_to_current_range() {
    _last_row_key = std::experimental::nullopt;
    _lower_bound = position_in_partition::for_range_start(*_ck_ranges_curr);
    _upper_bound = position_in_partition_view::for_range_end(*_ck_ranges_curr);
    auto complete_until_next = _next_row.advance_to(_lower_bound) || _next_row.continuous();
    _next_row_in_range = !after_current_range(_next_row.position());
    if (!complete_until_next) {
        return start_reading_from_underlying();
    }
    return make_ready_future<>();
}

// _next_row must be inside the range.
inline
future<> cache_streamed_mutation::move_to_next_entry() {
    if (no_clustering_row_between(*_schema, _next_row.position(), _upper_bound)) {
        return move_to_next_range();
    } else {
        if (!_next_row.next()) {
            move_to_end();
            return make_ready_future<>();
        }
        _next_row_in_range = !after_current_range(_next_row.position());
        if (!_next_row.continuous()) {
            return start_reading_from_underlying();
        }
        return make_ready_future<>();
    }
}

inline
void cache_streamed_mutation::drain_tombstones(position_in_partition_view pos) {
    while (auto mfo = _tombstones.get_next(pos)) {
        push_mutation_fragment(std::move(*mfo));
    }
}

inline
void cache_streamed_mutation::drain_tombstones() {
    while (auto mfo = _tombstones.get_next()) {
        push_mutation_fragment(std::move(*mfo));
    }
}

inline
void cache_streamed_mutation::add_to_buffer(mutation_fragment&& mf) {
    if (mf.is_clustering_row()) {
        add_to_buffer(std::move(std::move(mf).as_clustering_row()));
    } else {
        assert(mf.is_range_tombstone());
        add_to_buffer(std::move(mf).as_range_tombstone());
    }
}

inline
void cache_streamed_mutation::add_to_buffer(const partition_snapshot_row_cursor& row) {
    if (!row.dummy()) {
        add_to_buffer(row.row());
    }
}

inline
void cache_streamed_mutation::add_to_buffer(clustering_row&& row) {
    drain_tombstones(row.position());
    _last_row_key = row.key();
    _lower_bound = position_in_partition::after_key(row.key());
    push_mutation_fragment(std::move(row));
}

inline
void cache_streamed_mutation::add_to_buffer(range_tombstone&& rt) {
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
        _lsa_manager.run_in_update_section_with_allocator([&] {
            _snp->version()->partition().apply_row_tombstone(*_schema, rt);
        });
    }
}

inline
void cache_streamed_mutation::maybe_add_to_cache(const static_row& sr) {
    if (can_populate()) {
        _lsa_manager.run_in_update_section_with_allocator([&] {
            _snp->version()->partition().static_row().apply(*_schema, column_kind::static_column, sr.cells());
        });
    }
}

inline
void cache_streamed_mutation::maybe_set_static_row_continuous() {
    if (can_populate()) {
        _snp->version()->partition().set_static_row_continuous(true);
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
