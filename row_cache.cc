/*
 * Copyright (C) 2015 ScyllaDB
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

#include "row_cache.hh"
#include "core/memory.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>
#include "memtable.hh"
#include "partition_snapshot_reader.hh"
#include <chrono>
#include <boost/version.hpp>
#include <sys/sdt.h>
#include "stdx.hh"
#include "read_context.hh"
#include "schema_upgrader.hh"
#include "dirty_memory_manager.hh"
#include "cache_flat_mutation_reader.hh"
#include "real_dirty_memory_accounter.hh"

namespace cache {

logging::logger clogger("cache");

}

using namespace std::chrono_literals;
using namespace cache;

flat_mutation_reader
row_cache::create_underlying_reader(read_context& ctx, mutation_source& src, const dht::partition_range& pr) {
    ctx.on_underlying_created();
    return src.make_reader(_schema, pr, ctx.slice(), ctx.pc(), ctx.trace_state(), streamed_mutation::forwarding::yes);
}

cache_tracker::cache_tracker()
    : _garbage(_region, this)
    , _memtable_cleaner(_region, nullptr)
{
    setup_metrics();

    _region.make_evictable([this] {
        return with_allocator(_region.allocator(), [this] {
          // Removing a partition may require reading large keys when we rebalance
          // the rbtree, so linearize anything we read
          return with_linearized_managed_bytes([&] {
           try {
            if (!_garbage.empty()) {
                _garbage.clear_some();
                return memory::reclaiming_result::reclaimed_something;
            }
            if (!_memtable_cleaner.empty()) {
                _memtable_cleaner.clear_some();
                return memory::reclaiming_result::reclaimed_something;
            }
            if (_lru.empty()) {
                return memory::reclaiming_result::reclaimed_nothing;
            }
            _lru.back().on_evicted(*this);
            return memory::reclaiming_result::reclaimed_something;
           } catch (std::bad_alloc&) {
            // Bad luck, linearization during partition removal caused us to
            // fail.  Drop the entire cache so we can make forward progress.
            clear();
            return memory::reclaiming_result::reclaimed_something;
           }
          });
        });
    });
}

cache_tracker::~cache_tracker() {
    clear();
}

void cache_tracker::set_compaction_scheduling_group(seastar::scheduling_group sg) {
    _memtable_cleaner.set_scheduling_group(sg);
    _garbage.set_scheduling_group(sg);
}

void
cache_tracker::setup_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("cache", {
        sm::make_gauge("bytes_used", sm::description("current bytes used by the cache out of the total size of memory"), [this] { return _region.occupancy().used_space(); }),
        sm::make_gauge("bytes_total", sm::description("total size of memory for the cache"), [this] { return _region.occupancy().total_space(); }),
        sm::make_derive("partition_hits", sm::description("number of partitions needed by reads and found in cache"), _stats.partition_hits),
        sm::make_derive("partition_misses", sm::description("number of partitions needed by reads and missing in cache"), _stats.partition_misses),
        sm::make_derive("partition_insertions", sm::description("total number of partitions added to cache"), _stats.partition_insertions),
        sm::make_derive("row_hits", sm::description("total number of rows needed by reads and found in cache"), _stats.row_hits),
        sm::make_derive("row_misses", sm::description("total number of rows needed by reads and missing in cache"), _stats.row_misses),
        sm::make_derive("row_insertions", sm::description("total number of rows added to cache"), _stats.row_insertions),
        sm::make_derive("row_evictions", sm::description("total number of rows evicted from cache"), _stats.row_evictions),
        sm::make_derive("row_removals", sm::description("total number of invalidated rows"), _stats.row_removals),
        sm::make_derive("static_row_insertions", sm::description("total number of static rows added to cache"), _stats.static_row_insertions),
        sm::make_derive("concurrent_misses_same_key", sm::description("total number of operation with misses same key"), _stats.concurrent_misses_same_key),
        sm::make_derive("partition_merges", sm::description("total number of partitions merged"), _stats.partition_merges),
        sm::make_derive("partition_evictions", sm::description("total number of evicted partitions"), _stats.partition_evictions),
        sm::make_derive("partition_removals", sm::description("total number of invalidated partitions"), _stats.partition_removals),
        sm::make_derive("mispopulations", sm::description("number of entries not inserted by reads"), _stats.mispopulations),
        sm::make_gauge("partitions", sm::description("total number of cached partitions"), _stats.partitions),
        sm::make_gauge("rows", sm::description("total number of cached rows"), _stats.rows),
        sm::make_derive("reads", sm::description("number of started reads"), _stats.reads),
        sm::make_derive("reads_with_misses", sm::description("number of reads which had to read from sstables"), _stats.reads_with_misses),
        sm::make_gauge("active_reads", sm::description("number of currently active reads"), [this] { return _stats.active_reads(); }),
        sm::make_derive("sstable_reader_recreations", sm::description("number of times sstable reader was recreated due to memtable flush"), _stats.underlying_recreations),
        sm::make_derive("sstable_partition_skips", sm::description("number of times sstable reader was fast forwarded across partitions"), _stats.underlying_partition_skips),
        sm::make_derive("sstable_row_skips", sm::description("number of times sstable reader was fast forwarded within a partition"), _stats.underlying_row_skips),
        sm::make_derive("pinned_dirty_memory_overload", sm::description("amount of pinned bytes that we tried to unpin over the limit. This should sit constantly at 0, and any number different than 0 is indicative of a bug"), _stats.pinned_dirty_memory_overload),
        sm::make_derive("rows_processed_from_memtable", _stats.rows_processed_from_memtable,
            sm::description("total number of rows in memtables which were processed during cache update on memtable flush")),
        sm::make_derive("rows_dropped_from_memtable", _stats.rows_dropped_from_memtable,
            sm::description("total number of rows in memtables which were dropped during cache update on memtable flush")),
        sm::make_derive("rows_merged_from_memtable", _stats.rows_merged_from_memtable,
            sm::description("total number of rows in memtables which were merged with existing rows during cache update on memtable flush")),
    });
}

void cache_tracker::clear() {
    auto partitions_before = _stats.partitions;
    auto rows_before = _stats.rows;
    // We need to clear garbage first because garbage versions cannot be evicted from,
    // mutation_partition::clear_gently() destroys intrusive tree invariants.
    with_allocator(_region.allocator(), [this] {
        _garbage.clear();
        _memtable_cleaner.clear();
        while (!_lru.empty()) {
            _lru.back().on_evicted(*this);
        }
    });
    _stats.partition_removals += partitions_before;
    _stats.row_removals += rows_before;
    allocator().invalidate_references();
}

void cache_tracker::touch(rows_entry& e) {
    if (e._lru_link.is_linked()) { // last dummy may not be linked if evicted.
        _lru.erase(_lru.iterator_to(e));
    }
    _lru.push_front(e);
}

void cache_tracker::insert(cache_entry& entry) {
    insert(entry.partition());
    ++_stats.partition_insertions;
    ++_stats.partitions;
    // partition_range_cursor depends on this to detect invalidation of _end
    _region.allocator().invalidate_references();
}

void cache_tracker::on_partition_erase() {
    --_stats.partitions;
    ++_stats.partition_removals;
    allocator().invalidate_references();
}

void cache_tracker::unlink(rows_entry& row) noexcept {
    row._lru_link.unlink();
}

void cache_tracker::on_partition_merge() {
    ++_stats.partition_merges;
}

void cache_tracker::on_partition_hit() {
    ++_stats.partition_hits;
}

void cache_tracker::on_partition_miss() {
    ++_stats.partition_misses;
}

void cache_tracker::on_partition_eviction() {
    --_stats.partitions;
    ++_stats.partition_evictions;
}

void cache_tracker::on_row_eviction() {
    --_stats.rows;
    ++_stats.row_evictions;
}

void cache_tracker::on_row_hit() {
    ++_stats.row_hits;
}

void cache_tracker::on_row_miss() {
    ++_stats.row_misses;
}

void cache_tracker::on_mispopulate() {
    ++_stats.mispopulations;
}

void cache_tracker::on_miss_already_populated() {
    ++_stats.concurrent_misses_same_key;
}

void cache_tracker::pinned_dirty_memory_overload(uint64_t bytes) {
    _stats.pinned_dirty_memory_overload += bytes;
}

allocation_strategy& cache_tracker::allocator() {
    return _region.allocator();
}

logalloc::region& cache_tracker::region() {
    return _region;
}

const logalloc::region& cache_tracker::region() const {
    return _region;
}

// Stable cursor over partition entries from given range.
//
// Must be accessed with reclaim lock held on the cache region.
// The position of the cursor is always valid, but cache entry reference
// is not always valid. It remains valid as long as the iterators
// into _cache._partitions remain valid. Cache entry reference can be
// brought back to validity by calling refresh().
//
class partition_range_cursor final {
    std::reference_wrapper<row_cache> _cache;
    row_cache::partitions_type::iterator _it;
    row_cache::partitions_type::iterator _end;
    dht::ring_position_view _start_pos;
    dht::ring_position_view _end_pos;
    stdx::optional<dht::decorated_key> _last;
    uint64_t _last_reclaim_count;
private:
    void set_position(cache_entry& e) {
        // FIXME: make ring_position_view convertible to ring_position, so we can use e.position()
        if (e.is_dummy_entry()) {
            _last = {};
            _start_pos = dht::ring_position_view::max();
        } else {
            _last = e.key();
            _start_pos = dht::ring_position_view(*_last);
        }
    }
public:
    // Creates a cursor positioned at the lower bound of the range.
    // The cache entry reference is not valid.
    // The range reference must remain live as long as this instance is used.
    partition_range_cursor(row_cache& cache, const dht::partition_range& range)
        : _cache(cache)
        , _start_pos(dht::ring_position_view::for_range_start(range))
        , _end_pos(dht::ring_position_view::for_range_end(range))
        , _last_reclaim_count(std::numeric_limits<uint64_t>::max())
    { }

    // Returns true iff the cursor is valid
    bool valid() const {
        return _cache.get().get_cache_tracker().allocator().invalidate_counter() == _last_reclaim_count;
    }

    // Repositions the cursor to the first entry with position >= pos.
    // Returns true iff the position of the cursor is equal to pos.
    // Can be called on invalid cursor, in which case it brings it back to validity.
    // Strong exception guarantees.
    bool advance_to(dht::ring_position_view pos) {
        auto cmp = cache_entry::compare(_cache.get()._schema);
        if (cmp(_end_pos, pos)) { // next() may have moved _start_pos past the _end_pos.
            _end_pos = pos;
        }
        _end = _cache.get()._partitions.lower_bound(_end_pos, cmp);
        _it = _cache.get()._partitions.lower_bound(pos, cmp);
        auto same = !cmp(pos, _it->position());
        set_position(*_it);
        _last_reclaim_count = _cache.get().get_cache_tracker().allocator().invalidate_counter();
        return same;
    }

    // Ensures that cache entry reference is valid.
    // The cursor will point at the first entry with position >= the current position.
    // Returns true if and only if the position of the cursor did not change.
    // Strong exception guarantees.
    bool refresh() {
        if (valid()) {
            return true;
        }
        return advance_to(_start_pos);
    }

    // Positions the cursor at the next entry.
    // May advance past the requested range. Use in_range() after the call to determine that.
    // Call only when in_range() and cache entry reference is valid.
    // Strong exception guarantees.
    void next() {
        auto next = std::next(_it);
        set_position(*next);
        _it = std::move(next);
    }

    // Valid only after refresh() and before _cache._partitions iterators are invalidated.
    // Points inside the requested range if in_range().
    cache_entry& entry() {
        return *_it;
    }

    // Call only when cache entry reference is valid.
    bool in_range() {
        return _it != _end;
    }

    // Returns current position of the cursor.
    // Result valid as long as this instance is valid and not advanced.
    dht::ring_position_view position() const {
        return _start_pos;
    }
};

future<> read_context::create_underlying(bool skip_first_fragment, db::timeout_clock::time_point timeout) {
    if (_range_query) {
        // FIXME: Singular-range mutation readers don't support fast_forward_to(), so need to use a wide range
        // here in case the same reader will need to be fast forwarded later.
        _sm_range = dht::partition_range({dht::ring_position(*_key)}, {dht::ring_position(*_key)});
    } else {
        _sm_range = dht::partition_range::make_singular({dht::ring_position(*_key)});
    }
    return _underlying.fast_forward_to(std::move(_sm_range), *_underlying_snapshot, _phase, timeout).then([this, skip_first_fragment, timeout] {
        _underlying_snapshot = {};
        if (skip_first_fragment) {
            return _underlying.underlying()(timeout).then([](auto &&mf) {});
        } else {
            return make_ready_future<>();
        }
    });
}

static flat_mutation_reader read_directly_from_underlying(read_context& reader) {
    flat_mutation_reader res = make_delegating_reader(reader.underlying().underlying());
    if (reader.schema()->version() != reader.underlying().underlying().schema()->version()) {
        res = transform(std::move(res), schema_upgrader(reader.schema()));
    }
    if (reader.fwd() == streamed_mutation::forwarding::no) {
        res = make_nonforwardable(std::move(res), true);
    }
    return std::move(res);
}

// Reader which populates the cache using data from the delegate.
class single_partition_populating_reader final : public flat_mutation_reader::impl {
    row_cache& _cache;
    lw_shared_ptr<read_context> _read_context;
    flat_mutation_reader_opt _reader;
private:
    future<> create_reader(db::timeout_clock::time_point timeout) {
        auto src_and_phase = _cache.snapshot_of(_read_context->range().start()->value());
        auto phase = src_and_phase.phase;
        _read_context->enter_partition(_read_context->range().start()->value().as_decorated_key(), src_and_phase.snapshot, phase);
        return _read_context->create_underlying(false, timeout).then([this, phase, timeout] {
          return _read_context->underlying().underlying()(timeout).then([this, phase] (auto&& mfopt) {
            if (!mfopt) {
                if (phase == _cache.phase_of(_read_context->range().start()->value())) {
                    _cache._read_section(_cache._tracker.region(), [this] {
                        with_allocator(_cache._tracker.allocator(), [this] {
                            dht::decorated_key dk = _read_context->range().start()->value().as_decorated_key();
                            _cache.do_find_or_create_entry(dk, nullptr, [&] (auto i) {
                                mutation_partition mp(_cache._schema);
                                cache_entry* entry = current_allocator().construct<cache_entry>(
                                    _cache._schema, std::move(dk), std::move(mp));
                                _cache._tracker.insert(*entry);
                                entry->set_continuous(i->continuous());
                                return _cache._partitions.insert_before(i, *entry);
                            }, [&] (auto i) {
                                _cache._tracker.on_miss_already_populated();
                            });
                        });
                    });
                } else {
                    _cache._tracker.on_mispopulate();
                }
                _end_of_stream = true;
            } else if (phase == _cache.phase_of(_read_context->range().start()->value())) {
                _reader = _cache._read_section(_cache._tracker.region(), [&] {
                    cache_entry& e = _cache.find_or_create(mfopt->as_partition_start().key(), mfopt->as_partition_start().partition_tombstone(), phase);
                    return e.read(_cache, *_read_context, phase);
                });
            } else {
                _cache._tracker.on_mispopulate();
                _reader = read_directly_from_underlying(*_read_context);
                this->push_mutation_fragment(std::move(*mfopt));
            }
          });
        });
    }
public:
    single_partition_populating_reader(row_cache& cache,
            lw_shared_ptr<read_context> context)
        : impl(context->schema())
        , _cache(cache)
        , _read_context(std::move(context))
    { }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        if (!_reader) {
            return create_reader(timeout).then([this, timeout] {
                if (_end_of_stream) {
                    return make_ready_future<>();
                }
                return fill_buffer(timeout);
            });
        }
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
            return fill_buffer_from(*_reader, timeout).then([this] (bool reader_finished) {
                if (reader_finished) {
                    _end_of_stream = true;
                }
            });
        });
    }
    virtual void next_partition() override {
        if (_reader) {
            clear_buffer();
            _end_of_stream = true;
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _end_of_stream = true;
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        if (!_reader) {
            _end_of_stream = true;
            return make_ready_future<>();
        }
        assert(bool(_read_context->fwd()));
        _end_of_stream = false;
        forward_buffer_to(pr.start());
        return _reader->fast_forward_to(std::move(pr), timeout);
    }
    virtual size_t buffer_size() const override {
        if (_reader) {
            return flat_mutation_reader::impl::buffer_size() + _reader->buffer_size();
        }
        return flat_mutation_reader::impl::buffer_size();
    }
};

void cache_tracker::clear_continuity(cache_entry& ce) {
    ce.set_continuous(false);
}

void row_cache::on_partition_hit() {
    _tracker.on_partition_hit();
}

void row_cache::on_partition_miss() {
    _tracker.on_partition_miss();
}

void row_cache::on_row_hit() {
    _stats.hits.mark();
    _tracker.on_row_hit();
}

void row_cache::on_mispopulate() {
    _tracker.on_mispopulate();
}

void row_cache::on_row_miss() {
    _stats.misses.mark();
    _tracker.on_row_miss();
}

void row_cache::on_static_row_insert() {
    ++_tracker._stats.static_row_insertions;
}

class range_populating_reader {
    row_cache& _cache;
    autoupdating_underlying_reader& _reader;
    stdx::optional<row_cache::previous_entry_pointer> _last_key;
    read_context& _read_context;
private:
    bool can_set_continuity() const {
        return _last_key && _reader.creation_phase() == _cache.phase_of(_reader.population_range_start());
    }
    void handle_end_of_stream() {
        if (!can_set_continuity()) {
            _cache.on_mispopulate();
            return;
        }
        if (!_reader.range().end() || !_reader.range().end()->is_inclusive()) {
            cache_entry::compare cmp(_cache._schema);
            auto it = _reader.range().end() ? _cache._partitions.find(_reader.range().end()->value(), cmp)
                                           : std::prev(_cache._partitions.end());
            if (it != _cache._partitions.end()) {
                if (it == _cache._partitions.begin()) {
                    if (!_last_key->_key) {
                        it->set_continuous(true);
                    } else {
                        _cache.on_mispopulate();
                    }
                } else {
                    auto prev = std::prev(it);
                    if (prev->key().equal(*_cache._schema, *_last_key->_key)) {
                        it->set_continuous(true);
                    } else {
                        _cache.on_mispopulate();
                    }
                }
            }
        }
    }
public:
    range_populating_reader(row_cache& cache, read_context& ctx)
        : _cache(cache)
        , _reader(ctx.underlying())
        , _read_context(ctx)
    {}

    future<flat_mutation_reader_opt, mutation_fragment_opt > operator()(db::timeout_clock::time_point timeout) {
        return _reader.move_to_next_partition(timeout).then([this] (auto&& mfopt) mutable {
            {
                if (!mfopt) {
                    this->handle_end_of_stream();
                    return make_ready_future<flat_mutation_reader_opt, mutation_fragment_opt>(stdx::nullopt, stdx::nullopt);
                }
                _cache.on_partition_miss();
                const partition_start& ps = mfopt->as_partition_start();
                const dht::decorated_key& key = ps.key();
                if (_reader.creation_phase() == _cache.phase_of(key)) {
                    return _cache._read_section(_cache._tracker.region(), [&] {
                        cache_entry& e = _cache.find_or_create(key,
                                                               ps.partition_tombstone(),
                                                               _reader.creation_phase(),
                                                               this->can_set_continuity() ? &*_last_key : nullptr);
                        _last_key = row_cache::previous_entry_pointer(key);
                        return make_ready_future<flat_mutation_reader_opt, mutation_fragment_opt>(
                            e.read(_cache, _read_context, _reader.creation_phase()), stdx::nullopt);
                    });
                } else {
                    _cache._tracker.on_mispopulate();
                    _last_key = row_cache::previous_entry_pointer(key);
                    return make_ready_future<flat_mutation_reader_opt, mutation_fragment_opt>(
                        read_directly_from_underlying(_read_context), std::move(mfopt));
                }
            }
        });
    }

    future<> fast_forward_to(dht::partition_range&& pr, db::timeout_clock::time_point timeout) {
        if (!pr.start()) {
            _last_key = row_cache::previous_entry_pointer();
        } else if (!pr.start()->is_inclusive() && pr.start()->value().has_key()) {
            _last_key = row_cache::previous_entry_pointer(pr.start()->value().as_decorated_key());
        } else {
            // Inclusive start bound, cannot set continuity flag.
            _last_key = {};
        }

        return _reader.fast_forward_to(std::move(pr), timeout);
    }
};

class scanning_and_populating_reader final : public flat_mutation_reader::impl {
    const dht::partition_range* _pr;
    row_cache& _cache;
    lw_shared_ptr<read_context> _read_context;
    partition_range_cursor _primary;
    range_populating_reader _secondary_reader;
    bool _secondary_in_progress = false;
    bool _advance_primary = false;
    stdx::optional<dht::partition_range::bound> _lower_bound;
    dht::partition_range _secondary_range;
    flat_mutation_reader_opt _reader;
private:
    flat_mutation_reader read_from_entry(cache_entry& ce) {
        _cache.upgrade_entry(ce);
        _cache.on_partition_hit();
        return ce.read(_cache, *_read_context);
    }

    static dht::ring_position_view as_ring_position_view(const stdx::optional<dht::partition_range::bound>& lower_bound) {
        return lower_bound ? dht::ring_position_view(lower_bound->value(), dht::ring_position_view::after_key(!lower_bound->is_inclusive()))
                           : dht::ring_position_view::min();
    }

    flat_mutation_reader_opt do_read_from_primary(db::timeout_clock::time_point timeout) {
        return _cache._read_section(_cache._tracker.region(), [this] {
            return with_linearized_managed_bytes([&] () -> flat_mutation_reader_opt {
                bool not_moved = true;
                if (!_primary.valid()) {
                    not_moved = _primary.advance_to(as_ring_position_view(_lower_bound));
                }

                if (_advance_primary && not_moved) {
                    _primary.next();
                    not_moved = false;
                }
                _advance_primary = false;

                if (not_moved || _primary.entry().continuous()) {
                    if (!_primary.in_range()) {
                        return stdx::nullopt;
                    }
                    cache_entry& e = _primary.entry();
                    auto fr = read_from_entry(e);
                    _lower_bound = dht::partition_range::bound{e.key(), false};
                    // Delay the call to next() so that we don't see stale continuity on next invocation.
                    _advance_primary = true;
                    return flat_mutation_reader_opt(std::move(fr));
                } else {
                    if (_primary.in_range()) {
                        cache_entry& e = _primary.entry();
                        _secondary_range = dht::partition_range(_lower_bound,
                            dht::partition_range::bound{e.key(), false});
                        _lower_bound = dht::partition_range::bound{e.key(), true};
                        _secondary_in_progress = true;
                        return stdx::nullopt;
                    } else {
                        dht::ring_position_comparator cmp(*_read_context->schema());
                        auto range = _pr->trim_front(stdx::optional<dht::partition_range::bound>(_lower_bound), cmp);
                        if (!range) {
                            return stdx::nullopt;
                        }
                        _lower_bound = dht::partition_range::bound{dht::ring_position::max()};
                        _secondary_range = std::move(*range);
                        _secondary_in_progress = true;
                        return stdx::nullopt;
                    }
                }
            });
        });
    }

    future<flat_mutation_reader_opt> read_from_primary(db::timeout_clock::time_point timeout) {
        auto fro = do_read_from_primary(timeout);
        if (!_secondary_in_progress) {
            return make_ready_future<flat_mutation_reader_opt>(std::move(fro));
        }
        return _secondary_reader.fast_forward_to(std::move(_secondary_range), timeout).then([this, timeout] {
            return read_from_secondary(timeout);
        });
    }

    future<flat_mutation_reader_opt> read_from_secondary(db::timeout_clock::time_point timeout) {
        return _secondary_reader(timeout).then([this, timeout] (flat_mutation_reader_opt fropt, mutation_fragment_opt ps) {
            if (fropt) {
                if (ps) {
                    push_mutation_fragment(std::move(*ps));
                }
                return make_ready_future<flat_mutation_reader_opt>(std::move(fropt));
            } else {
                _secondary_in_progress = false;
                return read_from_primary(timeout);
            }
        });
    }
    future<> read_next_partition(db::timeout_clock::time_point timeout) {
        return (_secondary_in_progress ? read_from_secondary(timeout) : read_from_primary(timeout)).then([this] (auto&& fropt) {
            if (bool(fropt)) {
                _reader = std::move(fropt);
            } else {
                _end_of_stream = true;
            }
        });
    }
    void on_end_of_stream() {
        if (_read_context->fwd() == streamed_mutation::forwarding::yes) {
            _end_of_stream = true;
        } else {
            _reader = {};
        }
    }
public:
    scanning_and_populating_reader(row_cache& cache,
                                   const dht::partition_range& range,
                                   lw_shared_ptr<read_context> context)
        : impl(context->schema())
        , _pr(&range)
        , _cache(cache)
        , _read_context(std::move(context))
        , _primary(cache, range)
        , _secondary_reader(cache, *_read_context)
        , _lower_bound(range.start())
    { }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
            if (!_reader) {
                return read_next_partition(timeout);
            } else {
                return fill_buffer_from(*_reader, timeout).then([this] (bool reader_finished) {
                    if (reader_finished) {
                        on_end_of_stream();
                    }
                });
            }
        });
    }
    virtual void next_partition() override {
        if (_read_context->fwd() == streamed_mutation::forwarding::yes) {
            if (_reader) {
                clear_buffer();
                _reader->next_partition();
                _end_of_stream = false;
            }
        } else {
            clear_buffer_to_next_partition();
            if (_reader && is_buffer_empty()) {
                _reader->next_partition();
            }
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _reader = {};
        _end_of_stream = false;
        _secondary_in_progress = false;
        _advance_primary = false;
        _pr = &pr;
        _primary = partition_range_cursor{_cache, pr};
        _lower_bound = pr.start();
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) override {
        forward_buffer_to(cr.start());
        if (_reader) {
            _end_of_stream = false;
            return _reader->fast_forward_to(std::move(cr), timeout);
        } else {
            _end_of_stream = true;
            return make_ready_future<>();
        }
    }
    virtual size_t buffer_size() const override {
        if (_reader) {
            return flat_mutation_reader::impl::buffer_size() + _reader->buffer_size();
        }
        return flat_mutation_reader::impl::buffer_size();
    }
};

flat_mutation_reader
row_cache::make_scanning_reader(const dht::partition_range& range, lw_shared_ptr<read_context> context) {
    return make_flat_mutation_reader<scanning_and_populating_reader>(*this, range, std::move(context));
}

flat_mutation_reader
row_cache::make_reader(schema_ptr s,
                       const dht::partition_range& range,
                       const query::partition_slice& slice,
                       const io_priority_class& pc,
                       tracing::trace_state_ptr trace_state,
                       streamed_mutation::forwarding fwd,
                       mutation_reader::forwarding fwd_mr)
{
    auto ctx = make_lw_shared<read_context>(*this, s, range, slice, pc, trace_state, fwd, fwd_mr);

    if (!ctx->is_range_query()) {
        return _read_section(_tracker.region(), [&] {
            return with_linearized_managed_bytes([&] {
                cache_entry::compare cmp(_schema);
                auto&& pos = ctx->range().start()->value();
                auto i = _partitions.lower_bound(pos, cmp);
                if (i != _partitions.end() && !cmp(pos, i->position())) {
                    cache_entry& e = *i;
                    upgrade_entry(e);
                    on_partition_hit();
                    return e.read(*this, *ctx);
                } else if (i->continuous()) {
                    return make_empty_flat_reader(std::move(s));
                } else {
                    on_partition_miss();
                    return make_flat_mutation_reader<single_partition_populating_reader>(*this, std::move(ctx));
                }
            });
        });
    }

    return make_scanning_reader(range, std::move(ctx));
}


row_cache::~row_cache() {
    with_allocator(_tracker.allocator(), [this] {
        _partitions.clear_and_dispose([this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            if (!p->is_dummy_entry()) {
                _tracker.on_partition_erase();
            }
            p->evict(_tracker);
            deleter(p);
        });
    });
}

void row_cache::clear_now() noexcept {
    with_allocator(_tracker.allocator(), [this] {
        auto it = _partitions.erase_and_dispose(_partitions.begin(), partitions_end(), [this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_partition_erase();
            p->evict(_tracker);
            deleter(p);
        });
        _tracker.clear_continuity(*it);
    });
}

template<typename CreateEntry, typename VisitEntry>
//requires requires(CreateEntry create, VisitEntry visit, row_cache::partitions_type::iterator it) {
//        { create(it) } -> row_cache::partitions_type::iterator;
//        { visit(it) } -> void;
//    }
cache_entry& row_cache::do_find_or_create_entry(const dht::decorated_key& key,
    const previous_entry_pointer* previous, CreateEntry&& create_entry, VisitEntry&& visit_entry)
{
    return with_allocator(_tracker.allocator(), [&] () -> cache_entry& {
            return with_linearized_managed_bytes([&] () -> cache_entry& {
                auto i = _partitions.lower_bound(key, cache_entry::compare(_schema));
                if (i == _partitions.end() || !i->key().equal(*_schema, key)) {
                    i = create_entry(i);
                } else {
                    visit_entry(i);
                }

                if (!previous) {
                    return *i;
                }

                if ((!previous->_key && i == _partitions.begin())
                    || (previous->_key && i != _partitions.begin()
                        && std::prev(i)->key().equal(*_schema, *previous->_key))) {
                    i->set_continuous(true);
                } else {
                    on_mispopulate();
                }

                return *i;
            });
    });
}

cache_entry& row_cache::find_or_create(const dht::decorated_key& key, tombstone t, row_cache::phase_type phase, const previous_entry_pointer* previous) {
    return do_find_or_create_entry(key, previous, [&] (auto i) { // create
        auto entry = current_allocator().construct<cache_entry>(cache_entry::incomplete_tag{}, _schema, key, t);
        _tracker.insert(*entry);
        return _partitions.insert_before(i, *entry);
    }, [&] (auto i) { // visit
        _tracker.on_miss_already_populated();
        cache_entry& e = *i;
        e.partition().open_version(*e.schema(), &_tracker, phase).partition().apply(t);
        upgrade_entry(e);
    });
}

void row_cache::populate(const mutation& m, const previous_entry_pointer* previous) {
  _populate_section(_tracker.region(), [&] {
    do_find_or_create_entry(m.decorated_key(), previous, [&] (auto i) {
        cache_entry* entry = current_allocator().construct<cache_entry>(
                m.schema(), m.decorated_key(), m.partition());
        _tracker.insert(*entry);
        entry->set_continuous(i->continuous());
        i = _partitions.insert_before(i, *entry);
        upgrade_entry(*i);
        return i;
    }, [&] (auto i) {
        throw std::runtime_error(sprint("cache already contains entry for {}", m.key()));
    });
  });
}

mutation_source& row_cache::snapshot_for_phase(phase_type phase) {
    if (phase == _underlying_phase) {
        return _underlying;
    } else {
        if (phase + 1 < _underlying_phase) {
            throw std::runtime_error(sprint("attempted to read from retired phase {} (current={})", phase, _underlying_phase));
        }
        return *_prev_snapshot;
    }
}

row_cache::snapshot_and_phase row_cache::snapshot_of(dht::ring_position_view pos) {
    dht::ring_position_less_comparator less(*_schema);
    if (!_prev_snapshot_pos || less(pos, *_prev_snapshot_pos)) {
        return {_underlying, _underlying_phase};
    }
    return {*_prev_snapshot, _underlying_phase - 1};
}

void row_cache::invalidate_sync(memtable& m) noexcept {
    with_allocator(_tracker.allocator(), [&m, this] () {
        logalloc::reclaim_lock _(_tracker.region());
        bool blow_cache = false;
        // Note: clear_and_dispose() ought not to look up any keys, so it doesn't require
        // with_linearized_managed_bytes(), but invalidate() does.
        m.partitions.clear_and_dispose([this, deleter = current_deleter<memtable_entry>(), &blow_cache] (memtable_entry* entry) {
            with_linearized_managed_bytes([&] {
                try {
                    invalidate_locked(entry->key());
                } catch (...) {
                    blow_cache = true;
                }
                entry->partition().evict(_tracker.memtable_cleaner());
                deleter(entry);
            });
        });
        if (blow_cache) {
            // We failed to invalidate the key, presumably due to with_linearized_managed_bytes()
            // running out of memory.  Recover using clear_now(), which doesn't throw.
            clear_now();
        }
    });
}

row_cache::phase_type row_cache::phase_of(dht::ring_position_view pos) {
    dht::ring_position_less_comparator less(*_schema);
    if (!_prev_snapshot_pos || less(pos, *_prev_snapshot_pos)) {
        return _underlying_phase;
    }
    return _underlying_phase - 1;
}

template <typename Updater>
future<> row_cache::do_update(external_updater eu, memtable& m, Updater updater) {
  return do_update(std::move(eu), [this, &m, updater = std::move(updater)] {
    real_dirty_memory_accounter real_dirty_acc(m, _tracker);
    m.on_detach_from_region_group();
    _tracker.region().merge(m); // Now all data in memtable belongs to cache
    _tracker.memtable_cleaner().merge(m._cleaner);
    STAP_PROBE(scylla, row_cache_update_start);
    auto cleanup = defer([&m, this] {
        invalidate_sync(m);
        STAP_PROBE(scylla, row_cache_update_end);
    });

    return seastar::async([this, &m, updater = std::move(updater), real_dirty_acc = std::move(real_dirty_acc)] () mutable {
        coroutine update;
        size_t size_entry;
        // In case updater fails, we must bring the cache to consistency without deferring.
        auto cleanup = defer([&m, this] {
            invalidate_sync(m);
            _prev_snapshot_pos = {};
            _prev_snapshot = {};
        });
        partition_presence_checker is_present = _prev_snapshot->make_partition_presence_checker();
        while (!m.partitions.empty()) {
            with_allocator(_tracker.allocator(), [&] () {
                auto cmp = cache_entry::compare(_schema);
                {
                    size_t partition_count = 0;
                    {
                        STAP_PROBE(scylla, row_cache_update_one_batch_start);
                        // FIXME: we should really be checking should_yield() here instead of
                        // need_preempt(). However, should_yield() is currently quite
                        // expensive and we need to amortize it somehow.
                        do {
                          STAP_PROBE(scylla, row_cache_update_partition_start);
                          with_linearized_managed_bytes([&] {
                            if (!update) {
                                _update_section(_tracker.region(), [&] {
                                    memtable_entry& mem_e = *m.partitions.begin();
                                    size_entry = mem_e.size_in_allocator_without_rows(_tracker.allocator());
                                    auto cache_i = _partitions.lower_bound(mem_e.key(), cmp);
                                    update = updater(_update_section, cache_i, mem_e, is_present, real_dirty_acc);
                                });
                            }
                            // We use cooperative deferring instead of futures so that
                            // this layer has a chance to restore invariants before deferring,
                            // in particular set _prev_snapshot_pos to the correct value.
                            if (update.run() == stop_iteration::no) {
                                return;
                            }
                            update = {};
                            real_dirty_acc.unpin_memory(size_entry);
                            _update_section(_tracker.region(), [&] {
                                auto i = m.partitions.begin();
                                memtable_entry& mem_e = *i;
                                m.partitions.erase(i);
                                mem_e.partition().evict(_tracker.memtable_cleaner());
                                current_allocator().destroy(&mem_e);
                            });
                            ++partition_count;
                          });
                          STAP_PROBE(scylla, row_cache_update_partition_end);
                        } while (!m.partitions.empty() && !need_preempt());
                        with_allocator(standard_allocator(), [&] {
                            if (m.partitions.empty()) {
                                _prev_snapshot_pos = {};
                            } else {
                                _update_section(_tracker.region(), [&] {
                                    _prev_snapshot_pos = dht::ring_position(m.partitions.begin()->key());
                                });
                            }
                        });
                        STAP_PROBE1(scylla, row_cache_update_one_batch_end, partition_count);
                    }
                }
            });
            real_dirty_acc.commit();
            seastar::thread::yield();
        }
    }).finally([cleanup = std::move(cleanup)] {});
  });
}

future<> row_cache::update(external_updater eu, memtable& m) {
    return do_update(std::move(eu), m, [this] (logalloc::allocating_section& alloc,
            row_cache::partitions_type::iterator cache_i, memtable_entry& mem_e, partition_presence_checker& is_present,
            real_dirty_memory_accounter& acc) mutable {
        // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
        // FIXME: keep a bitmap indicating which sstables we do cover, so we don't have to
        //        search it.
        if (cache_i != partitions_end() && cache_i->key().equal(*_schema, mem_e.key())) {
            cache_entry& entry = *cache_i;
            upgrade_entry(entry);
            _tracker.on_partition_merge();
            return entry.partition().apply_to_incomplete(*_schema, std::move(mem_e.partition()), *mem_e.schema(), _tracker.memtable_cleaner(),
                alloc, _tracker.region(), _tracker, _underlying_phase, acc);
        } else if (cache_i->continuous()
                   || with_allocator(standard_allocator(), [&] { return is_present(mem_e.key()); })
                      == partition_presence_checker_result::definitely_doesnt_exist) {
            // Partition is absent in underlying. First, insert a neutral partition entry.
            cache_entry* entry = current_allocator().construct<cache_entry>(cache_entry::evictable_tag(),
                _schema, dht::decorated_key(mem_e.key()),
                partition_entry::make_evictable(*_schema, mutation_partition(_schema)));
            entry->set_continuous(cache_i->continuous());
            _tracker.insert(*entry);
            _partitions.insert_before(cache_i, *entry);
            return entry->partition().apply_to_incomplete(*_schema, std::move(mem_e.partition()), *mem_e.schema(), _tracker.memtable_cleaner(),
                alloc, _tracker.region(), _tracker, _underlying_phase, acc);
        } else {
            return make_empty_coroutine();
        }
    });
}

future<> row_cache::update_invalidating(external_updater eu, memtable& m) {
    return do_update(std::move(eu), m, [this] (logalloc::allocating_section& alloc,
        row_cache::partitions_type::iterator cache_i, memtable_entry& mem_e, partition_presence_checker& is_present,
        real_dirty_memory_accounter& acc)
    {
        if (cache_i != partitions_end() && cache_i->key().equal(*_schema, mem_e.key())) {
            // FIXME: Invalidate only affected row ranges.
            // This invalidates all information about the partition.
            cache_entry& e = *cache_i;
            e.evict(_tracker);
            e.on_evicted(_tracker);
        } else {
            _tracker.clear_continuity(*cache_i);
        }
        // FIXME: subtract gradually from acc.
        return make_empty_coroutine();
    });
}

void row_cache::refresh_snapshot() {
    _underlying = _snapshot_source();
}

void row_cache::touch(const dht::decorated_key& dk) {
 _read_section(_tracker.region(), [&] {
  with_linearized_managed_bytes([&] {
    auto i = _partitions.find(dk, cache_entry::compare(_schema));
    if (i != _partitions.end()) {
        for (partition_version& pv : i->partition().versions_from_oldest()) {
            for (rows_entry& row : pv.partition().clustered_rows()) {
                _tracker.touch(row);
            }
        }
    }
  });
 });
}

void row_cache::unlink_from_lru(const dht::decorated_key& dk) {
    _read_section(_tracker.region(), [&] {
        with_linearized_managed_bytes([&] {
            auto i = _partitions.find(dk, cache_entry::compare(_schema));
            if (i != _partitions.end()) {
                for (partition_version& pv : i->partition().versions_from_oldest()) {
                    for (rows_entry& row : pv.partition().clustered_rows()) {
                        _tracker.unlink(row);
                    }
                }
            }
        });
    });
}

void row_cache::invalidate_locked(const dht::decorated_key& dk) {
    auto pos = _partitions.lower_bound(dk, cache_entry::compare(_schema));
    if (pos == partitions_end() || !pos->key().equal(*_schema, dk)) {
        _tracker.clear_continuity(*pos);
    } else {
        auto it = _partitions.erase_and_dispose(pos,
            [this, &dk, deleter = current_deleter<cache_entry>()](auto&& p) mutable {
                _tracker.on_partition_erase();
                p->evict(_tracker);
                deleter(p);
            });
        _tracker.clear_continuity(*it);
    }
}

future<> row_cache::invalidate(external_updater eu, const dht::decorated_key& dk) {
    return invalidate(std::move(eu), dht::partition_range::make_singular(dk));
}

future<> row_cache::invalidate(external_updater eu, const dht::partition_range& range) {
    return invalidate(std::move(eu), dht::partition_range_vector({range}));
}

future<> row_cache::invalidate(external_updater eu, dht::partition_range_vector&& ranges) {
    return do_update(std::move(eu), [this, ranges = std::move(ranges)] {
        auto on_failure = defer([this] { this->clear_now(); });
        with_linearized_managed_bytes([&] {
            for (auto&& range : ranges) {
                this->invalidate_unwrapped(range);
            }
        });
        on_failure.cancel();
        return make_ready_future<>();
    });
}

void row_cache::evict(const dht::partition_range& range) {
    invalidate_unwrapped(range);
}

void row_cache::invalidate_unwrapped(const dht::partition_range& range) {
    logalloc::reclaim_lock _(_tracker.region());

    auto cmp = cache_entry::compare(_schema);
    auto begin = _partitions.lower_bound(dht::ring_position_view::for_range_start(range), cmp);
    auto end = _partitions.lower_bound(dht::ring_position_view::for_range_end(range), cmp);
    with_allocator(_tracker.allocator(), [this, begin, end] {
        auto it = _partitions.erase_and_dispose(begin, end, [this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_partition_erase();
            p->evict(_tracker);
            deleter(p);
        });
        assert(it != _partitions.end());
        _tracker.clear_continuity(*it);
    });
}

row_cache::row_cache(schema_ptr s, snapshot_source src, cache_tracker& tracker, is_continuous cont)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(cache_entry::compare(_schema))
    , _underlying(src())
    , _snapshot_source(std::move(src))
{
    with_allocator(_tracker.allocator(), [this, cont] {
        cache_entry* entry = current_allocator().construct<cache_entry>(cache_entry::dummy_entry_tag());
        _partitions.insert_before(_partitions.end(), *entry);
        entry->set_continuous(bool(cont));
    });
}

cache_entry::cache_entry(cache_entry&& o) noexcept
    : _schema(std::move(o._schema))
    , _key(std::move(o._key))
    , _pe(std::move(o._pe))
    , _flags(o._flags)
    , _cache_link()
{
    {
        using container_type = row_cache::partitions_type;
        container_type::node_algorithms::replace_node(o._cache_link.this_ptr(), _cache_link.this_ptr());
        container_type::node_algorithms::init(o._cache_link.this_ptr());
    }
}

cache_entry::~cache_entry() {
}

void cache_entry::evict(cache_tracker& tracker) noexcept {
    _pe.evict(tracker.cleaner());
}

void row_cache::set_schema(schema_ptr new_schema) noexcept {
    _schema = std::move(new_schema);
}

void cache_entry::on_evicted(cache_tracker& tracker) noexcept {
    auto it = row_cache::partitions_type::s_iterator_to(*this);
    std::next(it)->set_continuous(false);
    evict(tracker);
    current_deleter<cache_entry>()(this);
    tracker.on_partition_eviction();
}

void rows_entry::on_evicted(cache_tracker& tracker) noexcept {
    auto it = mutation_partition::rows_type::iterator_to(*this);
    if (is_last_dummy()) {
        // Every evictable partition entry must have a dummy entry at the end,
        // so don't remove it, just unlink from the LRU.
        // That dummy is linked in the LRU, because there may be partitions
        // with no regular rows, and we need to track them.
        _lru_link.unlink();
    } else {
        ++it;
        it->set_continuous(false);
        current_deleter<rows_entry>()(this);
        tracker.on_row_eviction();
    }

    if (mutation_partition::rows_type::is_only_member(*it)) {
        assert(it->is_last_dummy());
        partition_version& pv = partition_version::container_of(mutation_partition::container_of(
            mutation_partition::rows_type::container_of_only_member(*it)));
        if (pv.is_referenced_from_entry()) {
            cache_entry& ce = cache_entry::container_of(partition_entry::container_of(pv));
            ce.on_evicted(tracker);
        }
    }
}

flat_mutation_reader cache_entry::read(row_cache& rc, read_context& reader) {
    auto source_and_phase = rc.snapshot_of(_key);
    reader.enter_partition(_key, source_and_phase.snapshot, source_and_phase.phase);
    return do_read(rc, reader);
}

flat_mutation_reader cache_entry::read(row_cache& rc, read_context& reader, row_cache::phase_type phase) {
    reader.enter_partition(_key, phase);
    return do_read(rc, reader);
}

// Assumes reader is in the corresponding partition
flat_mutation_reader cache_entry::do_read(row_cache& rc, read_context& reader) {
    auto snp = _pe.read(rc._tracker.region(), rc._tracker.cleaner(), _schema, &rc._tracker, reader.phase());
    auto ckr = query::clustering_key_filter_ranges::get_ranges(*_schema, reader.slice(), _key.key());
    auto r = make_cache_flat_mutation_reader(_schema, _key, std::move(ckr), rc, reader.shared_from_this(), std::move(snp));
    if (reader.schema()->version() != _schema->version()) {
        r = transform(std::move(r), schema_upgrader(reader.schema()));
    }
    if (reader.fwd() == streamed_mutation::forwarding::yes) {
        r = make_forwardable(std::move(r));
    }
    return std::move(r);
}

const schema_ptr& row_cache::schema() const {
    return _schema;
}

void row_cache::upgrade_entry(cache_entry& e) {
    if (e._schema != _schema) {
        auto& r = _tracker.region();
        assert(!r.reclaiming_enabled());
        with_allocator(r.allocator(), [this, &e] {
          with_linearized_managed_bytes([&] {
            e.partition().upgrade(e._schema, _schema, _tracker.cleaner(), &_tracker);
            e._schema = _schema;
          });
        });
    }
}

std::ostream& operator<<(std::ostream& out, row_cache& rc) {
    rc._read_section(rc._tracker.region(), [&] {
        out << "{row_cache: " << ::join(", ", rc._partitions.begin(), rc._partitions.end()) << "}";
    });
    return out;
}

future<> row_cache::do_update(row_cache::external_updater eu, row_cache::internal_updater iu) noexcept {
    return futurize_apply([this] {
        return get_units(_update_sem, 1);
    }).then([this, eu = std::move(eu), iu = std::move(iu)] (auto permit) mutable {
        auto pos = dht::ring_position::min();
        eu();
        [&] () noexcept {
            _prev_snapshot_pos = std::move(pos);
            _prev_snapshot = std::exchange(_underlying, _snapshot_source());
            ++_underlying_phase;
        }();
        return futurize_apply([&iu] {
            return iu();
        }).then_wrapped([this, permit = std::move(permit)] (auto f) {
            _prev_snapshot_pos = {};
            _prev_snapshot = {};
            if (f.failed()) {
                clogger.warn("Failure during cache update: {}", f.get_exception());
            }
        });
    });
}

std::ostream& operator<<(std::ostream& out, cache_entry& e) {
    return out << "{cache_entry: " << e.position()
               << ", cont=" << e.continuous()
               << ", dummy=" << e.is_dummy_entry()
               << ", " << e.partition()
               << "}";
}
