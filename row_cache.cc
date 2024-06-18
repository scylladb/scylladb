/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "row_cache.hh"
#include <fmt/ranges.h>
#include <seastar/core/memory.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include "replica/memtable.hh"
#include <boost/version.hpp>
#include <sys/sdt.h>
#include "read_context.hh"
#include "real_dirty_memory_accounter.hh"
#include "readers/delegating_v2.hh"
#include "readers/forwardable_v2.hh"
#include "readers/nonforwardable.hh"
#include "cache_mutation_reader.hh"
#include "partition_snapshot_reader.hh"
#include "clustering_key_filter.hh"
#include "utils/assert.hh"
#include "utils/updateable_value.hh"

namespace cache {

logging::logger clogger("cache");

}

using namespace std::chrono_literals;
using namespace cache;

static schema_ptr to_query_domain(const query::partition_slice& slice, schema_ptr table_domain_schema) {
    if (slice.is_reversed()) [[unlikely]] {
        return table_domain_schema->make_reversed();
    }
    return table_domain_schema;
}

mutation_reader
row_cache::create_underlying_reader(read_context& ctx, mutation_source& src, const dht::partition_range& pr) {
    schema_ptr entry_schema = to_query_domain(ctx.slice(), _schema);
    auto reader = src.make_reader_v2(entry_schema, ctx.permit(), pr, ctx.slice(), ctx.trace_state(), streamed_mutation::forwarding::yes);
    ctx.on_underlying_created();
    return reader;
}

static thread_local mutation_application_stats dummy_app_stats;
static thread_local utils::updateable_value<double> dummy_index_cache_fraction(1.0);

cache_tracker::cache_tracker()
    : cache_tracker(dummy_index_cache_fraction, dummy_app_stats, register_metrics::no)
{}

cache_tracker::cache_tracker(utils::updateable_value<double> index_cache_fraction, register_metrics with_metrics)
    : cache_tracker(std::move(index_cache_fraction), dummy_app_stats, with_metrics)
{}

static thread_local cache_tracker* current_tracker;

cache_tracker::cache_tracker(utils::updateable_value<double> index_cache_fraction, mutation_application_stats& app_stats, register_metrics with_metrics)
    : _garbage(_region, this, app_stats)
    , _memtable_cleaner(_region, nullptr, app_stats)
    , _app_stats(app_stats)
    , _index_cache_fraction(std::move(index_cache_fraction))
{
    if (with_metrics) {
        setup_metrics();
    }

    _region.make_evictable([this] {
        return with_allocator(_region.allocator(), [this] () noexcept {
            if (!_garbage.empty()) {
                _garbage.clear_some();
                return memory::reclaiming_result::reclaimed_something;
            }
            if (!_memtable_cleaner.empty()) {
                _memtable_cleaner.clear_some();
                return memory::reclaiming_result::reclaimed_something;
            }
            current_tracker = this;

            // Cache replacement algorithm:
            //
            // if sstable index caches occupy more than index_cache_fraction of cache memory:
            //     evict the least recently used index entry
            // else:
            //     evict the least recently used entry (data or index)
            //
            // This algorithm has the following good properties:
            // 1. When index and data entries contend for cache space, it prevents
            //    index cache from taking more than index_cache_fraction of memory.
            //    This makes sure that the index cache doesn't catastrophically
            //    deprive the data cache of memory in small-partition workloads.
            // 2. Since it doesn't enforce a lower limit on the index space, but only
            //    the upper limit, the parameter shouldn't require careful balancing.
            //    In workloads where it makes sense to cache the index (usually: where
            //    the index is small enough to fit in RAM), setting the fraction to any big number (e.g. 1.0)
            //    should work well enough. In workloads where it doesn't make sense to cache the index,
            //    setting the fraction to any small number (e.g. 0.0) should work well enough.
            //    Setting it to a medium number (something like 0.2) should work well enough
            //    for both extremes, although it might be suboptimal for non-extremes.
            // 3. The parameter is trivially live-updateable.
            //
            // Perhaps this logic should be encapsulated somewhere else, maybe in `class lru` itself.
            size_t total_cache_space = _region.occupancy().total_space();
            size_t index_cache_space = _partition_index_cache_stats.used_bytes + _index_cached_file_stats.cached_bytes;
            bool should_evict_index = index_cache_space > total_cache_space * _index_cache_fraction.get();

            return _lru.evict(should_evict_index);
        });
    });
}

cache_tracker::~cache_tracker() {
    clear();
}

memory::reclaiming_result cache_tracker::evict_from_lru_shallow() noexcept {
    return with_allocator(_region.allocator(), [this] () noexcept {
        current_tracker = this;
        return _lru.evict_shallow();
    });
}

void cache_tracker::set_compaction_scheduling_group(seastar::scheduling_group sg) {
    _memtable_cleaner.set_scheduling_group(sg);
    _garbage.set_scheduling_group(sg);
}

namespace sstables {
void register_index_page_cache_metrics(seastar::metrics::metric_groups&, cached_file_stats&);
void register_index_page_metrics(seastar::metrics::metric_groups&, partition_index_cache_stats&);
};

void
cache_tracker::setup_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("cache", {
        sm::make_gauge("bytes_used", sm::description("current bytes used by the cache out of the total size of memory"), [this] { return _region.occupancy().used_space(); }),
        sm::make_gauge("bytes_total", sm::description("total size of memory for the cache"), [this] { return _region.occupancy().total_space(); }),
        sm::make_counter("partition_hits", sm::description("number of partitions needed by reads and found in cache"), _stats.partition_hits),
        sm::make_counter("partition_misses", sm::description("number of partitions needed by reads and missing in cache"), _stats.partition_misses),
        sm::make_counter("partition_insertions", sm::description("total number of partitions added to cache"), _stats.partition_insertions),
        sm::make_counter("row_hits", sm::description("total number of rows needed by reads and found in cache"), _stats.row_hits),
        sm::make_counter("dummy_row_hits", sm::description("total number of dummy rows touched by reads in cache"), _stats.dummy_row_hits),
        sm::make_counter("row_misses", sm::description("total number of rows needed by reads and missing in cache"), _stats.row_misses),
        sm::make_counter("row_insertions", sm::description("total number of rows added to cache"), _stats.row_insertions),
        sm::make_counter("row_evictions", sm::description("total number of rows evicted from cache"), _stats.row_evictions),
        sm::make_counter("row_removals", sm::description("total number of invalidated rows"), _stats.row_removals),
        sm::make_counter("rows_dropped_by_tombstones", _app_stats.rows_dropped_by_tombstones, sm::description("Number of rows dropped in cache by a tombstone write")),
        sm::make_counter("rows_compacted_with_tombstones", _app_stats.rows_compacted_with_tombstones, sm::description("Number of rows scanned during write of a tombstone for the purpose of compaction in cache")),
        sm::make_counter("static_row_insertions", sm::description("total number of static rows added to cache"), _stats.static_row_insertions),
        sm::make_counter("concurrent_misses_same_key", sm::description("total number of operation with misses same key"), _stats.concurrent_misses_same_key),
        sm::make_counter("partition_merges", sm::description("total number of partitions merged"), _stats.partition_merges),
        sm::make_counter("partition_evictions", sm::description("total number of evicted partitions"), _stats.partition_evictions),
        sm::make_counter("partition_removals", sm::description("total number of invalidated partitions"), _stats.partition_removals),
        sm::make_counter("mispopulations", sm::description("number of entries not inserted by reads"), _stats.mispopulations),
        sm::make_gauge("partitions", sm::description("total number of cached partitions"), _stats.partitions),
        sm::make_gauge("rows", sm::description("total number of cached rows"), _stats.rows),
        sm::make_counter("reads", sm::description("number of started reads"), _stats.reads),
        sm::make_counter("reads_with_misses", sm::description("number of reads which had to read from sstables"), _stats.reads_with_misses),
        sm::make_gauge("active_reads", sm::description("number of currently active reads"), [this] { return _stats.active_reads(); }),
        sm::make_counter("sstable_reader_recreations", sm::description("number of times sstable reader was recreated due to memtable flush"), _stats.underlying_recreations),
        sm::make_counter("sstable_partition_skips", sm::description("number of times sstable reader was fast forwarded across partitions"), _stats.underlying_partition_skips),
        sm::make_counter("sstable_row_skips", sm::description("number of times sstable reader was fast forwarded within a partition"), _stats.underlying_row_skips),
        sm::make_counter("pinned_dirty_memory_overload", sm::description("amount of pinned bytes that we tried to unpin over the limit. This should sit constantly at 0, and any number different than 0 is indicative of a bug"), _stats.pinned_dirty_memory_overload),
        sm::make_counter("rows_processed_from_memtable", _stats.rows_processed_from_memtable,
            sm::description("total number of rows in memtables which were processed during cache update on memtable flush")),
        sm::make_counter("rows_dropped_from_memtable", _stats.rows_dropped_from_memtable,
            sm::description("total number of rows in memtables which were dropped during cache update on memtable flush")),
        sm::make_counter("rows_merged_from_memtable", _stats.rows_merged_from_memtable,
            sm::description("total number of rows in memtables which were merged with existing rows during cache update on memtable flush")),
        sm::make_counter("range_tombstone_reads", _stats.range_tombstone_reads,
            sm::description("total amount of range tombstones processed during read")),
        sm::make_counter("row_tombstone_reads", _stats.row_tombstone_reads,
            sm::description("total amount of row tombstones processed during read")),
        sm::make_counter("rows_compacted", _stats.rows_compacted,
            sm::description("total amount of attempts to compact expired rows during read")),
        sm::make_counter("rows_compacted_away", _stats.rows_compacted_away,
            sm::description("total amount of compacted and removed rows during read")),
    });
    sstables::register_index_page_cache_metrics(_metrics, _index_cached_file_stats);
    sstables::register_index_page_metrics(_metrics, _partition_index_cache_stats);
}

void cache_tracker::clear() {
    auto partitions_before = _stats.partitions;
    auto rows_before = _stats.rows;
    // We need to clear garbage first because garbage versions cannot be evicted from,
    // mutation_partition::clear_gently() destroys intrusive tree invariants.
    with_allocator(_region.allocator(), [this] {
        _garbage.clear();
        _memtable_cleaner.clear();
        current_tracker = this;
        _lru.evict_all();
        // Eviction could have produced garbage.
        _garbage.clear();
        _memtable_cleaner.clear();
    });
    _stats.partition_removals += partitions_before;
    _stats.row_removals += rows_before;
    allocator().invalidate_references();
}

void cache_tracker::touch(rows_entry& e) {
    // last dummy may not be linked if evicted
    if (e.is_linked()) {
        _lru.remove(e);
    }
    _lru.add(e);
}

void cache_tracker::insert(cache_entry& entry) {
    insert(entry.partition());
    ++_stats.partition_insertions;
    ++_stats.partitions;
    // partition_range_cursor depends on this to detect invalidation of _end
    _region.allocator().invalidate_references();
}

void cache_tracker::on_partition_erase() noexcept {
    --_stats.partitions;
    ++_stats.partition_removals;
    allocator().invalidate_references();
}

void cache_tracker::on_partition_merge() noexcept {
    ++_stats.partition_merges;
}

void cache_tracker::on_partition_hit() noexcept {
    ++_stats.partition_hits;
}

void cache_tracker::on_partition_miss() noexcept {
    ++_stats.partition_misses;
}

void cache_tracker::on_partition_eviction() noexcept {
    --_stats.partitions;
    ++_stats.partition_evictions;
}

void cache_tracker::on_row_eviction() noexcept {
    --_stats.rows;
    ++_stats.row_evictions;
}

void cache_tracker::on_row_hit() noexcept {
    ++_stats.row_hits;
}

void cache_tracker::on_dummy_row_hit() noexcept {
    ++_stats.dummy_row_hits;
}

void cache_tracker::on_row_miss() noexcept {
    ++_stats.row_misses;
}

void cache_tracker::on_mispopulate() noexcept {
    ++_stats.mispopulations;
}

void cache_tracker::on_miss_already_populated() noexcept {
    ++_stats.concurrent_misses_same_key;
}

void cache_tracker::pinned_dirty_memory_overload(uint64_t bytes) noexcept {
    _stats.pinned_dirty_memory_overload += bytes;
}

allocation_strategy& cache_tracker::allocator() noexcept {
    return _region.allocator();
}

logalloc::region& cache_tracker::region() noexcept {
    return _region;
}

const logalloc::region& cache_tracker::region() const noexcept {
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
    std::optional<dht::decorated_key> _last;
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
        dht::ring_position_comparator cmp(*_cache.get()._schema);
        if (cmp(_end_pos, pos) < 0) { // next() may have moved _start_pos past the _end_pos.
            _end_pos = pos;
        }
        _end = _cache.get()._partitions.lower_bound(_end_pos, cmp);
        _it = _cache.get()._partitions.lower_bound(pos, cmp);
        auto same = cmp(pos, _it->position()) >= 0;
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

future<> read_context::create_underlying() {
    if (_range_query) {
        // FIXME: Singular-range mutation readers don't support fast_forward_to(), so need to use a wide range
        // here in case the same reader will need to be fast forwarded later.
        _sm_range = dht::partition_range({dht::ring_position(*_key)}, {dht::ring_position(*_key)});
    } else {
        _sm_range = dht::partition_range::make_singular({dht::ring_position(*_key)});
    }
    return _underlying.fast_forward_to(std::move(_sm_range), *_underlying_snapshot, _phase).then([this] {
        _underlying_snapshot = {};
    });
}

static mutation_reader read_directly_from_underlying(read_context& reader, mutation_fragment_v2 partition_start) {
    auto res = make_delegating_reader(reader.underlying().underlying());
    res.unpop_mutation_fragment(std::move(partition_start));
    res.upgrade_schema(reader.schema());
    return make_nonforwardable(std::move(res), true);
}

// Reader which populates the cache using data from the delegate.
class single_partition_populating_reader final : public mutation_reader::impl {
    row_cache& _cache;
    std::unique_ptr<read_context> _read_context;
    mutation_reader_opt _reader;
private:
    future<> create_reader() {
        auto src_and_phase = _cache.snapshot_of(_read_context->range().start()->value());
        auto phase = src_and_phase.phase;
        _read_context->enter_partition(_read_context->range().start()->value().as_decorated_key(), src_and_phase.snapshot, phase);
        return _read_context->create_underlying().then([this, phase] {
          return _read_context->underlying().underlying()().then([this, phase] (auto&& mfopt) {
            if (!mfopt) {
                if (phase == _cache.phase_of(_read_context->range().start()->value())) {
                    _cache._read_section(_cache._tracker.region(), [this] {
                        _cache.find_or_create_missing(_read_context->key());
                    });
                } else {
                    _cache._tracker.on_mispopulate();
                }
                _end_of_stream = true;
            } else if (phase == _cache.phase_of(_read_context->range().start()->value())) {
                _reader = _cache._read_section(_cache._tracker.region(), [&] {
                    cache_entry& e = _cache.find_or_create_incomplete(mfopt->as_partition_start(), phase);
                    return e.read(_cache, *_read_context, phase);
                });
            } else {
                _cache._tracker.on_mispopulate();
                _reader = read_directly_from_underlying(*_read_context, std::move(*mfopt));
            }
          });
        });
    }
public:
    single_partition_populating_reader(row_cache& cache,
            std::unique_ptr<read_context> context)
        : impl(context->schema(), context->permit())
        , _cache(cache)
        , _read_context(std::move(context))
    { }

    virtual future<> fill_buffer() override {
        if (!_reader) {
            return create_reader().then([this] {
                if (_end_of_stream) {
                    return make_ready_future<>();
                }
                return fill_buffer();
            });
        }
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return fill_buffer_from(*_reader).then([this] (bool reader_finished) {
                if (reader_finished) {
                    _end_of_stream = true;
                }
            });
        });
    }
    virtual future<> next_partition() override {
        if (_reader) {
            clear_buffer();
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
        auto close_reader = _reader ? _reader->close() : make_ready_future<>();
        auto close_read_context = _read_context->close();
        return when_all_succeed(std::move(close_reader), std::move(close_read_context)).discard_result();
    }
};

void cache_tracker::clear_continuity(cache_entry& ce) noexcept {
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
    std::optional<row_cache::previous_entry_pointer> _last_key;
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
            dht::ring_position_comparator cmp(*_cache._schema);
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

    future<mutation_reader_opt> operator()() {
        return _reader.move_to_next_partition().then([this] (auto&& mfopt) mutable {
            {
                if (!mfopt) {
                    return _cache._read_section(_cache._tracker.region(), [&] {
                        this->handle_end_of_stream();
                        return make_ready_future<mutation_reader_opt>(std::nullopt);
                    });
                }
                _cache.on_partition_miss();
                const partition_start& ps = mfopt->as_partition_start();
                const dht::decorated_key& key = ps.key();
                if (_reader.creation_phase() == _cache.phase_of(key)) {
                    return _cache._read_section(_cache._tracker.region(), [&] {
                        cache_entry& e = _cache.find_or_create_incomplete(ps, _reader.creation_phase(),
                                                               this->can_set_continuity() ? &*_last_key : nullptr);
                        _last_key = row_cache::previous_entry_pointer(key);
                        return make_ready_future<mutation_reader_opt>(e.read(_cache, _read_context, _reader.creation_phase()));
                    });
                } else {
                    _cache._tracker.on_mispopulate();
                    _last_key = row_cache::previous_entry_pointer(key);
                    return make_ready_future<mutation_reader_opt>(read_directly_from_underlying(_read_context, std::move(*mfopt)));
                }
            }
        });
    }

    future<> fast_forward_to(dht::partition_range&& pr) {
        if (!pr.start()) {
            _last_key = row_cache::previous_entry_pointer();
        } else if (!pr.start()->is_inclusive() && pr.start()->value().has_key()) {
            _last_key = row_cache::previous_entry_pointer(pr.start()->value().as_decorated_key());
        } else {
            // Inclusive start bound, cannot set continuity flag.
            _last_key = {};
        }

        return _reader.fast_forward_to(std::move(pr));
    }
    future<> close() noexcept {
        return _reader.close();
    }
};

class scanning_and_populating_reader final : public mutation_reader::impl {
    const dht::partition_range* _pr;
    row_cache& _cache;
    std::unique_ptr<read_context> _read_context;
    partition_range_cursor _primary;
    range_populating_reader _secondary_reader;
    bool _read_next_partition = false;
    bool _secondary_in_progress = false;
    bool _advance_primary = false;
    std::optional<dht::partition_range::bound> _lower_bound;
    dht::partition_range _secondary_range;
    mutation_reader_opt _reader;
private:
    mutation_reader read_from_entry(cache_entry& ce) {
        _cache.upgrade_entry(ce);
        _cache.on_partition_hit();
        return ce.read(_cache, *_read_context);
    }

    static dht::ring_position_view as_ring_position_view(const std::optional<dht::partition_range::bound>& lower_bound) {
        return lower_bound ? dht::ring_position_view(lower_bound->value(), dht::ring_position_view::after_key(!lower_bound->is_inclusive()))
                           : dht::ring_position_view::min();
    }

    mutation_reader_opt do_read_from_primary() {
        return _cache._read_section(_cache._tracker.region(), [this] () -> mutation_reader_opt {
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
                    return std::nullopt;
                }
                cache_entry& e = _primary.entry();
                auto fr = read_from_entry(e);
                _lower_bound = dht::partition_range::bound{e.key(), false};
                // Delay the call to next() so that we don't see stale continuity on next invocation.
                _advance_primary = true;
                return mutation_reader_opt(std::move(fr));
            } else {
                if (_primary.in_range()) {
                    cache_entry& e = _primary.entry();
                    _secondary_range = dht::partition_range(_lower_bound,
                        dht::partition_range::bound{e.key(), false});
                    _lower_bound = dht::partition_range::bound{e.key(), true};
                    _secondary_in_progress = true;
                    return std::nullopt;
                } else {
                    dht::ring_position_comparator cmp(*_read_context->schema());
                    auto range = _pr->trim_front(std::optional<dht::partition_range::bound>(_lower_bound), cmp);
                    if (!range) {
                        return std::nullopt;
                    }
                    _lower_bound = dht::partition_range::bound{dht::ring_position::max()};
                    _secondary_range = std::move(*range);
                    _secondary_in_progress = true;
                    return std::nullopt;
                }
            }
        });
    }

    future<mutation_reader_opt> read_from_primary() {
        auto reader = do_read_from_primary();
        if (!_secondary_in_progress) {
            return make_ready_future<mutation_reader_opt>(std::move(reader));
        }
        return _secondary_reader.fast_forward_to(std::move(_secondary_range)).then([this] {
            return read_from_secondary();
        });
    }

    future<mutation_reader_opt> read_from_secondary() {
        return _secondary_reader().then([this] (mutation_reader_opt&& fropt) {
            if (fropt) {
                return make_ready_future<mutation_reader_opt>(std::move(fropt));
            } else {
                _secondary_in_progress = false;
                return read_from_primary();
            }
        });
    }
    future<> read_next_partition() {
      auto close_reader = _reader ? _reader->close() : make_ready_future<>();
      return close_reader.then([this] {
        _read_next_partition = false;
        return (_secondary_in_progress ? read_from_secondary() : read_from_primary()).then([this] (auto&& fropt) {
            if (bool(fropt)) {
                _reader = std::move(fropt);
            } else {
                _end_of_stream = true;
            }
        });
      });
    }
public:
    scanning_and_populating_reader(row_cache& cache,
                                   const dht::partition_range& range,
                                   std::unique_ptr<read_context> context)
        : impl(context->schema(), context->permit())
        , _pr(&range)
        , _cache(cache)
        , _read_context(std::move(context))
        , _primary(cache, range)
        , _secondary_reader(cache, *_read_context)
        , _lower_bound(range.start())
    { }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            if (!_reader || _read_next_partition) {
                return read_next_partition();
            } else {
                return fill_buffer_from(*_reader).then([this] (bool reader_finished) {
                    if (reader_finished) {
                        _read_next_partition = true;
                    }
                });
            }
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (_reader && is_buffer_empty()) {
            return _reader->next_partition();
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        _secondary_in_progress = false;
        _advance_primary = false;
        _pr = &pr;
        _primary = partition_range_cursor{_cache, pr};
        _lower_bound = pr.start();
        return _reader->close();
    }
    virtual future<> fast_forward_to(position_range cr) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        auto close_reader = _reader ? _reader->close() : make_ready_future<>();
        auto close_secondary_reader = _secondary_reader.close();
        auto close_read_context = _read_context->close();
        return when_all_succeed(std::move(close_reader), std::move(close_secondary_reader), std::move(close_read_context)).discard_result();
    }
};

mutation_reader
row_cache::make_scanning_reader(const dht::partition_range& range, std::unique_ptr<read_context> context) {
    return make_mutation_reader<scanning_and_populating_reader>(*this, range, std::move(context));
}

mutation_reader_opt
row_cache::make_reader_opt(schema_ptr s,
                       reader_permit permit,
                       const dht::partition_range& range,
                       const query::partition_slice& slice,
                       const tombstone_gc_state* gc_state,
                       tracing::trace_state_ptr trace_state,
                       streamed_mutation::forwarding fwd,
                       mutation_reader::forwarding fwd_mr)
{
    auto make_context = [&] {
        return std::make_unique<read_context>(*this, s, permit, range, slice, gc_state, trace_state, fwd_mr);
    };

    if (query::is_single_partition(range) && !fwd_mr) {
        tracing::trace(trace_state, "Querying cache for range {} and slice {}",
                range, seastar::value_of([&slice] { return slice.get_all_ranges(); }));
        auto mr = _read_section(_tracker.region(), [&] () -> mutation_reader_opt {
            dht::ring_position_comparator cmp(*_schema);
            auto&& pos = range.start()->value();
            partitions_type::bound_hint hint;
            auto i = _partitions.lower_bound(pos, cmp, hint);
            if (hint.match) {
                cache_entry& e = *i;
                upgrade_entry(e);
                on_partition_hit();
                return e.read(*this, make_context());
            } else if (i->continuous()) {
                return {};
            } else {
                tracing::trace(trace_state, "Range {} not found in cache", range);
                on_partition_miss();
                return make_mutation_reader<single_partition_populating_reader>(*this, make_context());
            }
        });

        if (mr && fwd == streamed_mutation::forwarding::yes) {
            return make_forwardable(std::move(*mr));
        } else {
            return mr;
        }
    }

    tracing::trace(trace_state, "Scanning cache for range {} and slice {}",
                   range, seastar::value_of([&slice] { return slice.get_all_ranges(); }));
    auto mr = make_scanning_reader(range, make_context());
    if (fwd == streamed_mutation::forwarding::yes) {
        return make_forwardable(std::move(mr));
    } else {
        return mr;
    }
}

mutation_reader row_cache::make_nonpopulating_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
        const query::partition_slice& slice, tracing::trace_state_ptr ts) {
    if (!range.is_singular()) {
        throw std::runtime_error("row_cache::make_nonpopulating_reader(): only singular ranges are supported");
    }
    struct dummy_accounter {
        void operator()(const clustering_row&) {}
        void operator()(const static_row&) {}
        void operator()(const range_tombstone_change&) {}
        void operator()(const partition_start&) {}
        void operator()(const partition_end&) {}
    };
    return _read_section(_tracker.region(), [&] () -> mutation_reader {
        dht::ring_position_comparator cmp(*_schema);
        auto&& pos = range.start()->value();
        partitions_type::bound_hint hint;
        auto i = _partitions.lower_bound(pos, cmp, hint);
        if (hint.match) {
            cache_entry& e = *i;
            upgrade_entry(e);
            tracing::trace(ts, "Reading partition {} from cache", pos);
            return make_partition_snapshot_flat_reader<false, dummy_accounter>(
                    schema,
                    std::move(permit),
                    e.key(),
                    query::clustering_key_filter_ranges(slice.row_ranges(*schema, e.key().key())),
                    e.partition().read(_tracker.region(), _tracker.memtable_cleaner(), nullptr, phase_of(pos)),
                    false,
                    _tracker.region(),
                    _read_section,
                    {},
                    streamed_mutation::forwarding::no);
        } else {
            tracing::trace(ts, "Partition {} is not found in cache", pos);
            return make_empty_flat_reader_v2(std::move(schema), std::move(permit));
        }
    });
}

void row_cache::clear_on_destruction() noexcept {
    with_allocator(_tracker.allocator(), [this] {
        _partitions.clear_and_dispose([this] (cache_entry* p) mutable noexcept {
            if (!p->is_dummy_entry()) {
                _tracker.on_partition_erase();
            }
            p->evict(_tracker);
        });
    });
}

row_cache::~row_cache() {
    clear_on_destruction();
}

void row_cache::clear_now() noexcept {
    with_allocator(_tracker.allocator(), [this] {
        auto it = _partitions.erase_and_dispose(_partitions.begin(), partitions_end(), [this] (cache_entry* p) noexcept {
            _tracker.on_partition_erase();
            p->evict(_tracker);
        });
        _tracker.clear_continuity(*it);
    });
}

template<typename CreateEntry, typename VisitEntry>
requires requires(CreateEntry create, VisitEntry visit, row_cache::partitions_type::iterator it, row_cache::partitions_type::bound_hint hint) {
    { create(it, hint) } -> std::same_as<row_cache::partitions_type::iterator>;
    { visit(it) } -> std::same_as<void>;
}
cache_entry& row_cache::do_find_or_create_entry(const dht::decorated_key& key,
    const previous_entry_pointer* previous, CreateEntry&& create_entry, VisitEntry&& visit_entry)
{
    return with_allocator(_tracker.allocator(), [&] () -> cache_entry& {
        partitions_type::bound_hint hint;
        dht::ring_position_comparator cmp(*_schema);
        auto i = _partitions.lower_bound(key, cmp, hint);
        if (i == _partitions.end() || !hint.match) {
            i = create_entry(i, hint);
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
}

cache_entry& row_cache::find_or_create_incomplete(const partition_start& ps, row_cache::phase_type phase, const previous_entry_pointer* previous) {
    return do_find_or_create_entry(ps.key(), previous, [&] (auto i, const partitions_type::bound_hint& hint) { // create
        // Create an fully discontinuous, except for the partition tombstone, entry
        mutation_partition mp = mutation_partition::make_incomplete(*_schema, ps.partition_tombstone());
        partitions_type::iterator entry = _partitions.emplace_before(i, ps.key().token().raw(), hint,
                _schema, ps.key(), std::move(mp));
        _tracker.insert(*entry);
        return entry;
    }, [&] (auto i) { // visit
        _tracker.on_miss_already_populated();
        cache_entry& e = *i;
        e.partition().open_version(*e.schema(), &_tracker, phase).partition().apply(ps.partition_tombstone());
        upgrade_entry(e);
    });
}

cache_entry& row_cache::find_or_create_missing(const dht::decorated_key& key) {
    return do_find_or_create_entry(key, nullptr, [&] (auto i, const partitions_type::bound_hint& hint) {
        mutation_partition mp(*_schema);
        bool cont = i->continuous();
        partitions_type::iterator entry = _partitions.emplace_before(i, key.token().raw(), hint,
                _schema, key, std::move(mp));
        _tracker.insert(*entry);
        entry->set_continuous(cont);
        return entry;
    }, [&] (auto i) {
        _tracker.on_miss_already_populated();
    });
}

void row_cache::populate(const mutation& m, const previous_entry_pointer* previous) {
  _populate_section(_tracker.region(), [&] {
    do_find_or_create_entry(m.decorated_key(), previous, [&] (auto i, const partitions_type::bound_hint& hint) {
        partitions_type::iterator entry = _partitions.emplace_before(i, m.decorated_key().token().raw(), hint,
                m.schema(), m.decorated_key(), m.partition());
        _tracker.insert(*entry);
        entry->set_continuous(i->continuous());
        upgrade_entry(*entry);
        return entry;
    }, [&] (auto i) {
        throw std::runtime_error(format("cache already contains entry for {}", m.key()));
    });
  });
}

cache_entry& row_cache::lookup(const dht::decorated_key& key) {
    return do_find_or_create_entry(key, nullptr, [&] (auto i, const partitions_type::bound_hint& hint) {
        throw std::runtime_error(format("cache doesn't contain entry for {}", key));
        return i;
    }, [&] (auto i) {
        _tracker.on_miss_already_populated();
        upgrade_entry(*i);
    });
}

mutation_source& row_cache::snapshot_for_phase(phase_type phase) {
    if (phase == _underlying_phase) {
        return _underlying;
    } else {
        if (phase + 1 < _underlying_phase) {
            throw std::runtime_error(format("attempted to read from retired phase {} (current={})", phase, _underlying_phase));
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

void row_cache::invalidate_sync(replica::memtable& m) noexcept {
    with_allocator(_tracker.allocator(), [&m, this] () {
        logalloc::reclaim_lock _(_tracker.region());
        bool blow_cache = false;
        m.partitions.clear_and_dispose([this, &m, &blow_cache] (replica::memtable_entry* entry) noexcept {
            try {
                invalidate_locked(entry->key());
            } catch (...) {
                blow_cache = true;
            }
            m.evict_entry(*entry, _tracker.memtable_cleaner());
        });
        if (blow_cache) {
            // We failed to invalidate the key. Recover using clear_now(), which doesn't throw.
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

thread_local preemption_source row_cache::default_preemption_source;

template <typename Updater>
future<> row_cache::do_update(external_updater eu, replica::memtable& m, Updater updater, preemption_source& preempt_src) {
  return do_update(std::move(eu), [this, &m, &preempt_src, updater = std::move(updater)] {
    real_dirty_memory_accounter real_dirty_acc(m, _tracker);
    m.on_detach_from_region_group();
    _tracker.region().merge(m); // Now all data in memtable belongs to cache
    _tracker.memtable_cleaner().merge(m._cleaner);
    STAP_PROBE(scylla, row_cache_update_start);
    auto cleanup = defer([&m, this] () noexcept {
        invalidate_sync(m);
        STAP_PROBE(scylla, row_cache_update_end);
    });

    return seastar::async([this, &m, &preempt_src, updater = std::move(updater), real_dirty_acc = std::move(real_dirty_acc)] () mutable {
        size_t size_entry;
        // In case updater fails, we must bring the cache to consistency without deferring.
        auto cleanup = defer([&m, this] () noexcept {
            invalidate_sync(m);
            _prev_snapshot_pos = {};
            _prev_snapshot = {};
        });
        utils::coroutine update; // Destroy before cleanup to release snapshots before invalidating.
        auto destroy_update = defer([&] {
            with_allocator(_tracker.allocator(), [&] {
                update = {};
            });
        });
        partition_presence_checker is_present = _prev_snapshot->make_partition_presence_checker();
        while (!m.partitions.empty()) {
            with_allocator(_tracker.allocator(), [&] () {
                auto cmp = dht::ring_position_comparator(*_schema);
                {
                    size_t partition_count = 0;
                    {
                        STAP_PROBE(scylla, row_cache_update_one_batch_start);
                        do {
                          STAP_PROBE(scylla, row_cache_update_partition_start);
                          {
                            if (!update) {
                                _update_section(_tracker.region(), [&] {
                                    replica::memtable_entry& mem_e = *m.partitions.begin();
                                    size_entry = mem_e.size_in_allocator_without_rows(_tracker.allocator());
                                    partitions_type::bound_hint hint;
                                    auto cache_i = _partitions.lower_bound(mem_e.key(), cmp, hint);
                                    update = updater(_update_section, cache_i, mem_e, is_present, real_dirty_acc, hint, preempt_src);
                                });
                            }
                            // We use cooperative deferring instead of futures so that
                            // this layer has a chance to restore invariants before deferring,
                            // in particular set _prev_snapshot_pos to the correct value.
                            if (update.run() == stop_iteration::no) {
                                break;
                            }
                            update = {};
                            real_dirty_acc.unpin_memory(size_entry);
                            _update_section(_tracker.region(), [&] {
                                auto i = m.partitions.begin();
                                i.erase_and_dispose(dht::raw_token_less_comparator{}, [&] (replica::memtable_entry* e) noexcept {
                                    m.evict_entry(*e, _tracker.memtable_cleaner());
                                });
                            });
                            ++partition_count;
                          }
                          STAP_PROBE(scylla, row_cache_update_partition_end);
                        } while (!m.partitions.empty() && !preempt_src.should_preempt());
                        with_allocator(standard_allocator(), [&] {
                            if (m.partitions.empty()) {
                                _prev_snapshot_pos = {};
                            } else {
                                _update_section(_tracker.region(), [&] {
                                    _prev_snapshot_pos = m.partitions.begin()->key();
                                });
                            }
                        });
                        STAP_PROBE1(scylla, row_cache_update_one_batch_end, partition_count);
                    }
                }
            });
            real_dirty_acc.commit();
            preempt_src.thread_yield();
        }
    }).finally([cleanup = std::move(cleanup)] {});
  });
}

future<> row_cache::update(external_updater eu, replica::memtable& m, preemption_source& preempt_src) {
    return do_update(std::move(eu), m, [this] (logalloc::allocating_section& alloc,
            row_cache::partitions_type::iterator cache_i, replica::memtable_entry& mem_e, partition_presence_checker& is_present,
            real_dirty_memory_accounter& acc, const partitions_type::bound_hint& hint, preemption_source& preempt_src) mutable {
        // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
        // FIXME: keep a bitmap indicating which sstables we do cover, so we don't have to
        //        search it.
        if (cache_i != partitions_end() && hint.match) {
            cache_entry& entry = *cache_i;
            upgrade_entry(entry);
            SCYLLA_ASSERT(entry.schema() == _schema);
            _tracker.on_partition_merge();
            mem_e.upgrade_schema(_tracker.region(), _schema, _tracker.memtable_cleaner());
            return entry.partition().apply_to_incomplete(*_schema, std::move(mem_e.partition()), _tracker.memtable_cleaner(),
                alloc, _tracker.region(), _tracker, _underlying_phase, acc, preempt_src);
        } else if (cache_i->continuous()
                   || with_allocator(standard_allocator(), [&] { return is_present(mem_e.key()); })
                      == partition_presence_checker_result::definitely_doesnt_exist) {
            // Partition is absent in underlying. First, insert a neutral partition entry.
            partitions_type::iterator entry = _partitions.emplace_before(cache_i, mem_e.key().token().raw(), hint,
                cache_entry::evictable_tag(), _schema, dht::decorated_key(mem_e.key()),
                partition_entry::make_evictable(*_schema, mutation_partition(*_schema)));
            entry->set_continuous(cache_i->continuous());
            _tracker.insert(*entry);
            mem_e.upgrade_schema(_tracker.region(), _schema, _tracker.memtable_cleaner());
            return entry->partition().apply_to_incomplete(*_schema, std::move(mem_e.partition()), _tracker.memtable_cleaner(),
                alloc, _tracker.region(), _tracker, _underlying_phase, acc, preempt_src);
        } else {
            return utils::make_empty_coroutine();
        }
    }, preempt_src);
}

future<> row_cache::update_invalidating(external_updater eu, replica::memtable& m) {
    return do_update(std::move(eu), m, [this] (logalloc::allocating_section& alloc,
        row_cache::partitions_type::iterator cache_i, replica::memtable_entry& mem_e, partition_presence_checker& is_present,
        real_dirty_memory_accounter& acc, const partitions_type::bound_hint&, preemption_source&)
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
        return utils::make_empty_coroutine();
    }, default_preemption_source);
}

void row_cache::refresh_snapshot() {
    _underlying = _snapshot_source();
}

void row_cache::touch(const dht::decorated_key& dk) {
 _read_section(_tracker.region(), [&] {
    auto i = _partitions.find(dk, dht::ring_position_comparator(*_schema));
    if (i != _partitions.end()) {
        for (partition_version& pv : i->partition().versions_from_oldest()) {
            for (rows_entry& row : pv.partition().clustered_rows()) {
                _tracker.touch(row);
            }
        }
    }
 });
}

void row_cache::unlink_from_lru(const dht::decorated_key& dk) {
    _read_section(_tracker.region(), [&] {
        auto i = _partitions.find(dk, dht::ring_position_comparator(*_schema));
        if (i != _partitions.end()) {
            for (partition_version& pv : i->partition().versions_from_oldest()) {
                for (rows_entry& row : pv.partition().clustered_rows()) {
                    // Last dummy may already be unlinked.
                    if (row.is_linked()) {
                        _tracker.get_lru().remove(row);
                    }
                }
            }
        }
    });
}

void row_cache::invalidate_locked(const dht::decorated_key& dk) {
    auto pos = _partitions.lower_bound(dk, dht::ring_position_comparator(*_schema));
    if (pos == partitions_end() || !pos->key().equal(*_schema, dk)) {
        _tracker.clear_continuity(*pos);
    } else {
        auto it = pos.erase_and_dispose(dht::raw_token_less_comparator{},
            [this](cache_entry* p) mutable noexcept {
                _tracker.on_partition_erase();
                p->evict(_tracker);
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
        return seastar::async([this, ranges = std::move(ranges)] {
            auto on_failure = defer([this] () noexcept {
                this->clear_now();
                _prev_snapshot_pos = {};
                _prev_snapshot = {};
            });

            for (auto&& range : ranges) {
                _prev_snapshot_pos = dht::ring_position_view::for_range_start(range);
                seastar::thread::maybe_yield();

                while (true) {
                    auto done = _update_section(_tracker.region(), [&] {
                        auto cmp = dht::ring_position_comparator(*_schema);
                        auto it = _partitions.lower_bound(*_prev_snapshot_pos, cmp);
                        auto end = _partitions.lower_bound(dht::ring_position_view::for_range_end(range), cmp);
                        return with_allocator(_tracker.allocator(), [&] {
                            while (it != end) {
                                it = it.erase_and_dispose(dht::raw_token_less_comparator{},
                                    [&] (cache_entry* p) mutable noexcept {
                                        _tracker.on_partition_erase();
                                        p->evict(_tracker);
                                    });
                                // it != end is necessary for correctness. We cannot set _prev_snapshot_pos to end->position()
                                // because after resuming something may be inserted before "end" which falls into the next range.
                                if (need_preempt() && it != end) {
                                    with_allocator(standard_allocator(), [&] {
                                        _prev_snapshot_pos = it->key();
                                    });
                                    break;
                                }
                            }
                            SCYLLA_ASSERT(it != _partitions.end());
                            _tracker.clear_continuity(*it);
                            return stop_iteration(it == end);
                        });
                    });
                    if (done == stop_iteration::yes) {
                        break;
                    }
                    // _prev_snapshot_pos must be updated at this point such that every position < _prev_snapshot_pos
                    // is already invalidated and >= _prev_snapshot_pos is not yet invalidated.
                    seastar::thread::yield();
                }
            }

            on_failure.cancel();
        });
    });
}

void row_cache::evict() {
    while (_tracker.region().evict_some() == memory::reclaiming_result::reclaimed_something) {}
}

row_cache::row_cache(schema_ptr s, snapshot_source src, cache_tracker& tracker, is_continuous cont)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(dht::raw_token_less_comparator{})
    , _underlying(src())
    , _snapshot_source(std::move(src))
{
  try {
    with_allocator(_tracker.allocator(), [this, cont] {
        cache_entry entry(cache_entry::dummy_entry_tag{});
        entry.set_continuous(bool(cont));
        auto raw_token = entry.position().token().raw();
        _partitions.insert(raw_token, std::move(entry), dht::ring_position_comparator{*_schema});
    });
  } catch (...) {
    // The code above might have allocated something in _partitions.
    // The destructor of _partitions will be called with the wrong allocator,
    // so we have to clear _partitions manually here, before it is destroyed.
    clear_on_destruction();
    throw;
  }
}

cache_entry::cache_entry(cache_entry&& o) noexcept
    : _key(std::move(o._key))
    , _pe(std::move(o._pe))
    , _flags(o._flags)
{
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
    row_cache::partitions_type::iterator it(this);
    std::next(it)->set_continuous(false);
    evict(tracker);
    tracker.on_partition_eviction();
    it.erase(dht::raw_token_less_comparator{});
}

static
mutation_partition_v2::rows_type::iterator on_evicted_shallow(rows_entry& e, cache_tracker& tracker) noexcept {
    mutation_partition_v2::rows_type::iterator it(&e);
    if (e.is_last_dummy()) {
        // Every evictable partition entry must have a dummy entry at the end,
        // so don't remove it, just unlink from the LRU.
        // That dummy is linked in the LRU, because there may be partitions
        // with no regular rows, and we need to track them.

        // We still need to break continuity in order to preserve the "older versions are evicted first"
        // invariant.
        it->set_continuous(false);
    } else {
        // When evicting a dummy with both sides continuous we don't need to break continuity.
        //
        auto still_continuous = e.continuous() && e.dummy();
        auto old_rt = e.range_tombstone();
        mutation_partition_v2::rows_type::key_grabber kg(it);
        kg.release(current_deleter<rows_entry>());
        if (!still_continuous || old_rt != it->range_tombstone()) {
            it->set_continuous(false);
        }
        tracker.on_row_eviction();
    }
    return it;
}

void rows_entry::on_evicted(cache_tracker& tracker) noexcept {
    auto it = ::on_evicted_shallow(*this, tracker);

    mutation_partition_v2::rows_type* rows = it.tree_if_singular();
    if (rows != nullptr) {
        SCYLLA_ASSERT(it->is_last_dummy());
        partition_version& pv = partition_version::container_of(mutation_partition_v2::container_of(*rows));
        if (pv.is_referenced_from_entry()) {
            partition_entry& pe = partition_entry::container_of(pv);
            if (!pe.is_locked()) {
                cache_entry& ce = cache_entry::container_of(pe);
                ce.on_evicted(tracker);
            }
        }
    }
}

void rows_entry::on_evicted() noexcept {
    on_evicted(*current_tracker);
}

void rows_entry::on_evicted_shallow() noexcept {
    ::on_evicted_shallow(*this, *current_tracker);
}

mutation_reader cache_entry::read(row_cache& rc, read_context& reader) {
    auto source_and_phase = rc.snapshot_of(_key);
    reader.enter_partition(_key, source_and_phase.snapshot, source_and_phase.phase);
    return do_read(rc, reader);
}

mutation_reader cache_entry::read(row_cache& rc, read_context& reader, row_cache::phase_type phase) {
    reader.enter_partition(_key, phase);
    return do_read(rc, reader);
}

mutation_reader cache_entry::read(row_cache& rc, std::unique_ptr<read_context> unique_ctx) {
    auto source_and_phase = rc.snapshot_of(_key);
    unique_ctx->enter_partition(_key, source_and_phase.snapshot, source_and_phase.phase);
    return do_read(rc, std::move(unique_ctx));
}

mutation_reader cache_entry::read(row_cache& rc, std::unique_ptr<read_context> unique_ctx, row_cache::phase_type phase) {
    unique_ctx->enter_partition(_key, phase);
    return do_read(rc, std::move(unique_ctx));
}

// Assumes reader is in the corresponding partition
mutation_reader cache_entry::do_read(row_cache& rc, read_context& reader) {
    auto snp = _pe.read(rc._tracker.region(), rc._tracker.cleaner(), &rc._tracker, reader.phase());
    auto ckr = query::clustering_key_filter_ranges::get_ranges(*schema(), reader.native_slice(), _key.key());
    schema_ptr entry_schema = to_query_domain(reader.slice(), schema());
    auto r = make_cache_mutation_reader(entry_schema, _key, std::move(ckr), rc, reader, std::move(snp));
    r.upgrade_schema(to_query_domain(reader.slice(), rc.schema()));
    r.upgrade_schema(reader.schema());
    return r;
}

mutation_reader cache_entry::do_read(row_cache& rc, std::unique_ptr<read_context> unique_ctx) {
    auto snp = _pe.read(rc._tracker.region(), rc._tracker.cleaner(), &rc._tracker, unique_ctx->phase());
    auto ckr = query::clustering_key_filter_ranges::get_ranges(*schema(), unique_ctx->native_slice(), _key.key());
    schema_ptr reader_schema = unique_ctx->schema();
    schema_ptr entry_schema = to_query_domain(unique_ctx->slice(), schema());
    auto rc_schema = to_query_domain(unique_ctx->slice(), rc.schema());
    auto r = make_cache_mutation_reader(entry_schema, _key, std::move(ckr), rc, std::move(unique_ctx), std::move(snp));
    r.upgrade_schema(rc_schema);
    r.upgrade_schema(reader_schema);
    return r;
}

const schema_ptr& row_cache::schema() const {
    return _schema;
}

void row_cache::upgrade_entry(cache_entry& e) {
    if (e.schema() != _schema && !e.partition().is_locked()) {
        auto& r = _tracker.region();
        SCYLLA_ASSERT(!r.reclaiming_enabled());
        e.partition().upgrade(r, _schema, _tracker.cleaner(), &_tracker);
    }
}

std::ostream& operator<<(std::ostream& out, row_cache& rc) {
    rc._read_section(rc._tracker.region(), [&] {
        fmt::print(out, "{{row_cache: {}}}", fmt::join(rc._partitions.begin(), rc._partitions.end(), ", "));
    });
    return out;
}

future<> row_cache::do_update(row_cache::external_updater eu, row_cache::internal_updater iu) noexcept {
  // FIXME: indentation
  return do_with(std::move(eu), std::move(iu), [this] (auto& eu, auto& iu) {
    return futurize_invoke([this] {
        return get_units(_update_sem, 1);
    }).then([this, &eu, &iu] (auto permit) mutable {
      return eu.prepare().then([this, &eu, &iu, permit = std::move(permit)] () mutable {
        try {
            eu.execute();
        } catch (...) {
            // Any error from execute is considered fatal
            // to enforce exception safety.
            on_fatal_internal_error(clogger, fmt::format("Fatal error during cache update: {}", std::current_exception()));
        }
        [&] () noexcept {
            _prev_snapshot_pos = dht::ring_position::min();
            _prev_snapshot = std::exchange(_underlying, _snapshot_source());
            ++_underlying_phase;
        }();
        return futurize_invoke([&iu] {
            return iu();
        }).then_wrapped([this, permit = std::move(permit)] (auto f) {
            _prev_snapshot_pos = {};
            _prev_snapshot = {};
            if (f.failed()) {
                clogger.warn("Failure during cache update: {}", f.get_exception());
            }
        });
      });
    });
  });
}

auto fmt::formatter<cache_entry>::format(const cache_entry& e, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{cache_entry: {}, cont={}, dummy={}, {}}}",
               e.position(), e.continuous(), e.is_dummy_entry(),
               partition_entry::printer(e.partition()));
}
