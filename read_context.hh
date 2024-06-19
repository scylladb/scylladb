/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "schema/schema_fwd.hh"
#include "query-request.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/partition_version.hh"
#include "tracing/tracing.hh"
#include "row_cache.hh"

namespace cache {

/*
* Represent a flat reader to the underlying source.
* This reader automatically makes sure that it's up to date with all cache updates
*/
class autoupdating_underlying_reader final {
    row_cache& _cache;
    read_context& _read_context;
    mutation_reader_opt _reader;
    utils::phased_barrier::phase_type _reader_creation_phase = 0;
    dht::partition_range _range = { };
    std::optional<dht::decorated_key> _last_key;
    std::optional<dht::decorated_key> _new_last_key;

    future<> close_reader() noexcept {
        return _reader ? _reader->close() : make_ready_future<>();
    }
public:
    autoupdating_underlying_reader(row_cache& cache, read_context& context)
        : _cache(cache)
        , _read_context(context)
    { }
    future<mutation_fragment_v2_opt> move_to_next_partition() {
        _last_key = std::move(_new_last_key);
        auto start = population_range_start();
        auto phase = _cache.phase_of(start);
        if (!_reader || _reader_creation_phase != phase) {
            if (_last_key) {
                auto cmp = dht::ring_position_comparator(*_cache._schema);
                auto&& new_range = _range.split_after(*_last_key, cmp);
                if (!new_range) {
                    co_await close_reader();
                    co_return std::nullopt;
                }
                _range = std::move(*new_range);
                _last_key = {};
            }
            if (_reader) {
                ++_cache._tracker._stats.underlying_recreations;
            }
            auto old_reader = std::move(*_reader);
            std::exception_ptr ex;
            try {
                _reader = _cache.create_underlying_reader(_read_context, _cache.snapshot_for_phase(phase), _range);
                _reader_creation_phase = phase;
            } catch(...) {
                  ex = std::current_exception();
            }
            co_await old_reader.close();
            maybe_rethrow_exception(std::move(ex));
        }
        co_await _reader->next_partition();
        if (_reader->is_end_of_stream() && _reader->is_buffer_empty()) {
            co_return std::nullopt;
        }
        auto mfopt = co_await (*_reader)();
        if (mfopt) {
            SCYLLA_ASSERT(mfopt->is_partition_start());
            _new_last_key = mfopt->as_partition_start().key();
        }
        co_return std::move(mfopt);
    }
    future<> fast_forward_to(dht::partition_range&& range) {
        auto snapshot_and_phase = _cache.snapshot_of(dht::ring_position_view::for_range_start(_range));
        return fast_forward_to(std::move(range), snapshot_and_phase.snapshot, snapshot_and_phase.phase);
    }
    future<> fast_forward_to(dht::partition_range&& range, mutation_source& snapshot, row_cache::phase_type phase) {
        _range = std::move(range);
        _last_key = { };
        _new_last_key = { };
        if (_reader) {
            if (_reader_creation_phase == phase) {
                ++_cache._tracker._stats.underlying_partition_skips;
                return _reader->fast_forward_to(_range);
            } else {
                ++_cache._tracker._stats.underlying_recreations;
            }
        }
        return close_reader().then([this, snapshot, phase] () mutable {
            _reader = _cache.create_underlying_reader(_read_context, snapshot, _range);
            _reader_creation_phase = phase;
        });
    }
    future<> close() noexcept {
        return close_reader();
    }
    utils::phased_barrier::phase_type creation_phase() const {
        return _reader_creation_phase;
    }
    const dht::partition_range& range() const {
        return _range;
    }
    mutation_reader& underlying() { return *_reader; }
    dht::ring_position_view population_range_start() const {
        return _last_key ? dht::ring_position_view::for_after_key(*_last_key)
                         : dht::ring_position_view::for_range_start(_range);
    }
};

class read_context final : public enable_lw_shared_from_this<read_context> {
    row_cache& _cache;
    schema_ptr _schema;
    reader_permit _permit;
    const dht::partition_range& _range;
    const query::partition_slice& _slice;
    tracing::trace_state_ptr _trace_state;
    mutation_reader::forwarding _fwd_mr;
    bool _range_query;
    const tombstone_gc_state* _tombstone_gc_state;
    // When reader enters a partition, it must be set up for reading that
    // partition from the underlying mutation source (_underlying) in one of two ways:
    //
    //  1) either _underlying is already in that partition
    //
    //  2) _underlying is before the partition, then _underlying_snapshot and _key
    //     are set so that _underlying_flat can be fast forwarded to the right partition.
    //
    autoupdating_underlying_reader _underlying;
    uint64_t _underlying_created = 0;

    mutation_source_opt _underlying_snapshot;
    dht::partition_range _sm_range;
    std::optional<dht::decorated_key> _key;
    bool _partition_exists;
    row_cache::phase_type _phase;
public:
    read_context(row_cache& cache,
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const tombstone_gc_state* gc_state,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr)
        : _cache(cache)
        , _schema(std::move(schema))
        , _permit(std::move(permit))
        , _range(range)
        , _slice(slice)
        , _trace_state(std::move(trace_state))
        , _fwd_mr(fwd_mr)
        , _range_query(!query::is_single_partition(range))
        , _tombstone_gc_state(gc_state)
        , _underlying(_cache, *this)
    {
        ++_cache._tracker._stats.reads;
        if (!_range_query) {
            _key = range.start()->value().as_decorated_key();
        }
    }
    ~read_context() {
        ++_cache._tracker._stats.reads_done;
        if (_underlying_created) {
            _cache._stats.reads_with_misses.mark();
            ++_cache._tracker._stats.reads_with_misses;
        } else {
            _cache._stats.reads_with_no_misses.mark();
        }
    }
    read_context(const read_context&) = delete;
    row_cache& cache() { return _cache; }
    const schema_ptr& schema() const { return _schema; }
    reader_permit permit() const { return _permit; }
    const dht::partition_range& range() const { return _range; }
    const query::partition_slice& slice() const { return _slice; }
    bool is_reversed() const { return _slice.is_reversed(); }
    // Returns a slice in the native format (for reversed reads, in native-reversed format).
    const query::partition_slice& native_slice() const { return _slice; }
    tracing::trace_state_ptr trace_state() const { return _trace_state; }
    mutation_reader::forwarding fwd_mr() const { return _fwd_mr; }
    bool is_range_query() const { return _range_query; }
    autoupdating_underlying_reader& underlying() { return _underlying; }
    row_cache::phase_type phase() const { return _phase; }
    const dht::decorated_key& key() const { return *_key; }
    bool partition_exists() const { return _partition_exists; }
    void on_underlying_created() { ++_underlying_created; }
    bool digest_requested() const { return _slice.options.contains<query::partition_slice::option::with_digest>(); }
    const tombstone_gc_state* tombstone_gc_state() const { return _tombstone_gc_state; }
public:
    future<> ensure_underlying() {
        if (_underlying_snapshot) {
            return create_underlying().then([this] {
                return _underlying.underlying()().then([this] (mutation_fragment_v2_opt&& mfopt) {
                    _partition_exists = bool(mfopt);
                });
            });
        }
        // We know that partition exists because all the callers of
        // enter_partition(const dht::decorated_key&, row_cache::phase_type)
        // check that and there's no other way of setting _underlying_snapshot
        // to empty. Except for calling create_underlying.
        _partition_exists = true;
        return make_ready_future<>();
    }
public:
    future<> create_underlying();
    void enter_partition(const dht::decorated_key& dk, mutation_source& snapshot, row_cache::phase_type phase) {
        _phase = phase;
        _underlying_snapshot = snapshot;
        _key = dk;
    }
    // Precondition: each caller needs to make sure that partition with |dk| key
    //               exists in underlying before calling this function.
    void enter_partition(const dht::decorated_key& dk, row_cache::phase_type phase) {
        _phase = phase;
        _underlying_snapshot = {};
        _key = dk;
    }
    future<> close() noexcept {
        return _underlying.close();
    }
};

}
