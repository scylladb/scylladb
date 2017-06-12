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

#include "schema.hh"
#include "query-request.hh"
#include "streamed_mutation.hh"
#include "partition_version.hh"
#include "tracing/tracing.hh"
#include "row_cache.hh"

namespace cache {

/*
 * Represent a reader to the underlying source.
 * This reader automatically makes sure that it's up to date with all cache updates
 */
class autoupdating_underlying_reader final {
    row_cache& _cache;
    read_context& _read_context;
    stdx::optional<mutation_reader> _reader;
    utils::phased_barrier::phase_type _reader_creation_phase;
    dht::partition_range _range = { };
    stdx::optional<dht::decorated_key> _last_key;
    stdx::optional<dht::decorated_key> _new_last_key;
public:
    autoupdating_underlying_reader(row_cache& cache, read_context& context)
        : _cache(cache)
        , _read_context(context)
    { }
    // Reads next partition without changing mutation source snapshot.
    future<streamed_mutation_opt> read_next_same_phase() {
        _last_key = std::move(_new_last_key);
        return (*_reader)().then([this] (auto&& smopt) {
            if (smopt) {
                _new_last_key = smopt->decorated_key();
            }
            return std::move(smopt);
        });
    }
    future<streamed_mutation_opt> operator()() {
        _last_key = std::move(_new_last_key);
        auto start = population_range_start();
        auto phase = _cache.phase_of(start);
        if (!_reader || _reader_creation_phase != phase) {
            if (_last_key) {
                auto cmp = dht::ring_position_comparator(*_cache._schema);
                auto&& new_range = _range.split_after(*_last_key, cmp);
                if (!new_range) {
                    return make_ready_future<streamed_mutation_opt>(streamed_mutation_opt());
                }
                _range = std::move(*new_range);
                _last_key = {};
            }
            auto& snap = _cache.snapshot_for_phase(phase);
            _reader = _cache.create_underlying_reader(_read_context, snap, _range);
            _reader_creation_phase = phase;
        }
        return (*_reader)().then([this] (auto&& smopt) {
            if (smopt) {
                _new_last_key = smopt->decorated_key();
            }
            return std::move(smopt);
        });
    }
    future<> fast_forward_to(dht::partition_range&& range) {
        auto snapshot_and_phase = _cache.snapshot_of(dht::ring_position_view::for_range_start(_range));
        return fast_forward_to(std::move(range), snapshot_and_phase.snapshot, snapshot_and_phase.phase);
    }
    future<> fast_forward_to(dht::partition_range&& range, mutation_source& snapshot, row_cache::phase_type phase) {
        _range = std::move(range);
        _last_key = { };
        _new_last_key = { };
        if (_reader && _reader_creation_phase == phase) {
            return _reader->fast_forward_to(_range);
        }
        _reader = _cache.create_underlying_reader(_read_context, snapshot, _range);
        _reader_creation_phase = phase;
        return make_ready_future<>();
    }
    utils::phased_barrier::phase_type creation_phase() const {
        assert(_reader);
        return _reader_creation_phase;
    }
    const dht::partition_range& range() const {
        return _range;
    }
    dht::ring_position_view population_range_start() const {
        return _last_key ? dht::ring_position_view::for_after_key(*_last_key)
                         : dht::ring_position_view::for_range_start(_range);
    }
};

class read_context final : public enable_lw_shared_from_this<read_context> {
    row_cache& _cache;
    schema_ptr _schema;
    const dht::partition_range& _range;
    const query::partition_slice& _slice;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    streamed_mutation::forwarding _fwd;
    mutation_reader::forwarding _fwd_mr;
    bool _range_query;
    autoupdating_underlying_reader _underlying;
public:
    read_context(row_cache& cache,
            schema_ptr schema,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr)
        : _cache(cache)
        , _schema(std::move(schema))
        , _range(range)
        , _slice(slice)
        , _pc(pc)
        , _trace_state(std::move(trace_state))
        , _fwd(fwd)
        , _fwd_mr(fwd_mr)
        , _range_query(!range.is_singular() || !range.start()->value().has_key())
        , _underlying(_cache, *this)
    { }
    read_context(const read_context&) = delete;
    row_cache& cache() { return _cache; }
    const schema_ptr& schema() const { return _schema; }
    const dht::partition_range& range() const { return _range; }
    const query::partition_slice& slice() const { return _slice; }
    const io_priority_class& pc() const { return _pc; }
    tracing::trace_state_ptr trace_state() const { return _trace_state; }
    streamed_mutation::forwarding fwd() const { return _fwd; }
    mutation_reader::forwarding fwd_mr() const { return _fwd_mr; }
    bool is_range_query() const { return _range_query; }
    autoupdating_underlying_reader& underlying() { return _underlying; }
};

}
