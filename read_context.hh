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

class read_context final : public enable_lw_shared_from_this<read_context> {
    row_cache& _cache;
    schema_ptr _schema;
    const dht::partition_range& _range;
    const query::partition_slice& _slice;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    streamed_mutation::forwarding _fwd;
    mutation_reader::forwarding _fwd_mr;
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
};

}
