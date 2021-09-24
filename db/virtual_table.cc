/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
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

#include "db/virtual_table.hh"
#include "db/chained_delegating_reader.hh"

namespace db {

void virtual_table::set_cell(row& cr, const bytes& column_name, data_value value) {
    auto ts = api::new_timestamp();
    auto cdef = schema()->get_column_definition(column_name);
    if (!cdef) {
        throw_with_backtrace<std::runtime_error>(format("column not found: {}", column_name));
    }
    if (!value.is_null()) {
        cr.apply(*cdef, atomic_cell::make_live(*cdef->type, ts, value.serialize_nonnull()));
    }
}

bool virtual_table::this_shard_owns(const dht::decorated_key& dk) const {
    return dht::shard_of(*_s, dk.token()) == this_shard_id();
}

bool virtual_table::contains_key(const dht::partition_range& pr, const dht::decorated_key& dk) const {
    return pr.contains(dk, dht::ring_position_comparator(*_s));
}

mutation_source memtable_filling_virtual_table::as_mutation_source() {
    return mutation_source([this] (schema_ptr s,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) {

        struct my_units {
            reader_permit::resource_units units;
            uint64_t memory_used;

            my_units(reader_permit::resource_units&& units) : units(std::move(units)), memory_used(0) {}
        };

        auto units = make_lw_shared<my_units>(permit.consume_memory(0));

        auto populate = [this, mt = make_lw_shared<memtable>(schema()), s, units, range, slice, pc, trace_state, fwd, fwd_mr] () mutable {
            auto mutation_sink = [units, mt] (mutation m) mutable {
                mt->apply(m);
                units->units.add(units->units.permit().consume_memory(mt->occupancy().used_space() - units->memory_used));
                units->memory_used = mt->occupancy().used_space();
            };

            return execute(mutation_sink).then([this, mt, s, units, &range, &slice, &pc, &trace_state, &fwd, &fwd_mr] () {
                auto rd = mt->as_data_source().make_reader(s, units->units.permit(), range, slice, pc, trace_state, fwd, fwd_mr);

                if (!_shard_aware) {
                    rd = make_filtering_reader(std::move(rd), [this] (const dht::decorated_key& dk) -> bool {
                        return this_shard_owns(dk);
                    });
                }

                return rd;
            });
        };

        // populate keeps the memtable alive.
        return make_flat_mutation_reader<chained_delegating_reader>(s, std::move(populate), units->units.permit());
    });
}

mutation_source streaming_virtual_table::as_mutation_source() {
    return mutation_source([this] (schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) {

        // We cannot pass the partition_range directly to execute()
        // because it is not guaranteed to be alive until execute() resolves.
        // It is only guaranteed to be alive as long as the returned reader is alive.
        // We achieve safety by mediating access through query_restrictions. When the reader
        // dies, pr is cleared and execute() will get an exception.
        struct my_result_collector : public result_collector, public query_restrictions {
            queue_reader_handle handle;

            // Valid until handle.is_terminated(), which is set to true when the
            // queue_reader dies.
            const dht::partition_range* pr;
            mutation_reader::forwarding fwd_mr;

            my_result_collector(schema_ptr s, reader_permit p, const dht::partition_range* pr, queue_reader_handle&& handle)
                : result_collector(s, p)
                , handle(std::move(handle))
                , pr(pr)
            { }

            // result_collector
            future<> take(mutation_fragment fragment) override {
                return handle.push(std::move(fragment));
            }

            // query_restrictions
            const dht::partition_range& partition_range() const override {
                if (handle.is_terminated()) {
                    throw std::runtime_error("read abandoned");
                }
                return *pr;
            }
        };

        auto reader_and_handle = make_queue_reader(s, permit);
        auto consumer = std::make_unique<my_result_collector>(s, permit, &pr, std::move(reader_and_handle.second));
        auto f = execute(permit, *consumer, *consumer);

        // It is safe to discard this future because:
        // - after calling `handle.push_end_of_stream()` the reader can be discarded;
        // - if the reader dies first, `execute()` will get an exception on attempt to push fragments.
        (void)f.then_wrapped([c = std::move(consumer)] (auto&& f) {
            if (f.failed()) {
                c->handle.abort(f.get_exception());
            } else if (!c->handle.is_terminated()) {
                c->handle.push_end_of_stream();
            }
        });

        auto rd = make_slicing_filtering_reader(std::move(reader_and_handle.first), pr, slice);

        if (!_shard_aware) {
            rd = make_filtering_reader(std::move(rd), [this] (const dht::decorated_key& dk) -> bool {
                return this_shard_owns(dk);
            });
        }

        if (fwd == streamed_mutation::forwarding::yes) {
            rd = make_forwardable(std::move(rd));
        }

        return rd;
    });
}

future<> result_collector::emit_partition_start(dht::decorated_key dk) {
        return take(mutation_fragment(*_schema, _permit, partition_start(std::move(dk), {})));
}

future<> result_collector::emit_partition_end() {
    return take(mutation_fragment(*_schema, _permit, partition_end()));
}

future<> result_collector::emit_row(clustering_row&& cr) {
    return take(mutation_fragment(*_schema, _permit, std::move(cr)));
}

}