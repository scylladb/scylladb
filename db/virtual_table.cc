/*
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "db/virtual_table.hh"
#include "db/chained_delegating_reader.hh"
#include "readers/queue.hh"
#include "readers/reversing_v2.hh"
#include "readers/filtering.hh"
#include "readers/forwardable_v2.hh"
#include "readers/slicing_filtering.hh"
#include "dht/i_partitioner.hh"

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
    return dht::static_shard_of(*_s, dk.token()) == this_shard_id();
}

bool virtual_table::contains_key(const dht::partition_range& pr, const dht::decorated_key& dk) const {
    return pr.contains(dk, dht::ring_position_comparator(*_s));
}

mutation_source memtable_filling_virtual_table::as_mutation_source() {
    return mutation_source([this] (schema_ptr s,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) {

        struct my_units {
            reader_permit::resource_units units;
            uint64_t memory_used;

            my_units(reader_permit::resource_units&& units) : units(std::move(units)), memory_used(0) {}
        };

        auto units = make_lw_shared<my_units>(permit.consume_memory(0));

        auto populate = [this, mt = make_lw_shared<replica::memtable>(schema()), s, units, range, slice, trace_state, fwd, fwd_mr] () mutable {
            auto mutation_sink = [units, mt] (mutation m) mutable {
                mt->apply(m);
                units->units.add(units->units.permit().consume_memory(mt->occupancy().used_space() - units->memory_used));
                units->memory_used = mt->occupancy().used_space();
            };

            return execute(mutation_sink).then([this, mt, s, units, &range, &slice, &trace_state, &fwd, &fwd_mr] () {
                auto rd = mt->as_data_source().make_reader_v2(s, units->units.permit(), range, slice, trace_state, fwd, fwd_mr);

                if (!_shard_aware) {
                    rd = make_filtering_reader(std::move(rd), [this] (const dht::decorated_key& dk) -> bool {
                        return this_shard_owns(dk);
                    });
                }

                return rd;
            });
        };

        // populate keeps the memtable alive.
        return make_mutation_reader<chained_delegating_reader>(s, std::move(populate), units->units.permit());
    });
}

mutation_source streaming_virtual_table::as_mutation_source() {
    return mutation_source([this] (schema_ptr query_schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& query_slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) {

        std::unique_ptr<query::partition_slice> unreversed_slice;
        bool reversed = query_slice.is_reversed();
        if (reversed) {
            unreversed_slice = std::make_unique<query::partition_slice>(query::reverse_slice(*query_schema, query_slice));
        }
        const auto& slice = reversed ? *unreversed_slice : query_slice;
        auto table_schema = reversed ? query_schema->make_reversed() : query_schema;

        // We cannot pass the partition_range directly to execute()
        // because it is not guaranteed to be alive until execute() resolves.
        // It is only guaranteed to be alive as long as the returned reader is alive.
        // We achieve safety by mediating access through query_restrictions. When the reader
        // dies, pr is cleared and execute() will get an exception.
        struct my_result_collector : public result_collector, public query_restrictions {
            queue_reader_handle_v2 handle;

            // Valid until handle.is_terminated(), which is set to true when the
            // queue_reader dies.
            const dht::partition_range* pr;
            mutation_reader::forwarding fwd_mr;

            my_result_collector(schema_ptr s, reader_permit p, const dht::partition_range* pr, queue_reader_handle_v2&& handle)
                : result_collector(s, p)
                , handle(std::move(handle))
                , pr(pr)
            { }

            // result_collector
            future<> take(mutation_fragment_v2 fragment) override {
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

        auto reader_and_handle = make_queue_reader_v2(table_schema, permit);
        auto consumer = std::make_unique<my_result_collector>(table_schema, permit, &pr, std::move(reader_and_handle.second));
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

        if (reversed) {
            rd = make_reversing_reader(std::move(rd), permit.max_result_size(), std::move(unreversed_slice));
        }

        if (fwd == streamed_mutation::forwarding::yes) {
            rd = make_forwardable(std::move(rd));
        }

        return rd;
    });
}

future<> result_collector::emit_partition_start(dht::decorated_key dk) {
        return take(mutation_fragment_v2(*_schema, _permit, partition_start(std::move(dk), {})));
}

future<> result_collector::emit_partition_end() {
    return take(mutation_fragment_v2(*_schema, _permit, partition_end()));
}

future<> result_collector::emit_row(clustering_row&& cr) {
    return take(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
}

future<> virtual_table::apply(const frozen_mutation&) {
    return make_exception_future<>(
        virtual_table_update_exception("this virtual table doesn't allow updates")
    );
}

}
