/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/coroutine/maybe_yield.hh>

#include "mutation_query.hh"
#include "mutation/mutation_compactor.hh"
#include "query/query-result-writer.hh"
#include "readers/mutation_source.hh"
#include "schema/schema_registry.hh"

#include <boost/range/algorithm/equal.hpp>

reconcilable_result::~reconcilable_result() {}

reconcilable_result::reconcilable_result()
    : _row_count_low_bits(0)
    , _row_count_high_bits(0)
{ }

reconcilable_result::reconcilable_result(uint32_t row_count_low_bits, utils::chunked_vector<partition> p, query::short_read short_read,
                                         uint32_t row_count_high_bits, query::result_memory_tracker memory_tracker)
    : _row_count_low_bits(row_count_low_bits)
    , _short_read(short_read)
    , _memory_tracker(std::move(memory_tracker))
    , _partitions(std::move(p))
    , _row_count_high_bits(row_count_high_bits)
{ }

reconcilable_result::reconcilable_result(uint64_t row_count, utils::chunked_vector<partition> p, query::short_read short_read,
                                         query::result_memory_tracker memory_tracker)
    : reconcilable_result(static_cast<uint32_t>(row_count), std::move(p), short_read, static_cast<uint32_t>(row_count >> 32), std::move(memory_tracker))
{ }

const utils::chunked_vector<partition>& reconcilable_result::partitions() const {
    return _partitions;
}

utils::chunked_vector<partition>& reconcilable_result::partitions() {
    return _partitions;
}

bool
reconcilable_result::operator==(const reconcilable_result& other) const {
    return boost::equal(_partitions, other._partitions);
}

void
reconcilable_result::merge_disjoint(schema_ptr schema, const reconcilable_result& other) {
    std::copy(other._partitions.begin(), other._partitions.end(), std::back_inserter(_partitions));
    _short_read = _short_read || other._short_read;
    uint64_t row_count = this->row_count() + other.row_count();
    _row_count_low_bits = static_cast<uint32_t>(row_count);
    _row_count_high_bits = static_cast<uint32_t>(row_count >> 32);
}

auto fmt::formatter<reconcilable_result::printer>::format(
    const reconcilable_result::printer& pr,
    fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out,
                         "{{rows={}, short_read={}, ",
                         pr.self.row_count(),
                         pr.self.is_short_read());
    bool first = true;
    for (const partition& p : pr.self.partitions()) {
        if (!first) {
            out = fmt::format_to(out, ", ");
        }
        first = false;
        out = fmt::format_to(out,
                             "{{rows={}, {}}}",
                             p.row_count(),
                             p._m.pretty_printer(pr.schema));
    }
    return fmt::format_to(out, "]}}");
}

reconcilable_result::printer reconcilable_result::pretty_printer(schema_ptr s) const {
    return { *this, std::move(s) };
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> reversed(foreign_ptr<lw_shared_ptr<reconcilable_result>> result)
{
    for (auto& partition : result->partitions())
    {
        auto& m = partition.mut();
        auto schema = local_schema_registry().get(m.schema_version());
        m = frozen_mutation(reverse(m.unfreeze(schema)));
        co_await coroutine::maybe_yield();
    }

    co_return std::move(result);
}

void reconcilable_result_builder::consume_new_partition(const dht::decorated_key& dk) {
    _rt_assembler.reset();
    _return_static_content_on_partition_with_no_rows =
        _slice.options.contains(query::partition_slice::option::always_return_static_content) ||
        !has_ck_selector(_slice.row_ranges(*_query_schema, dk.key()));
    _static_row_is_alive = false;
    _live_rows = 0;
    _mutation_consumer.emplace(streamed_mutation_freezer(*_query_schema, dk.key()));
    _used_at_entry = _memory_accounter.used_memory();
}

void reconcilable_result_builder::consume(tombstone t) {
    _mutation_consumer->consume(t);
}

stop_iteration reconcilable_result_builder::consume(static_row&& sr, tombstone, bool is_alive) {
    _static_row_is_alive = is_alive;
    _memory_accounter.update(sr.memory_usage(*_query_schema));
    return _mutation_consumer->consume(std::move(sr));
}

stop_iteration reconcilable_result_builder::consume(clustering_row&& cr, row_tombstone, bool is_alive) {
    if (_rt_assembler.needs_flush()) {
        if (auto rt_opt = _rt_assembler.flush(*_query_schema, position_in_partition::after_key(*_query_schema, cr.key()))) {
            consume(std::move(*rt_opt));
        }
    }
    _live_rows += is_alive;
    auto stop = _memory_accounter.update_and_check(cr.memory_usage(*_query_schema));
    if (is_alive || _slice.options.contains<query::partition_slice::option::allow_mutation_read_page_without_live_row>()) {
        // We are considering finishing current read only after consuming a
        // live clustering row. While sending a single live row is enough to
        // guarantee progress, not ending the result on a live row would
        // mean that the next page fetch will read all tombstones after the
        // last live row again.
        _stop = stop;
    }
    return _mutation_consumer->consume(std::move(cr)) || _stop;
}

stop_iteration reconcilable_result_builder::consume(range_tombstone&& rt) {
    _memory_accounter.update(rt.memory_usage(*_query_schema));
    return _mutation_consumer->consume(std::move(rt));
}

stop_iteration reconcilable_result_builder::consume(range_tombstone_change&& rtc) {
    if (auto rt_opt = _rt_assembler.consume(*_query_schema, std::move(rtc))) {
        return consume(std::move(*rt_opt));
    }
    return stop_iteration::no;
}

stop_iteration reconcilable_result_builder::consume_end_of_partition() {
    _rt_assembler.on_end_of_stream();
    if (_live_rows == 0 && _static_row_is_alive && _return_static_content_on_partition_with_no_rows) {
        ++_live_rows;
        // Normally we count only live clustering rows, to guarantee that
        // the next page fetch won't ask for the same range. However,
        // if we return just a single static row we can stop the result as
        // well. Next page fetch will ask for the next partition and if we
        // don't do that we could end up with an unbounded number of
        // partitions with only a static row.
        _stop = _stop || _memory_accounter.check();
    }
    _total_live_rows += _live_rows;
    _result.emplace_back(partition { _live_rows, _mutation_consumer->consume_end_of_stream() });

    auto accounted = _memory_accounter.used_memory() - _used_at_entry;
    auto actually_used = sizeof(partition) + _result.back().mut().representation().size();
    if (actually_used > accounted) {
        _memory_accounter.update(actually_used - accounted);
    }

    if (_slice.options.contains<query::partition_slice::option::allow_mutation_read_page_without_live_row>()) {
        _stop = _stop || _memory_accounter.check();
    }

    return _stop;
}

reconcilable_result reconcilable_result_builder::consume_end_of_stream() {
    return reconcilable_result(_total_live_rows, std::move(_result),
                               query::short_read(bool(_stop)),
                               std::move(_memory_accounter).done());
}

future<query::result>
to_data_query_result(const reconcilable_result& r, schema_ptr s, const query::partition_slice& slice, uint64_t max_rows, uint32_t max_partitions,
        query::result_options opts) {
    // This result was already built with a limit, don't apply another one.
    query::result::builder builder(slice, opts, query::result_memory_accounter{ query::result_memory_limiter::unlimited_result_size }, query::max_tombstones);
    auto consumer = compact_for_query<query_result_builder>(*s, gc_clock::time_point::min(), slice, max_rows,
            max_partitions, tombstone_gc_state::no_gc(), query_result_builder(*s, builder));
    auto compaction_state = consumer.get_state();
    frozen_mutation_consumer_adaptor adaptor(s, consumer);
    for (const partition& p : r.partitions()) {
        const auto res = co_await p.mut().consume_gently(s, adaptor);
        if (res.stop == stop_iteration::yes) {
            break;
        }
    }
    if (r.is_short_read()) {
        builder.mark_as_short_read();
    }
    co_return builder.build(compaction_state->current_full_position());
}

query::result
query_mutation(mutation&& m, const query::partition_slice& slice, uint64_t row_limit, gc_clock::time_point now, query::result_options opts) {
    query::result::builder builder(slice, opts, query::result_memory_accounter{ query::result_memory_limiter::unlimited_result_size }, query::max_tombstones);
    auto consumer = compact_for_query<query_result_builder>(*m.schema(), now, slice, row_limit,
            query::max_partitions, tombstone_gc_state::no_gc(), query_result_builder(*m.schema(), builder));
    auto compaction_state = consumer.get_state();
    std::move(m).consume(consumer, consume_in_reverse::no);
    return builder.build(compaction_state->current_full_position());
}

class counter_write_query_result_builder {
    const schema& _schema;
    mutation_opt _mutation;
public:
    counter_write_query_result_builder(const schema& s) : _schema(s) { }
    void consume_new_partition(const dht::decorated_key& dk) {
        _mutation = mutation(_schema.shared_from_this(), dk);
    }
    void consume(tombstone) { }
    stop_iteration consume(static_row&& sr, tombstone, bool is_live) {
        if (!is_live) {
            return stop_iteration::no;
        }
        _mutation->partition().static_row().maybe_create() = std::move(sr.cells());
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone,  bool is_live) {
        if (!is_live) {
            return stop_iteration::no;
        }
        _mutation->partition().insert_row(_schema, cr.key(), std::move(cr).as_deletable_row());
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }
    mutation_opt consume_end_of_stream() {
        return std::move(_mutation);
    }
};

future<mutation_opt> counter_write_query(schema_ptr s, const mutation_source& source, reader_permit permit,
                                         const dht::decorated_key& dk,
                                         const query::partition_slice& slice,
                                         tracing::trace_state_ptr trace_ptr)
{
    struct range_and_reader {
        dht::partition_range range;
        mutation_reader reader;

        range_and_reader(range_and_reader&&) = delete;
        range_and_reader(const range_and_reader&) = delete;

        range_and_reader(schema_ptr s, const mutation_source& source, reader_permit permit,
                         const dht::decorated_key& dk,
                         const query::partition_slice& slice,
                         tracing::trace_state_ptr trace_ptr)
            : range(dht::partition_range::make_singular(dk))
            , reader(source.make_mutation_reader(s, std::move(permit), range, slice,
                                                      std::move(trace_ptr), streamed_mutation::forwarding::no,
                                                      mutation_reader::forwarding::no))
        { }
    };

    // do_with() doesn't support immovable objects
    auto r_a_r = std::make_unique<range_and_reader>(s, source, std::move(permit), dk, slice, std::move(trace_ptr));
    auto cwqrb = counter_write_query_result_builder(*s);
    auto cfq = compact_for_query<counter_write_query_result_builder>(
            *s, gc_clock::now(), slice, query::max_rows, query::max_partitions, tombstone_gc_state::no_gc(), std::move(cwqrb));
    auto f = r_a_r->reader.consume(std::move(cfq));
    return f.finally([r_a_r = std::move(r_a_r)] {
        return r_a_r->reader.close();
    });
}
