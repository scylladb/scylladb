/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

#include "replica/querier.hh"
#include "replica/query_state.hh"
#include "mutation_query.hh"
#include "query/query-result-writer.hh"

// The page drivers of table queries, with some extension points
// exposed through template parameters.

namespace replica {

/// The caller's work around a page read. A context provides:
/// - start_page(), which the page calls first. Its result lives until the
///   page ends. replica::table holds its gate and measures the latency there.
/// - before_new_querier(range), which the page awaits before it creates a
///   querier for a new partition range. A saved querier does not wait.
template <typename Context>
concept page_context = requires (Context& ctx, const dht::partition_range& range) {
    ctx.start_page();
    { ctx.before_new_querier(range) } -> std::same_as<future<>>;
};

/// A page_context which additionally provides, for a data page:
/// - make_accounter(), which the page awaits for its memory accounter, after
///   start_page().
/// - before_read(), which the page awaits after make_accounter(), before it
///   reads anything. replica::table injects errors there.
template <typename Context>
concept data_page_context = page_context<Context> && requires (Context& ctx) {
    { ctx.make_accounter() } -> std::same_as<future<query::result_memory_accounter>>;
    { ctx.before_read() } -> std::same_as<future<>>;
};

/// Reads one page of a data query from `source`.
///
/// Reads `ranges` in order, with a new querier for each range, until the page
/// reaches its limits.
///
/// The result's last position is the position of the last fragment which the
/// querier that read last consumed. It is set whenever that querier entered a
/// partition, also when the page read its ranges to the end. So it tells where
/// the reader is, not whether the page stopped before the end of its ranges.
///
/// `saved_querier` is an input and an output. On input, it holds the querier
/// which the previous page saved, if any. That querier reads the first range.
/// On output, it holds the querier to save for the next page, if any. Pass
/// nullptr when queriers are not saved.
///
/// The command's limits must be positive. `ctx` does the caller's work around
/// the page, see data_page_context.
template <data_page_context Context>
future<lw_shared_ptr<query::result>> read_data_page(Context ctx,
        mutation_source source,
        schema_ptr query_schema,
        reader_permit permit,
        const query::read_command& cmd,
        query::result_options opts,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        tombstone_gc_state gc_state,
        querier_base::querier_config config,
        std::optional<querier>* saved_querier) {
    [[maybe_unused]] const auto page = ctx.start_page();
    query_state qs(query_schema, cmd, opts, ranges, co_await ctx.make_accounter());
    co_await ctx.before_read();

    std::optional<querier> querier_opt;
    if (saved_querier) {
        querier_opt = std::move(*saved_querier);
    }

    while (!qs.done()) {
        auto&& range = *qs.current_partition_range++;

        if (!querier_opt) {
            co_await ctx.before_new_querier(range);
            querier_opt.emplace(source, query_schema, permit, range, qs.cmd.slice, trace_state, gc_state, config);
        }
        auto& q = *querier_opt;

        future<> fut = co_await coroutine::as_future(q.consume_page(query_result_builder(*query_schema, qs.builder), qs.remaining_rows(), qs.remaining_partitions(), qs.cmd.timestamp, trace_state));

        if (fut.failed() || !qs.done()) {
            co_await q.close();
            querier_opt = {};
        }
        if (fut.failed()) {
            co_return coroutine::exception(fut.get_exception());
        }
    }

    std::optional<full_position> last_pos;
    if (querier_opt) {
        if (querier_opt->current_position()) {
            last_pos.emplace(*querier_opt->current_position());
        }
        if (!saved_querier || (!querier_opt->are_limits_reached() && !qs.builder.is_short_read())) {
            co_await querier_opt->close();
            querier_opt = {};
        }
    }
    if (saved_querier) {
        *saved_querier = std::move(querier_opt);
    }

    co_return make_lw_shared<query::result>(qs.builder.build(std::move(last_pos)));
}

/// Reads one page of a mutation query of `range` from `source`.
///
/// A mutation page holds the replica's data with its tombstones, which the
/// coordinator merges with other replicas' pages to reconcile them. Unlike a
/// data page, it carries no last position.
///
/// See read_data_page() for `saved_querier` and the requirements. The saved
/// querier reads `range`. The caller creates `accounter`.
template <page_context Context>
future<reconcilable_result> read_mutation_page(Context ctx,
        mutation_source source,
        schema_ptr query_schema,
        reader_permit permit,
        const query::read_command& cmd,
        const dht::partition_range& range,
        tracing::trace_state_ptr trace_state,
        query::result_memory_accounter accounter,
        tombstone_gc_state gc_state,
        querier_base::querier_config config,
        std::optional<querier>* saved_querier) {
    [[maybe_unused]] const auto page = ctx.start_page();

    std::optional<querier> querier_opt;
    if (saved_querier) {
        querier_opt = std::move(*saved_querier);
    }
    if (!querier_opt) {
        co_await ctx.before_new_querier(range);
        querier_opt.emplace(source, query_schema, permit, range, cmd.slice, trace_state, gc_state, config);
    }
    auto& q = *querier_opt;

    std::exception_ptr ex;
  try {
    auto rrb = reconcilable_result_builder(*query_schema, cmd.slice, std::move(accounter));
    auto r = co_await q.consume_page(std::move(rrb), cmd.get_row_limit(), cmd.partition_limit, cmd.timestamp, trace_state);

    if (!saved_querier || (!q.are_limits_reached() && !r.is_short_read())) {
        co_await q.close();
        querier_opt = {};
    }
    if (saved_querier) {
        *saved_querier = std::move(querier_opt);
    }

    co_return r;
  } catch (...) {
    ex = std::current_exception();
  }
    co_await q.close();
    co_return coroutine::exception(std::move(ex));
}

} // namespace replica
