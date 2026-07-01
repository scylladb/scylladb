/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/insert_select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "service/pager/paging_state.hh"
#include "service_permit.hh"
#include "db/timeout_clock.hh"
#include "transport/messages/result_message.hh"
#include "mutation/mutation.hh"
#include "utils/chunked_vector.hh"
#include "exceptions/exceptions.hh"
#include "dht/token.hh"

#include <seastar/core/smp.hh>
#include <seastar/core/loop.hh>

#include <limits>

namespace cql3 {

namespace statements {

// Tunables: bound coordinator memory / in-flight write load. The read side
// pages the source so only one page is materialized at a time; writes are
// flushed in batches of this many rows.
static constexpr int32_t read_page_size = 1000;
static constexpr size_t write_batch_rows = 100;
// Token sub-ranges processed concurrently. Bounds coordinator memory and
// in-flight read+write load (backpressure): at most this many per-range
// pipelines are live at once, regardless of how many ranges the ring is split
// into.
static constexpr size_t max_concurrent_ranges = 8;

insert_select_statement::insert_select_statement(schema_ptr target_schema,
        ::shared_ptr<cql3::statements::select_statement> source,
        std::vector<column_mapping_entry> mapping,
        std::vector<uint32_t> partition_key_mapping_indices,
        std::vector<uint32_t> clustering_key_mapping_indices,
        std::unique_ptr<attributes> attrs,
        uint32_t bound_terms,
        cql_stats& stats)
    : cql_statement_no_metadata(&timeout_config::write_timeout)
    , _target_schema(std::move(target_schema))
    , _source(std::move(source))
    , _mapping(std::move(mapping))
    , _partition_key_mapping_indices(std::move(partition_key_mapping_indices))
    , _clustering_key_mapping_indices(std::move(clustering_key_mapping_indices))
    , _attrs(std::move(attrs))
    , _bound_terms(bound_terms)
{
    // Resolve the source result-set column name for each mapping entry once, so
    // value reads use the SELECTed column (by position) rather than the target
    // column name. These differ under positional remapping, e.g.
    //   INSERT INTO t (a, b) SELECT x, y FROM s
    const auto& source_names = _source->get_result_metadata()->get_names();
    _source_names.reserve(_mapping.size());
    for (const auto& entry : _mapping) {
        _source_names.push_back(source_names[entry.source_index]->name->text());
    }
}

future<> insert_select_statement::check_access(query_processor& qp, const service::client_state& state) const {
    // Reading the source requires SELECT on it; writing the target requires
    // MODIFY on the target.
    co_await _source->check_access(qp, state);
    co_await state.has_column_family_access(_target_schema->ks_name(), _target_schema->cf_name(),
            auth::permission::MODIFY);
}

bool insert_select_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    if (_source->depends_on(ks_name, cf_name)) {
        return true;
    }
    if (ks_name != _target_schema->ks_name()) {
        return false;
    }
    return !cf_name || *cf_name == _target_schema->cf_name();
}

size_t insert_select_statement::compute_range_split() const {
    // Scale the split to the shard count so the per-range read+write pipelines
    // spread across shards (and, via the write path, across owning replicas).
    // The factor over-splits relative to shards so that ranges that happen to
    // hold little data don't leave shards idle.
    return std::max<size_t>(1, seastar::smp::count) * 4;
}

dht::partition_range_vector insert_select_statement::split_token_ring(size_t n) {
    // Evenly spaced interior boundaries across the int64 token space, producing
    // half-open ranges [starting_at(b_i), starting_at(b_{i+1})). The first range
    // is open at the start and the last open at the end, so the union is the
    // entire ring [min, max] and the ranges are pairwise disjoint: every token
    // falls into exactly one range, so each source partition is read once.
    dht::partition_range_vector ranges;
    if (n <= 1) {
        ranges.push_back(dht::partition_range::make_open_ended_both_sides());
        return ranges;
    }
    ranges.reserve(n);
    using int128 = __int128;
    const int128 lo = std::numeric_limits<int64_t>::min();
    const int128 hi = std::numeric_limits<int64_t>::max();
    const int128 width = hi - lo; // spans the whole 2^64-1 token interval

    std::optional<dht::partition_range::bound> start; // unbounded for the first range
    for (size_t i = 0; i < n; ++i) {
        if (i + 1 < n) {
            const int64_t b = static_cast<int64_t>(lo + (width * int128(i + 1)) / int128(n));
            auto boundary = dht::ring_position::starting_at(dht::token::from_int64(b));
            // End this range just before the boundary (exclusive)...
            ranges.emplace_back(std::move(start),
                    dht::partition_range::bound(boundary, false));
            // ...and start the next one inclusively at the same boundary.
            start = dht::partition_range::bound(std::move(boundary), true);
        } else {
            // Last range runs to the end of the ring.
            ranges.emplace_back(std::move(start), std::nullopt);
        }
    }
    return ranges;
}

future<::shared_ptr<cql_transport::messages::result_message>>
insert_select_statement::execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const {
    // Distributed copy. The source is read with native paging (the coordinator
    // scans token ranges across the cluster, one page at a time), each page is
    // converted to target mutations, and the mutations are applied through the
    // normal write path which routes each one to its owning replicas. Read
    // memory is bounded to one page per in-flight range; writes are batched.
    //
    // When the source is a plain full-ring scan, the ring is split into N
    // disjoint token sub-ranges and up to `max_concurrent_ranges` per-range
    // pipelines run concurrently (the semaphore bounds coordinator memory and
    // in-flight load). Each sub-range restricts its inner SELECT to its tokens,
    // so the union is a single complete copy with no row read twice. Anything
    // that can't be split that way (aggregation, LIMIT, ORDER BY, secondary
    // index, or a partition-restricted query) falls back to one full-ring pass.
    //
    // The write timestamp and TTL are resolved once here so every copied row
    // shares them across all ranges (matching single-pass semantics).
    const api::timestamp_type timestamp = _attrs->get_timestamp(api::new_timestamp(), options);
    std::optional<gc_clock::duration> ttl;
    if (auto raw_ttl = _attrs->get_time_to_live(options)) {
        ttl = std::chrono::duration_cast<gc_clock::duration>(std::chrono::seconds(*raw_ttl));
    }

    const size_t n = _source->can_be_split_into_token_ranges(options) ? compute_range_split() : 1;

    uint64_t copied = 0;
    if (n <= 1) {
        copied = co_await process_token_range(qp, state, options,
                dht::partition_range::make_open_ended_both_sides(), /*restrict_to_range=*/false,
                timestamp, ttl);
    } else {
        auto ranges = split_token_ring(n);
        co_await seastar::max_concurrent_for_each(ranges, max_concurrent_ranges,
                [&] (const dht::partition_range& range) -> future<> {
            // Single-threaded per shard: this `+=` runs between co_await points,
            // so no synchronization is needed across the concurrent ranges.
            copied += co_await process_token_range(qp, state, options, range,
                    /*restrict_to_range=*/true, timestamp, ttl);
        });
    }
    (void)copied;
    co_return seastar::make_shared<cql_transport::messages::result_message::void_message>();
}

future<uint64_t>
insert_select_statement::process_token_range(query_processor& qp, service::query_state& state,
        const query_options& options, const dht::partition_range& range, bool restrict_to_range,
        api::timestamp_type timestamp, std::optional<gc_clock::duration> ttl) const {
    // storage_proxy::mutate() takes a chunked_vector.
    utils::chunked_vector<mutation> batch;
    batch.reserve(write_batch_rows);
    uint64_t copied = 0;

    auto flush = [&] () -> future<> {
        if (batch.empty()) {
            co_return;
        }
        utils::chunked_vector<mutation> to_write = std::exchange(batch, {});
        batch.reserve(write_batch_rows);
        // Each mutation is routed to its owning replicas: writes fan out across
        // the cluster rather than being applied locally.
        co_await qp.proxy().mutate(std::move(to_write), options.get_consistency(),
                db::no_timeout, state.get_trace_state(), empty_service_permit(),
                db::allow_per_partition_rate_limit::yes);
    };

    // Build one target mutation from a source result-set row.
    auto row_to_mutation = [&] (const cql3::untyped_result_set_row& row) -> mutation {
        std::vector<bytes> pk_components;
        pk_components.reserve(_partition_key_mapping_indices.size());
        for (uint32_t mi : _partition_key_mapping_indices) {
            pk_components.push_back(row.get_blob_unfragmented(_source_names[mi]));
        }
        auto pkey = partition_key::from_exploded(*_target_schema, pk_components);
        mutation m(_target_schema, pkey);

        std::vector<bytes> ck_components;
        ck_components.reserve(_clustering_key_mapping_indices.size());
        for (uint32_t mi : _clustering_key_mapping_indices) {
            ck_components.push_back(row.get_blob_unfragmented(_source_names[mi]));
        }
        clustering_key ckey = ck_components.empty()
                ? clustering_key::make_empty()
                : clustering_key::from_exploded(*_target_schema, ck_components);

        auto& crow = m.partition().clustered_row(*_target_schema, ckey);
        for (size_t k = 0; k < _mapping.size(); ++k) {
            const column_definition& def = *_mapping[k].target_column;
            if (def.is_primary_key()) {
                continue;
            }
            const sstring& source_name = _source_names[k];
            if (!row.has(source_name)) {
                continue; // NULL selected value -> leave the cell unset
            }
            bytes value = row.get_blob_unfragmented(source_name);
            auto cell = ttl
                    ? atomic_cell::make_live(*def.type, timestamp, value, gc_clock::now() + *ttl, *ttl)
                    : atomic_cell::make_live(*def.type, timestamp, value);
            crow.cells().apply(def, std::move(cell));
        }
        // INSERT writes a row marker so the row exists even with only key
        // columns. (A row marker on an empty clustering key for a non-clustered
        // table is also correct.)
        crow.apply(row_marker(timestamp));
        return m;
    };

    // Drive the prepared inner SELECT with native paging, reusing the client's
    // bound values (carried by `options`). One page is materialized at a time.
    lw_shared_ptr<service::pager::paging_state> page_state; // empty => first page
    for (;;) {
        auto base = std::make_unique<query_options>(options);
        query_options paged(std::move(base), page_state, read_page_size);

        // Restricted ranges read only their token sub-range (range-parallel
        // fan-out); the single-pass fallback reads the whole ring.
        auto msg = restrict_to_range
                ? co_await _source->execute_paged_for_token_range(qp, state, paged, range)
                : co_await _source->execute(qp, state, paged, std::nullopt);
        auto rows_msg = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        if (!rows_msg) {
            // A SELECT always yields rows; anything else is unexpected.
            break;
        }

        cql3::untyped_result_set urs(rows_msg);
        for (const auto& row : urs) {
            batch.push_back(row_to_mutation(row));
            ++copied;
            if (batch.size() >= write_batch_rows) {
                co_await flush();
            }
        }

        auto next = rows_msg->rs().get_metadata().paging_state();
        if (!next) {
            break;
        }
        // The paging ctor wants a mutable lw_shared_ptr; copy the const state.
        page_state = make_lw_shared<service::pager::paging_state>(*next);
    }

    co_await flush();
    co_return copied;
}

}

}
