/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"

#include "bytes.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "cql3/untyped_result_set.hh"
#include "db_clock.hh"
#include "db/config.hh"
#include "db/consistency_level.hh"
#include "db/consistency_level_type.hh"
#include "db/object_storage_endpoint_param.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"
#include "keys/keys.hh"
#include "locator/token_metadata.hh"
#include "partition_slice_builder.hh"
#include "query/query-request.hh"
#include "query/query-result-set.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "service_permit.hh"
#include "service/client_state.hh"
#include "service/pager/paging_state.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/sstables_manager.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/base64.hh"
#include "utils/chain_abort_source.hh"
#include "utils/chunked_vector.hh"
#include "utils/error_injection.hh"
#include "utils/hashers.hh"
#include "utils/overloaded_functor.hh"
#include "utils/rjson.hh"
#include "utils/s3/client.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <exception>
#include <limits>
#include <optional>
#include <ranges>
#include <set>
#include <stdexcept>
#include <string_view>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <zlib.h>
#include <fmt/format.h>

namespace alternator {

// Helper functions for accessing export to S3 metadata in system_distributed keyspace.
// `system_distributed` keyspace contains two tables related to Alternator export to S3 functionality:
// 1) `alternator_export_to_s3_exports` - keyed by export ARN, contains metadata about exports, such as table id, client token, export status, timestamps, etc.
// 2) `alternator_export_to_s3_client_tokens` - used for idempotency of export creation, maps client tokens to export ARNs and contains the original request for comparison during idempotent retries.
// For the schemas of both tables see db/system_distributed_keyspace.cc.
// All modifications to those tables must go through the helper functions - those make sure proper create-if-not-exists, update-if semantics are applied.
// The modifications themselves are made by db::alternator_export_table_helper, which states them in CQL; most of the reads below
// are made through storage_proxy, the exception being get_all_client_tokens(), which goes through the helper like the writes do.

// Identifies a single incarnation of a node - host_id plus gossip generation.
// The value must change after reboot / restart (gossip_generation will increase) as
// export algorithm and garbage collection relies on it.
// Stored in `node_id` columns in the format "{host_id}:{gossip_generation}".
// A default constructed value (empty host_id) means "no node".
struct live_node_identifier {
    sstring host_id;
    std::uint32_t gossip_generation = 0;

    sstring to_string() const {
        if (host_id.empty()) {
            return sstring();
        }
        return host_id + ":" + to_sstring(gossip_generation);
    }

    // Parses value created by to_string(). Empty string results in a default constructed value.
    // Throws std::invalid_argument on malformed input.
    static live_node_identifier from_string(std::string_view str);

    auto operator<=>(const live_node_identifier&) const = default;
};

struct client_row {
    sstring client_token;
    sstring export_arn;
    sstring request;
    live_node_identifier node_id;
};

struct export_row {
    sstring export_arn;
    sstring client_token;
    sstring request;
    ::table_id table_id;
    sstring export_status;
    sstring export_id_token;
    db_clock::time_point export_time;
    db_clock::time_point accepted_at;
    live_node_identifier node_id;
    // Everything that describes a finished export is optional, because it is only filled in
    // as the export runs.
    std::optional<sstring> export_manifest;
    std::optional<sstring> failure_code;
    std::optional<sstring> failure_message;
    std::optional<int64_t> item_count;
    std::optional<int64_t> billed_size_bytes;
    std::optional<db_clock::time_point> completed_at;
};

} // namespace alternator

template <> struct fmt::formatter<alternator::live_node_identifier> : fmt::formatter<string_view> {
    auto format(const alternator::live_node_identifier& id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.to_string());
    }
};

namespace alternator {

static logging::logger xlogger("alternator-export");

// Time-to-live of export rows in a terminal state (COMPLETED / FAILED). DynamoDB keeps
// the export history (as returned by DescribeExport / ListExports) for 90 days.
static constexpr std::chrono::seconds export_row_ttl = std::chrono::days(90);

live_node_identifier live_node_identifier::from_string(std::string_view str) {
    if (str.empty()) {
        return live_node_identifier{};
    }
    auto pos = str.rfind(':');
    if (pos == std::string_view::npos || pos == 0) {
        throw std::invalid_argument(fmt::format("Invalid node_id format: {}", str));
    }
    std::uint32_t generation = 0;
    auto gen_str = str.substr(pos + 1);
    auto [ptr, ec] = std::from_chars(gen_str.data(), gen_str.data() + gen_str.size(), generation);
    if (ec != std::errc() || ptr != gen_str.data() + gen_str.size()) {
        throw std::invalid_argument(fmt::format("Invalid node_id format: {}", str));
    }
    return live_node_identifier{ .host_id = sstring(str.substr(0, pos)), .gossip_generation = generation };
}

// system_distributed uses SimpleStrategy with RF=3, but in a cluster with a
// single token owner there is only one replica, so QUORUM (computed from RF)
// can never be reached. Use ONE in that case, like the rest of
// system_distributed_keyspace does (see quorum_if_many()).
static db::consistency_level get_consistency_level(service::storage_proxy& proxy) {
    return proxy.get_token_metadata_ptr()->count_normal_token_owners() > 1
            ? db::consistency_level::QUORUM : db::consistency_level::ONE;
}

// Everything needed to read whole rows of one of the two export tables through storage_proxy:
// a wildcard selection (SELECT *) and the partition slice matching it, so that what a read
// returns can be presented as rows with named columns, the way the CQL layer would. Both
// tables have a single text partition key column and no clustering columns, so every
// partition holds exactly one row.
class export_table {
    schema_ptr _schema;
    ::shared_ptr<cql3::selection::selection> _selection;
    query::partition_slice _slice;
public:
    export_table(service::storage_proxy& proxy, std::string_view table)
        : _schema(proxy.data_dictionary().find_schema(db::system_distributed_keyspace::NAME, table))
        , _selection(cql3::selection::selection::wildcard(_schema))
        , _slice(std::vector<query::clustering_range>{query::clustering_range::make_open_ended_both_sides()},
                query::column_id_vector{},
                _schema->regular_columns() | std::views::transform(&column_definition::id)
                        | std::ranges::to<query::column_id_vector>(),
                _selection->get_query_options())
    { }

    const schema_ptr& schema() const { return _schema; }

    lw_shared_ptr<query::read_command> make_read_command(service::storage_proxy& proxy) const {
        return ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), _slice,
                proxy.get_max_result_size(_slice), query::tombstone_limit(proxy.get_tombstone_limit()));
    }

    // The partition holding the row keyed by `key`.
    dht::decorated_key decorate(const sstring& key) const {
        return dht::decorate_key(*_schema, partition_key::from_single_value(*_schema, utf8_type->decompose(key)));
    }

    // Presents the result of a read made with this table's slice as rows with named columns.
    cql3::untyped_result_set rows(foreign_ptr<lw_shared_ptr<query::result>> result, const query::partition_slice& slice) const {
        return cql3::untyped_result_set(*_schema, std::move(result), *_selection, slice);
    }
};

// Reads the single row keyed by `key` and returns it converted by `convert`, or nullopt when
// there is no such row. Like every other access below this is an internal read - no
// authorization check and no user's service level applies. `permit` is the admission control
// permit the read is to be accounted to - the requesting client's one when the read is made on
// behalf of a client request, an empty one when it is made by a background fiber.
template <typename Converter>
static future<std::optional<std::invoke_result_t<Converter, const cql3::untyped_result_set_row&>>>
read_row(service::storage_proxy& proxy, const export_table& table, const sstring& key, service_permit permit, Converter convert) {
    auto& client_state = service::client_state::for_internal_calls();
    tracing::trace_state_ptr trace_state;
    auto command = table.make_read_command(proxy);
    // query_result() reports the errors the coordinator can answer with instead of throwing them,
    // so that overload and rate limiting reach the client as a retriable RequestLimitExceeded
    // rather than as an internal error.
    auto result = co_await proxy.query_result(table.schema(), command,
            dht::partition_range_vector{dht::partition_range(table.decorate(key))},
            get_consistency_level(proxy),
            service::storage_proxy::coordinator_query_options(executor::default_timeout(),
                    std::move(permit), client_state, trace_state));
    if (!result) {
        co_await coroutine::return_exception(create_api_error_from_coordinators_exception(std::move(result).assume_error()));
    }
    auto rows = table.rows(std::move(result.assume_value().query_result), command->slice);
    if (rows.empty()) {
        co_return std::nullopt;
    }
    co_return convert(rows.front());
}

static live_node_identifier get_node_id(const cql3::untyped_result_set_row& row) {
    return live_node_identifier::from_string(row.get_or<sstring>("node_id", sstring()));
}

static export_row row_to_export(const cql3::untyped_result_set_row& row) {
    return export_row{
        .export_arn = row.get_as<sstring>("export_arn"),
        .client_token = row.get_or<sstring>("client_token", sstring()),
        .request = row.get_or<sstring>("request", sstring()),
        .table_id = row.has("table_id") ? ::table_id(row.get_as<utils::UUID>("table_id")) : ::table_id(),
        .export_status = row.get_or<sstring>("export_status", sstring()),
        .export_id_token = row.get_or<sstring>("export_id_token", sstring()),
        .export_time = row.get_or<db_clock::time_point>("export_time", db_clock::time_point()),
        .accepted_at = row.get_or<db_clock::time_point>("accepted_at", db_clock::time_point()),
        .node_id = get_node_id(row),
        .export_manifest = row.get_opt<sstring>("export_manifest"),
        .failure_code = row.get_opt<sstring>("failure_code"),
        .failure_message = row.get_opt<sstring>("failure_message"),
        .item_count = row.get_opt<int64_t>("item_count"),
        .billed_size_bytes = row.get_opt<int64_t>("billed_size_bytes"),
        .completed_at = row.get_opt<db_clock::time_point>("completed_at"),
    };
}

static client_row row_to_client(const cql3::untyped_result_set_row& row) {
    return client_row{
        .client_token = row.get_as<sstring>("client_token"),
        .export_arn = row.get_or<sstring>("export_arn", sstring()),
        .request = row.get_or<sstring>("request", sstring()),
        .node_id = get_node_id(row),
    };
}

static future<std::optional<client_row>> get_client_row(service::storage_proxy& proxy, const sstring& client_token, service_permit permit) {
    export_table table(proxy, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS);
    co_return co_await read_row(proxy, table, client_token, std::move(permit), row_to_client);
}

// The export tables are written through db::alternator_export_table_helper, which puts every
// write below in a single CQL statement - see the comment on that class.
static db::alternator_export_table_helper::export_row to_stored_row(const export_row& row) {
    return db::alternator_export_table_helper::export_row{
        .export_arn = row.export_arn,
        .client_token = row.client_token,
        .request = row.request,
        .table_id = row.table_id,
        .export_manifest = row.export_manifest,
        .export_status = row.export_status,
        .failure_code = row.failure_code,
        .failure_message = row.failure_message,
        .item_count = row.item_count,
        .billed_size_bytes = row.billed_size_bytes,
        .export_id_token = row.export_id_token,
        .export_time = row.export_time,
        .accepted_at = row.accepted_at,
        .completed_at = row.completed_at,
        .node_id = row.node_id.to_string(),
    };
}

static future<bool> insert_client_row(db::system_distributed_keyspace& sdks, const client_row& row) {
    db::alternator_export_table_helper helper(sdks.qp());
    co_return co_await helper.insert_client_token({
        .client_token = row.client_token,
        .export_arn = row.export_arn,
        .request = row.request,
        .node_id = row.node_id.to_string(),
    });
}

static future<bool> insert_export(db::system_distributed_keyspace& sdks, const export_row& row) {
    db::alternator_export_table_helper helper(sdks.qp());
    // Every export row is written with the retention TTL from the start: update_export() below
    // cannot rewrite the row marker, so a row inserted without a TTL would never expire.
    co_return co_await helper.insert_export(to_stored_row(row), export_row_ttl);
}

static future<bool> update_export(db::system_distributed_keyspace& sdks, const export_row& row, const sstring& old_export_status,
        const live_node_identifier& old_node_id, std::optional<std::chrono::seconds> ttl = std::nullopt) {
    db::alternator_export_table_helper helper(sdks.qp());
    co_return co_await helper.update_export(to_stored_row(row), old_export_status, old_node_id.to_string(), ttl);
}

// Writes the terminal state (COMPLETED / FAILED) of an `IN_PROGRESS` export row owned by this
// node, retrying for as long as the write keeps throwing (a timed out or unavailable write may
// still apply, so every attempt is conditional on the row still being ours and `IN_PROGRESS`).
// This is the one write which must not be given up on: the garbage collector takes over only the
// rows of the nodes gossip no longer lists as live, so a row left `IN_PROGRESS` under this node's
// current, live incarnation is nobody's to finish - DescribeExport would keep reporting it as
// `IN_PROGRESS` until the node restarts or the row's TTL runs out.
// Returns whether the row was written - `false` means the row is no longer ours (the garbage
// collector has taken the export over), so there is nothing left to retry. Throws when `as` is
// aborted, i.e. when the node is stopping - the row is then left to the garbage collector of its
// next incarnation, which is a dead one by then.
static future<bool> write_final_export_row(db::system_distributed_keyspace& sdks, const export_row& row, abort_source& as) {
    constexpr std::chrono::seconds first_retry_delay{1};
    constexpr std::chrono::seconds max_retry_delay{60};
    for (auto delay = first_retry_delay; ; delay = std::min(delay * 2, max_retry_delay)) {
        try {
            if (co_await update_export(sdks, row, "IN_PROGRESS", row.node_id, export_row_ttl)) {
                co_return true;
            }
            // The update is conditional, so it also fails when there is no row at all - the export
            // may have failed before the row was written. Write it ourselves then; if that fails
            // too, the row is there in a state which is not ours to overwrite.
            co_return co_await insert_export(sdks, row);
        } catch (...) {
            xlogger.warn("Export {}: failed to write the final state {}, retrying in {}s: {}",
                    row.export_arn, row.export_status, delay.count(), std::current_exception());
        }
        co_await seastar::sleep_abortable(delay, as);
    }
}

static future<std::optional<export_row>> get_export(service::storage_proxy& proxy, sstring export_arn, service_permit permit) {
    export_table table(proxy, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    co_return co_await read_row(proxy, table, export_arn, std::move(permit), row_to_export);
}

// `as`, when given, aborts the scan - the scan can run for a while, so a caller which can be
// stopped needs a way to cut it short.
static future<utils::chunked_vector<client_row>> get_all_client_tokens(db::system_distributed_keyspace& sdks, const abort_source* as = nullptr) {
    db::alternator_export_table_helper helper(sdks.qp());
    auto rows = co_await helper.get_all_client_tokens(as);
    utils::chunked_vector<client_row> results;
    for (auto& row : rows) {
        results.push_back(client_row{
            .client_token = std::move(row.client_token),
            .export_arn = std::move(row.export_arn),
            .request = std::move(row.request),
            .node_id = live_node_identifier::from_string(row.node_id),
        });
    }
    co_return results;
}

// Reads at most `row_limit` rows of the given partitions of the exports table, whose schema is `schema`.
static future<foreign_ptr<lw_shared_ptr<query::result>>> read_exports(service::storage_proxy& proxy, schema_ptr schema, dht::partition_range_vector partition_ranges,
        const query::partition_slice& partition_slice, query::row_limit row_limit, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit) {
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice), query::tombstone_limit(proxy.get_tombstone_limit()), row_limit);
    service::storage_proxy::result<service::storage_proxy::coordinator_query_result> rqr =
            co_await proxy.query_result(schema, std::move(command), std::move(partition_ranges), get_consistency_level(proxy),
                    service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state, std::move(trace_state)));
    if (!rqr) {
        co_await coroutine::return_exception(create_api_error_from_coordinators_exception(std::move(rqr).assume_error()));
    }
    co_return std::move(rqr.assume_value().query_result);
}

// An export reduced to what ListExports reports about it, and what the garbage collector needs to
// pick the exports to take over. The optional members are unset in a row which lacks the column.
struct export_summary_row {
    sstring export_arn;
    // The export's status, as stored. ListExports reports it as ExportStatus.
    std::optional<sstring> status;
    // The original ExportTableToPointInTime request, as JSON. ExportType is read back from it.
    std::optional<sstring> request;
    // The node running the export, as stored (see live_node_identifier).
    std::optional<sstring> node_id;
};

// Returns every export there is, in no particular order. The table is read a page at a time, as an
// unpaged read fails once its result outgrows max_memory_for_unlimited_query_hard_limit.
// `as`, when given, aborts the scan - the scan can run for a while, so a caller which can be
// stopped needs a way to cut it short.
static future<std::vector<export_summary_row>> get_all_exports(service::storage_proxy& proxy, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit,
        const abort_source* as = nullptr) {
    using sdks = db::system_distributed_keyspace;
    static constexpr uint64_t page_size = 1000;
    schema_ptr schema = proxy.data_dictionary().find_schema(sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    auto partition_slice = partition_slice_builder(*schema)
            .with_regular_column(to_bytes("export_status"))
            .with_regular_column(to_bytes("request"))
            .with_regular_column(to_bytes("node_id"))
            .with_option<query::partition_slice::option::allow_short_read>()
            .build();
    dht::partition_range_vector partition_ranges{dht::partition_range::make_open_ended_both_sides()};
    std::vector<export_summary_row> exports;
    while (true) {
        if (as) {
            as->check();
        }
        auto result = co_await read_exports(proxy, schema, std::move(partition_ranges), partition_slice, query::row_limit(page_size), client_state, trace_state, permit);
        query::result_set rs = query::result_set::from_raw_result(schema, partition_slice, *result);
        for (const query::result_set_row& row : rs.rows()) {
            exports.push_back(export_summary_row {
                .export_arn = row.get_nonnull<sstring>("export_arn"),
                .status = row.get<sstring>("export_status"),
                .request = row.get<sstring>("request"),
                .node_id = row.get<sstring>("node_id"),
            });
        }
        // As in query_pager, a short read can end a page early, even before any row.
        if (rs.rows().size() < page_size && !result->is_short_read()) {
            co_return exports;
        }
        // Each partition holds a single row, so the next page starts after the last partition read.
        auto last_key = dht::decorate_key(*schema, result->get_or_calculate_last_position().partition);
        partition_ranges = {dht::partition_range::make_starting_with(dht::partition_range::bound(std::move(last_key), false))};
        co_await coroutine::maybe_yield();
    }
}

// Interfaces for `sink` / `source` pipelines.
// The `sink` pipeline consists of 3 stages:
//   - formatter (implements public interface `export_pipeline_interface`) - serializes rjson::value as-is to simple binary format (JSON lines, Ion, CSV are required by Amazon specs),
//   - compressor (implements `compression_interface`) - optionally compresses the data - Amazon S3 requires support for gzip compression,
//   - writer (implements `storage_sink_interface`) - writes the data to the storage (e.g. S3 or in-memory).
// After construction user is expected to call `export_pipeline_interface::process()` with an item to export,
// which will call a compressor and a writer for that item. After that the future will complete and user is free to call
// `export_pipeline_interface::process()` with another item. Once all items are processed, user is expected to call
// `export_pipeline_interface::flush_and_close()` to finalize the pipeline - the call is cascaded down the pipeline
// (`compression_interface::flush_and_close()`, then `storage_sink_interface::flush_and_close()`), flushing whatever is still
// buffered and returning the `export_pipeline_interface::result` (etag, md5 and size) of the written object. The call is
// mandatory - without it part of the data might not be written - and must be done exactly once, with no `process()` calls after it.

struct storage_sink_interface {
    virtual future<> write(std::span<const std::byte>) = 0;
    virtual future<export_pipeline_interface::result> flush_and_close() = 0;
    virtual ~storage_sink_interface() = default;
};

struct compression_interface {
    virtual future<> compress(std::span<const std::byte>) = 0;
    virtual future<export_pipeline_interface::result> flush_and_close() = 0;
    virtual ~compression_interface() = default;
};

// The `source` pipeline consists of 3 stages:
//   - reader (implements `source_interface`) - reads the data from the storage (e.g. S3 or in-memory).
//   - decompressor (implements `decompression_interface`) - optionally decompresses the data - Amazon S3 requires support for gzip compression,
//   - parser (implements `parsing_interface`) - deserializes binary data to rjson::value as-is (JSON lines, Ion, CSV are required by Amazon specs),
// After pipeline construction (see `create_**` family of factory functions in header `export.hh`) user is expected to
// call `import_pipeline_interface::read_all()`. This will read all data from the source (implementing `source_interface`), calling `read_some()` in a loop, for each blob
// calling `decompression_interface::decompress()` with it, which will - optionally - decompress it,
// then calling `parsing_interface::parse()` with the decompressed data. The `parse()` call will invoke the `on_item` (passed to the pipeline factory function) callback
// for each parsed item. Once all data is read, `read_all()` future will complete. After that user is expected to call `import_pipeline_interface::close()` to finalize the pipeline.
struct source_interface {
    virtual future<std::span<const std::byte>> read_some() = 0;
    virtual future<> close() = 0;
    virtual ~source_interface() = default;
};

struct parsing_interface {
    virtual future<> parse(std::span<const std::byte>) = 0;
    virtual future<> close() = 0;
    virtual void cancel() noexcept = 0;
    virtual ~parsing_interface() = default;
};

struct decompression_interface {
    virtual future<> decompress(std::span<const std::byte>) = 0;
    virtual future<> close() = 0;
    virtual void cancel() noexcept = 0;
    virtual ~decompression_interface() = default;
};

// Accumulates the summary of the bytes handed to a storage sink - their total size and md5 digest -
// and turns it into the `export_pipeline_interface::result` reported by `flush_and_close()`.
// Shared by all sinks, so that every one of them reports the summary the same way. The etag is the
// one part of the summary the builder cannot know, so the sink hands it in - see the two finalizers.
class sink_result_builder {
    md5_hasher _md5;
    size_t _total_size = 0;

    export_pipeline_interface::result build(sstring etag, const bytes& md5) {
        auto md5_encoded = base64_encode(md5);
        return export_pipeline_interface::result{
            .etag = std::move(etag),
            .md5 = std::move(md5_encoded),
            .size_in_bytes = _total_size
        };
    }
public:
    void update(std::span<const std::byte> data) {
        _total_size += data.size();
        _md5.update(reinterpret_cast<const char*>(data.data()), data.size());
    }
    // An object uploaded in several parts gets an etag which is not the md5 of its content, so the
    // S3 sink cannot compute the etag itself and passes in the one from S3's reply to the request
    // that created the object. This function is meant for such sinks.
    export_pipeline_interface::result finalize(sstring etag) {
        return build(std::move(etag), _md5.finalize());
    }
    // For sinks which have no etag of their own - the in-memory test sink. Amazon S3 reports the hex
    // md5 of the content as the etag of an object uploaded in a single part, so report the same here.
    export_pipeline_interface::result finalize_with_md5_etag() {
        auto md5 = _md5.finalize();
        return build(to_hex(md5), md5);
    }
};

// In memory sink - stores data to a caller owned in_memory_test_storage buffer object.
// Single threaded, single "file" use only.
class in_memory_storage_sink : public storage_sink_interface {
    std::shared_ptr<in_memory_test_storage> _storage;
    sink_result_builder _result;
public:
    explicit in_memory_storage_sink(std::shared_ptr<in_memory_test_storage> storage)
        : _storage(std::move(storage)) {}

    future<> write(std::span<const std::byte> data) override {
        _storage->append(data);
        _result.update(data);
        co_return;
    }
    future<export_pipeline_interface::result> flush_and_close() override {
        _storage->flush_write();
        co_return _result.finalize_with_md5_etag();
    }
};

// No compression compressor - passes data further down the pipeline.
class noop_compressor : public compression_interface {
    std::unique_ptr<storage_sink_interface> _sink;

public:
    // Nothing here can fail, so the sink is taken over right away - see gzip_compressor below for
    // why a stage of the pipeline takes over the stage below it only once it no longer can.
    explicit noop_compressor(std::unique_ptr<storage_sink_interface>&& sink) : _sink(std::move(sink)) {}

    future<> compress(std::span<const std::byte> data) override {
        co_await _sink->write(data);
    }
    future<export_pipeline_interface::result> flush_and_close() override {
        return _sink->flush_and_close();
    }
};

// Gzip compressor - compresses data using gzip format and writes compressed chunks to the storage sink.
// Not every call to compress() produces output; zlib may buffer data internally.
// All data is guaranteed to be flushed when flush_and_close() is called.
class gzip_compressor : public compression_interface {
    std::unique_ptr<storage_sink_interface> _sink;
    z_stream _zs = {};
    static constexpr size_t _buf_size = 4096;
    // Upper bound on the input handed to a single deflate() call - see compress().
    static constexpr size_t _max_input_size = 16 * 1024;

    // Calls deflate() until a call leaves room in the output buffer, writing out whatever it produced.
    // Leftover output space is how zlib signals that it is done with what it was given, so the single
    // condition covers both callers: with Z_NO_FLUSH (compress()) it means the input has been consumed,
    // with Z_FINISH (flush_and_close()) it means deflate() has returned Z_STREAM_END.
    future<> deflate_to_sink(int flush_type) {
        do {
            std::array<std::byte, _buf_size> output;
            _zs.next_out = reinterpret_cast<Bytef*>(output.data());
            _zs.avail_out = _buf_size;

            int ret = deflate(&_zs, flush_type);
            if (ret < Z_OK) {
                throw std::runtime_error(fmt::format("Alternator S3 export pipeline: gzip compression {}error (deflate returned {}): {}",
                        flush_type == Z_FINISH ? "flush " : "", ret, _zs.msg ? _zs.msg : "<no message>"));
            }

            auto produced = _buf_size - _zs.avail_out;
            if (produced > 0) {
                co_await _sink->write(std::span<const std::byte>(output.data(), produced));
            }
        } while (_zs.avail_out == 0);
    }

public:
    // Takes the sink over only once zlib is initialized. Initialization can fail - deflateInit2()
    // reports Z_MEM_ERROR when zlib's internal state fails to allocate - and a storage sink
    // destroyed without flush_and_close() is not something a destructor can deal with: the S3
    // one leaks its upload stream together with a started multipart upload (see ~s3_storage_sink()).
    // Taking the parameter by reference rather than by value leaves the sink - and with it the
    // responsibility to close it asynchronously - with the caller until that can no longer happen.
    explicit gzip_compressor(std::unique_ptr<storage_sink_interface>&& sink) {
        auto ret = deflateInit2(&_zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY);
        if (ret != Z_OK) {
            throw std::runtime_error(fmt::format("Alternator S3 export pipeline: gzip compressor initialization error (deflateInit2 returned {}): {}", ret, _zs.msg ? _zs.msg : "<no message>"));
        }
        _sink = std::move(sink);
    }
    gzip_compressor(const gzip_compressor&) = delete;
    gzip_compressor(gzip_compressor&&) = delete;
    gzip_compressor& operator=(const gzip_compressor&) = delete;
    gzip_compressor& operator=(gzip_compressor&&) = delete;

    ~gzip_compressor() {
        deflateEnd(&_zs);
    }

    future<> compress(std::span<const std::byte> data) override {
        // A single deflate() call is CPU bound and returns only once it has consumed all of its
        // input or filled its output buffer, so bounding its cost means keeping both small. A
        // small output buffer alone is not enough: on compressible data - and serialized items
        // are - 4 KB of output can absorb megabytes of input, which is milliseconds of reactor
        // stall in one call. Feeding the input in slices of at most _max_input_size bounds the
        // work of each call regardless of the compression ratio; a preemption point between the
        // slices is what actually breaks the stall up, and it has to be here rather than next to
        // the sink write in deflate_to_sink(), because a slice may produce no output at all.
        while (!data.empty()) {
            auto chunk = data.first(std::min(data.size(), _max_input_size));
            data = data.subspan(chunk.size());
            _zs.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(chunk.data()));
            _zs.avail_in = static_cast<uInt>(chunk.size());

            co_await deflate_to_sink(Z_NO_FLUSH);

            co_await coroutine::maybe_yield();
        }
    }

    future<export_pipeline_interface::result> flush_and_close() override {
        std::exception_ptr exception = nullptr;
        try {
            _zs.next_in = nullptr;
            _zs.avail_in = 0;
            co_await deflate_to_sink(Z_FINISH);
        }
        catch (...) {
            exception = std::current_exception();
        }
        auto res = export_pipeline_interface::result{};
        try {
            res = co_await _sink->flush_and_close();
        } catch (...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
        if (exception) {
            std::rethrow_exception(exception);
        }
        co_return res;
    }
};

// Formatter that converts rjson::value item to single JSON line and passes it to the compressor.
// The line is terminated with a newline character, so that the source pipeline can parse it line by line.
class json_formatter : public export_pipeline_interface {
    std::unique_ptr<compression_interface> _sink;
public:
    json_formatter(std::unique_ptr<compression_interface> sink) : _sink(std::move(sink)) {}

    future<> process(const rjson::value &item) override {
        // TODO(rcybulski): this is extremely slow and naive - we need a streaming version of `rjson::print` here.
        auto line = rjson::print(item);
        line += "\n";
        co_await _sink->compress(std::as_bytes(std::span<const char>(line)));
    }
    future<result> flush_and_close() override {
        return _sink->flush_and_close();
    }
};

// In memory source object - reads data from a caller owned in_memory_test_storage buffer object.
// Single threaded, single "file" use only.
// Due to a low chunk size this is test only class.
class in_memory_source : public source_interface {
    std::shared_ptr<in_memory_test_storage> _storage;
    size_t _position = 0;

public:
    in_memory_source(std::shared_ptr<in_memory_test_storage> storage)
        : _storage(std::move(storage)) {}

    future<std::span<const std::byte>> read_some() override {
        auto data = _storage->data();
        auto* ptr = reinterpret_cast<const char*>(data.data());
        if (!ptr) {
            co_return std::span<const std::byte>{};
        }
        constexpr size_t chunk_size = 16;
        // We feed the data in small chunks here to test the pipeline.
        const auto n = std::min(chunk_size, data.size() - _position);
        const auto result = std::span<const std::byte>(reinterpret_cast<const std::byte*>(ptr + _position), n);
        _position += n;
        co_return result;
    }

    future<> close() override {
        _storage->flush_read();
        return make_ready_future();
    }
};

// No compression decompressor - passes data further up the pipeline.
class noop_decompressor : public decompression_interface {
    std::unique_ptr<parsing_interface> _parser;
public:
    noop_decompressor(std::unique_ptr<parsing_interface> parser) : _parser(std::move(parser)) {}

    future<> decompress(std::span<const std::byte> data) override {
        co_await _parser->parse(data);
    }
    void cancel() noexcept override {
        _parser->cancel();
    }
    future<> close() override {
        return _parser->close();
    }
};

// Gzip decompressor - decompresses gzip data and passes decompressed chunks to the parser.
class gzip_decompressor : public decompression_interface {
    std::unique_ptr<parsing_interface> _parser;
    z_stream _zs = {};
    // Set when inflate() reports the end of a gzip member, cleared when the next member is started.
    // At close() time it tells a complete object from a truncated one: zlib verifies the CRC32 and
    // ISIZE trailer - gzip's only integrity check - exclusively at the end of a member, so an
    // object that was cut short (an interrupted export, a partial download) decodes as far as it
    // goes and would otherwise be reported as a clean, complete import.
    bool _stream_ended = false;
    // Set by cancel(), so that close() does not report a truncated stream on top of whatever error
    // aborted the import in the first place.
    bool _cancelled = false;

    // True if we have not yet processed any data. Needed to distinguish between an empty stream
    // and a truncated one.
    bool _no_input_yet = true;

    static constexpr size_t _buf_size = 4096;

    // A gzip file may consist of several members concatenated (RFC 1952 2.2) - what pigz,
    // `cat a.gz b.gz` and any producer that compresses in chunks emit. zlib stops at the end of
    // each member and keeps returning Z_STREAM_END without consuming anything until the stream is
    // reset, so continuing into the next member has to be done explicitly.
    void start_next_member() {
        auto res = inflateReset(&_zs);
        if (res != Z_OK) {
            throw std::runtime_error(fmt::format("Alternator S3 import pipeline: gzip decompression error (inflateReset returned {}): {}", res, _zs.msg ? _zs.msg : "<no message>"));
        }
        _stream_ended = false;
    }

public:
    explicit gzip_decompressor(std::unique_ptr<parsing_interface> parser)
        : _parser(std::move(parser)) {
        auto res = inflateInit2(&_zs, 16 + MAX_WBITS);
        if (res != Z_OK) {
            throw std::runtime_error(fmt::format("Alternator S3 import pipeline: gzip decompression error (inflateInit2 returned {}): {}", res, _zs.msg ? _zs.msg : "<no message>"));
        }
    }
    gzip_decompressor(const gzip_decompressor&) = delete;
    gzip_decompressor(gzip_decompressor&&) = delete;
    gzip_decompressor& operator=(const gzip_decompressor&) = delete;
    gzip_decompressor& operator=(gzip_decompressor&&) = delete;

    ~gzip_decompressor() {
        inflateEnd(&_zs);
    }

    future<> decompress(std::span<const std::byte> data) override {
        if (data.empty()) {
            co_return;
        }

        _no_input_yet = false;

        // The previous chunk ended exactly on a member boundary, so this one opens a new member.
        if (_stream_ended) {
            start_next_member();
        }

        _zs.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(data.data()));
        _zs.avail_in = static_cast<uInt>(data.size());

        do {
            std::array<std::byte, _buf_size> output;
            _zs.next_out = reinterpret_cast<Bytef*>(output.data());
            _zs.avail_out = _buf_size;

            int ret = inflate(&_zs, Z_NO_FLUSH);
            if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
                throw api_error::validation(fmt::format("Alternator S3 import pipeline: gzip decompression error: {}", _zs.msg ? _zs.msg : "<no message>"));
            }

            auto produced = _buf_size - _zs.avail_out;
            if (produced > 0) {
                // The loop's preemption point: co_await yields once the task quota is used up, even on a ready future.
                co_await _parser->parse(std::span<const std::byte>(output.data(), produced));
            }

            if (ret == Z_STREAM_END) {
                _stream_ended = true;
                // Anything left in the input buffer belongs to the next member. Resetting only
                // when there is input left keeps an object whose last member ends on a chunk
                // boundary from being restarted into a member that never arrives.
                if (_zs.avail_in == 0) {
                    co_return;
                }
                start_next_member();
            } else if (ret == Z_BUF_ERROR && produced == 0) {
                // No progress is possible - inflate() needs more input than this chunk holds.
                // Every iteration hands zlib an empty _buf_size output buffer, so the only way it
                // can stall is on input, which means it has already taken the whole chunk into its
                // internal state - nothing of `data` is left to carry over, and the next chunk
                // resumes from there. If the object ends here instead, close() reports the
                // truncation. The check guards the carry-over-free assumption: `data` does not
                // outlive this call, so anything zlib left behind would be silently lost.
                if (_zs.avail_in != 0) {
                    on_internal_error(xlogger, "gzip decompression stalled with unconsumed input");
                }
                co_return;
            }
        } while (_zs.avail_in > 0 || _zs.avail_out == 0);
    }
    void cancel() noexcept override {
        inflateEnd(&_zs);
        _zs = {};
        _cancelled = true;
        _parser->cancel();
    }

    future<> close() override {
        // We consider stream of size 0 (_no_input_yet set to true) as valid, not truncated and empty stream
        // (this is done because AWS can produce .gz files of size 0 for empty partitions).
        if (_cancelled || _stream_ended || _no_input_yet) {
            co_return co_await _parser->close();
        }
        // Cancel the parser (to prevent any complaining from it) before throwing the error.
        _parser->cancel();
        throw api_error::validation("Alternator S3 import pipeline: gzip decompression error: truncated gzip stream");
    }
};

// Json parser that accumulates incoming data until it sees a newline character,
// then parses the accumulated line as JSON and invokes the on_item callback with the parsed rjson::value.
// The last line is parsed and sent to callback even if it doesn't end with a newline.
class json_parser : public parsing_interface {
    std::function<future<>(rjson::value)> _on_item;
    std::string _buffer;
public:
    json_parser(std::function<future<>(rjson::value)> on_item) : _on_item(std::move(on_item)) {}
    future<> parse(std::span<const std::byte> data) override {
        static constexpr const size_t maximum_buffer_size_in_mb = 16;

        auto sv = std::string_view(reinterpret_cast<const char*>(data.data()), data.size());
        size_t pos = 0;
        while (pos < sv.size()) {
            auto nl = sv.find('\n', pos);
            if (nl == std::string_view::npos) {
                nl = sv.size();
            }
            if (_buffer.size() + (nl - pos) > maximum_buffer_size_in_mb * 1024 * 1024) {
                throw api_error::payload_too_large(fmt::format("JSON line exceeds maximum buffer size of {} MB", maximum_buffer_size_in_mb));
            }
            _buffer.append(sv.substr(pos, nl - pos));
            // We found a newline character, so we have a complete line to process.
            if (nl < sv.size()) {
                // We ignore empty lines here as a good will (those should not happen in valid JSON input).
                if (!_buffer.empty()) {
                    auto _ = defer([&]() noexcept {
                        _buffer.clear();
                    });
                    co_await _on_item(rjson::parse(_buffer));
                    co_await coroutine::maybe_yield();
                }
            }
            pos = nl + 1;
        }
    }
    void cancel() noexcept override {
        _buffer.clear();
    }
    future<> close() override {
        if (!_buffer.empty()) {
            // Process any remaining data as a final line (even if it doesn't end with a newline).
            auto _ = defer([&]() noexcept {
                _buffer.clear();
            });
            co_await _on_item(rjson::parse(_buffer));
        }
        co_return;
    }
};

// S3 reports the etag as an RFC 9110 entity-tag, i.e. wrapped in double quotes. The manifest
// files report it the way DynamoDB does - as a bare string - so drop the quotes.
// The returned view points into the caller's buffer, which has to outlive it.
static std::string_view strip_etag_quotes(std::string_view etag) {
    if (etag.size() >= 2 && etag.starts_with('"') && etag.ends_with('"')) {
        etag.remove_prefix(1);
        etag.remove_suffix(1);
    }
    return etag;
}

/// Writes data to an S3 object. The data is passed to `s3::client` object, which will upload it to S3 asynchronously.
/// The upload is completed when `flush_and_close()` is called.
class s3_storage_sink : public storage_sink_interface {
    sstring _object_name;
    std::unique_ptr<output_stream<char>> _upload_stream;
    // A future that will produce the ETag value once the underlying sink's flush() resolves.
    // If the upload fails, it resolves exceptionally instead - with the flush()'s exception, or
    // with broken_promise if the sink got closed without ever being flushed. Whenever closing
    // the stream throws, flush_and_close() discards this future - with whatever exception it
    // carries - and propagates the close()'s exception instead, as that is the one describing
    // why the upload failed.
    future<sstring> _etag_future;
    sink_result_builder _result;
    bool _closed = false;

    // The target of the delegating constructor below - the sink and the etag future have to be
    // created before the member initialization starts, as a future has no default constructor.
    // If the allocation of the stream throws, sink_and_etag is destroyed while unwinding. A pair
    // destroys its members in reverse order, so the future goes away first - detaching itself
    // from its promise - and the sink, carrying that promise, only afterwards: nothing is left
    // to wait for and no exception is ever produced. The opposite order would not do: destroying
    // the sink first breaks the promise and leaves an exceptional future to be dropped. The
    // future cannot be ignore_ready_future()-d either: at this point it is still unresolved and
    // ignoring an unresolved future asserts.
    s3_storage_sink(sstring&& object_name, std::pair<data_sink, future<sstring>> sink_and_etag)
        : _object_name(std::move(object_name))
        , _upload_stream(std::make_unique<output_stream<char>>(std::move(sink_and_etag.first)))
        , _etag_future(std::move(sink_and_etag.second))
    {
    }

public:
    // The constructor above takes object_name by rvalue reference, so it stays intact until
    // it runs - the second argument may use it no matter in which order the arguments happen
    // to be evaluated. The client is not kept here: the sink the stream holds keeps it alive
    // for as long as the upload needs it.
    s3_storage_sink(shared_ptr<s3::client> client, sstring object_name, abort_source *as)
        : s3_storage_sink(std::move(object_name),
                          client->make_upload_jumbo_sink_and_get_etag(object_name, s3::object_metadata{}, std::nullopt, as))
    {
    }
    ~s3_storage_sink() {
        if (!_closed) {
            // The stream still holds buffered data and, most likely, a started multipart upload.
            // Getting rid of them is asynchronous (see `upload_sink_base::close()`) and there is
            // nothing left here that could wait for it, while destroying the stream as-is would
            // trip output_stream's own assert. Report the bug and leak the stream instead - the
            // upload keeps the s3::client alive, so nothing dangles, and the operation that failed
            // to close the sink fails on its own, without taking the whole node down with it.
            on_internal_error_noexcept(xlogger, fmt::format("s3_storage_sink {} destroyed without calling flush_and_close() - it will leave leftovers in S3", _object_name));
            [[maybe_unused]] auto* leaked = _upload_stream.release();
            // _etag_future is destroyed unresolved, as the leaked sink never sets its promise.
        }
    }

    future<> write(std::span<const std::byte> data) override {
        if (_closed) {
            on_internal_error(xlogger, fmt::format("s3_storage_sink {} write called after flush_and_close", _object_name));
        }
        // we will return the future from write() directly, no need to add `co_await` here.
        _result.update(data);
        return _upload_stream->write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    future<export_pipeline_interface::result> flush_and_close() override {
        if (_closed) {
            on_internal_error(xlogger, fmt::format("s3_storage_sink {} already closed", _object_name));
        }
        _closed = true;
        try {
            co_await _upload_stream->close();
        }
        catch(...) {
            // We will ignore _etag_future if the upload failed, as the tag holds no interest
            // then - but it has to be ignored explicitly, or seastar reports the exception it
            // carries as an ignored one. It is resolved by now even though close() threw:
            // output_stream::close() closes the sink whether flushing it succeeded or not,
            // and the sink resolves the tag before it finishes closing (see
            // make_upload_jumbo_sink_and_get_etag()).
            _etag_future.ignore_ready_future();
            throw;
        }
        auto etag = co_await std::move(_etag_future);

        auto stripped_etag = strip_etag_quotes(etag);
        // The upload itself succeeded, but S3 reported no tag the client could make sense of
        // (see make_upload_jumbo_sink_and_get_etag()) - the caller is the one expected to check
        // for that. The manifest describing the export has to carry the tag of the object, so
        // an export with no tag to report is of no use to anybody - fail it instead of handing
        // out a result with an empty etag.
        if (stripped_etag.empty()) {
            throw std::runtime_error(fmt::format("S3 reported no usable etag for the uploaded object {}", _object_name));
        }
        co_return _result.finalize(sstring(stripped_etag));
    }
};

/// Reads some data from an S3 object. Returns empty span when the S3 object is fully read.
class s3_storage_source : public source_interface {
    shared_ptr<s3::client> _client;
    sstring _object_name;
    temporary_buffer<char> _buffer;
    std::optional<input_stream<char>> _src;
public:
    s3_storage_source(shared_ptr<s3::client> client, sstring object_name, abort_source *as)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _src(_client->make_chunked_download_source(_object_name, s3::full_range, as))
    {
    }
    ~s3_storage_source() {
        SCYLLA_ASSERT(!_src && "s3_storage_source destroyed without calling close().");
    }

    future<std::span<const std::byte>> read_some() override {
        if (!_src) {
            on_internal_error(xlogger, fmt::format("s3_storage_source {} already closed", _object_name));
        }
        _buffer = co_await _src->read();
        co_return std::span<const std::byte>(reinterpret_cast<const std::byte*>(_buffer.get()), _buffer.size());
    }
    future<> close() override {
        if (_src) {
            auto z = std::exchange(_src, std::nullopt);
            co_await z->close();
        }
    }
};


/// Reads data from a source object (in-memory or S3) and feeds it through a decompression_interface.
/// read_all() streams the entire object, calling decompress() for each chunk.
class import_pipeline_impl : public import_pipeline_interface {
    std::unique_ptr<source_interface> _source;
    std::unique_ptr<decompression_interface> _decompressor;

public:
    import_pipeline_impl(std::unique_ptr<source_interface> source, std::unique_ptr<decompression_interface> decompressor) noexcept
        : _source(std::move(source))
        , _decompressor(std::move(decompressor))
    {
    }
    ~import_pipeline_impl() {
        SCYLLA_ASSERT(!_source && !_decompressor && "import_pipeline_impl destroyed without calling close().");
    }

    future<> read_all() override {
        if (!_source) {
            on_internal_error(xlogger, "import_pipeline_impl read_all() called after source was closed");
        }
        try {
            while (true) {
                auto buf = co_await _source->read_some();
                if (buf.empty()) {
                    break;
                }
                co_await _decompressor->decompress(buf);
            }
        }
        catch(...) {
            _decompressor->cancel();
            throw;
        }
    }
    future<> close() override {
        std::exception_ptr exception;

        if (_source) {
            try {
                auto z = std::exchange(_source, nullptr);
                co_await z->close();
            } catch(...) {
                if (!exception) {
                    exception = std::current_exception();
                }
            }
        }
        if (_decompressor) {
            try {
                auto z = std::exchange(_decompressor, nullptr);
                co_await z->close();
            } catch(...) {
                if (!exception) {
                    exception = std::current_exception();
                }
            }
        }
        if (exception) {
            std::rethrow_exception(std::move(exception));
        }
    }
};

// Note: `sink` is taken over by the created compressor only if the construction succeeds - on failure
// it is left with the caller, which has to close it asynchronously. See gzip_compressor's constructor.
static std::unique_ptr<compression_interface> make_compressor(compression_type compression, std::unique_ptr<storage_sink_interface>&& sink) {
    return std::visit(overloaded_functor{
        [&](const no_compression&) -> std::unique_ptr<compression_interface> {
            return std::make_unique<noop_compressor>(std::move(sink));
        },
        [&](const gzip_compression&) -> std::unique_ptr<compression_interface> {
            return std::make_unique<gzip_compressor>(std::move(sink));
        }
    }, compression);
}

static std::unique_ptr<decompression_interface> make_decompressor(compression_type compression, std::unique_ptr<parsing_interface> parser) {
    return std::visit(overloaded_functor{
        [&](const no_compression&) -> std::unique_ptr<decompression_interface> {
            return std::make_unique<noop_decompressor>(std::move(parser));
        },
        [&](const gzip_compression&) -> std::unique_ptr<decompression_interface> {
            return std::make_unique<gzip_decompressor>(std::move(parser));
        }
    }, compression);
}

static std::unique_ptr<decompression_interface> create_decompression_pipeline(std::function<future<>(rjson::value)> on_item, compression_type compression) {
    auto parser = std::make_unique<json_parser>(std::move(on_item));
    return make_decompressor(compression, std::move(parser));
}

// Factory function to create sink pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory sink pipeline for testing.
// - s3_target_config - pipeline that will write to S3 object.
future<std::unique_ptr<export_pipeline_interface>> create_sink_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, compression_type compression) {
    std::unique_ptr<storage_sink_interface> sink;
    std::unique_ptr<compression_interface> compressor;

    // Unfortunately we can't rely on destructors, as those don't handle exceptions and don't work in asynchronous context.
    // A half-built pipeline has to be torn down explicitly: the stages above the sink can fail to construct
    // (zlib initialization allocates, and so does every `make_unique` here), while an s3_storage_sink
    // destroyed without flush_and_close() leaks its upload stream together with a started multipart
    // upload - see its destructor. Each stage therefore takes the stage below it over only once it can
    // no longer fail, so that whatever the failing step did not take is still owned here and can be closed.
    std::exception_ptr exception;
    try {
        sink = std::visit(overloaded_functor{
            [&](in_memory_target_config &cfg) -> std::unique_ptr<storage_sink_interface> {
                if (!cfg.storage) {
                    on_internal_error(xlogger, "in_memory_target_config::storage is null");
                }
                return std::make_unique<in_memory_storage_sink>(std::move(cfg.storage));
            },
            [&](s3_target_config &cfg) -> std::unique_ptr<storage_sink_interface> {
                if (!cfg.client) {
                    on_internal_error(xlogger, "s3_target_config::client is null");
                }
                return std::make_unique<s3_storage_sink>(std::move(cfg.client), std::move(cfg.object_name), cfg.as);
            }
        }, target_config);
        // json_formatter's own constructor cannot fail, but the allocation which precedes it can - and
        // then the compressor, with the sink already inside it, is still owned by the local below.
        compressor = make_compressor(compression, std::move(sink));
        co_return std::make_unique<json_formatter>(std::move(compressor));
    } catch(...) {
        exception = std::current_exception();
    }

    // Closing the pipeline on this path finalizes the target object, so a failed export can leave an
    // empty object behind - with gzip that is a header-and-trailer-only member, or zero bytes when it was
    // the compressor itself which failed to construct and only the bare sink is closed below - overwriting
    // whatever was stored under that name before. It is the `output_stream` inside s3_storage_sink which
    // forces that: it cannot be disposed of without flushing, and flushing a stream which wrote nothing
    // PUTs an empty object (no multipart upload has been started yet at this point, so there is nothing
    // to abort either). The caller learns about the failure from the exception rethrown below.
    // Note that such an object reads back as a valid, empty export rather than as a failure: a
    // header-and-trailer-only member ends with Z_STREAM_END like any other, and an object with no bytes
    // at all is accepted as an empty stream by gzip_decompressor::close(). The caller must therefore not
    // take the presence of the object for success. See the contract on
    // export_pipeline_interface::flush_and_close() in export.hh.
    if (compressor) {
        // The compressor got the sink, so closing it closes the sink too.
        try {
            auto z = std::exchange(compressor, nullptr);
            co_await z->flush_and_close().discard_result();
        } catch(...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
    } else if (sink) {
        try {
            auto z = std::exchange(sink, nullptr);
            co_await z->flush_and_close().discard_result();
        } catch(...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
    }
    std::rethrow_exception(exception);
}

// Factory function to create source pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory source pipeline for testing.
// - s3_target_config - pipeline that will read from S3 object.
// Note: `on_item` callback must be valid until `import_pipeline_interface::close()` is resolved.
future<std::unique_ptr<import_pipeline_interface>> create_source_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, std::function<future<>(rjson::value)> on_item, compression_type compression) {
    std::unique_ptr<source_interface> source;
    std::unique_ptr<decompression_interface> decompressor;

    // Unfortunately we can't rely on destructors, as those don't handle exceptions and don't work in asynchronous context.
    std::exception_ptr exception;
    try {
        source = std::visit(overloaded_functor{
            [&](in_memory_target_config &cfg) -> std::unique_ptr<source_interface> {
                if (!cfg.storage) {
                    on_internal_error(xlogger, "in_memory_target_config::storage is null");
                }
                return std::make_unique<in_memory_source>(std::move(cfg.storage));
            },
            [&](s3_target_config &cfg) -> std::unique_ptr<source_interface> {
                if (!cfg.client) {
                    on_internal_error(xlogger, "s3_target_config::client is null");
                }
                return std::make_unique<s3_storage_source>(std::move(cfg.client), std::move(cfg.object_name), cfg.as);
            }
        }, target_config);
        decompressor = create_decompression_pipeline(std::move(on_item), compression);

        co_return std::make_unique<import_pipeline_impl>(std::move(source), std::move(decompressor));
    } catch(...) {
        exception = std::current_exception();
    }

    if (source) {
        try {
            co_await source->close();
        } catch(...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
    }
    if (decompressor) {
        try {
            co_await decompressor->close();
        } catch(...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
    }
    std::rethrow_exception(exception);
}

// DynamoDB reports the timestamps of an export as seconds since the epoch, keeping the sub-second
// part.
static double to_epoch_seconds(db_clock::time_point tp) {
    return std::chrono::duration<double>(tp.time_since_epoch()).count();
}

// For the export operations, DynamoDB reports a malformed ARN as ValidationException. parse_arn,
// which other operations also use, reports some malformed ARNs as AccessDeniedException, so this
// function changes that error to ValidationException.
static arn_parts parse_arn_reporting_validation(std::string_view arn, std::string_view arn_field_name, std::string_view type_name, std::string_view expected_postfix) {
    try {
        return parse_arn(arn, arn_field_name, type_name, expected_postfix);
    } catch (const api_error& e) {
        if (e._type == "AccessDeniedException") {
            throw api_error::validation(e._msg);
        }
        throw;
    }
}

// --- Export orchestration helpers ---

// Obtain an S3 client for the given bucket.
static seastar::shared_ptr<s3::client> get_s3_client(service::storage_proxy& proxy, sstring bucket_name) {
    // The bucket is not tied to an endpoint in the configuration, so we require
    // exactly one S3 endpoint in object_storage_endpoints and use it for all buckets.
    auto& sstm = proxy.local_db().get_user_sstables_manager();
    auto endpoints = sstm.endpoints(db::object_storage_endpoint_param::s3_type);
    if (endpoints.size() != 1) {
        throw std::runtime_error(fmt::format("S3 client not configured for bucket '{}': expected exactly one S3 endpoint in object_storage_endpoints, found {}",
                bucket_name, endpoints.size()));
    }
    auto client = sstables::get_s3_client(sstm.get_endpoint_client(endpoints.front()));
    if (!client) {
        on_internal_error(xlogger, fmt::format("object storage endpoint {} is not an S3 endpoint", endpoints.front()));
    }
    return client;
}

// Returns this node's identifier (host_id + gossip_generation), which uniquely identifies a node
// incarnation and changes on reboot (gossip_generation will increase).
// Returns nullopt when the gossiper has no state for this node (before gossiping starts
// or after this node announced its shutdown), i.e. when there's no incarnation to report.
static std::optional<live_node_identifier> get_self_node_id(gms::gossiper& gossiper) {
    auto host_id = gossiper.my_host_id();
    auto ep_state = gossiper.get_this_endpoint_state_ptr();
    if (!ep_state) {
        // The gossiper doesn't know this node yet (or anymore), so there's no
        // incarnation to report.
        return std::nullopt;
    }
    auto generation = ep_state->get_heart_beat_state().get_generation();
    return live_node_identifier{ .host_id = fmt::to_string(host_id), .gossip_generation = std::uint32_t(generation.value()) };
}

// How long a node has to stay unreachable before the garbage collector may take its exports
// over. The failure detector marking a node DOWN says nothing about the export fiber running
// on it: a long reactor stall or a network partition between this node and that one both look
// the same, while the node keeps scanning the table and uploading to S3. Taking such an export
// over would tell the user it failed and let the garbage collector write a terminal row while
// the data and the manifests keep appearing in the bucket, so a node is only given up on once
// it has been unreachable for much longer than any such hiccup lasts. A node which really died
// never comes back with the same incarnation, so all this costs is a delay of the cleanup.
static constexpr std::chrono::hours node_unreachable_timeout{1};

// The nodes which may still be running an export.
struct running_node_ids {
    // Every incarnation the gossiper knows of, except those unreachable for longer than
    // `node_unreachable_timeout`. An incarnation missing from here is gone for good: the node
    // either restarted (and gossips a higher generation now), left the cluster, or has been
    // unreachable for too long - so whatever it was running has stopped.
    std::set<live_node_identifier> incarnations;
    // The nodes whose incarnation the gossiper doesn't know. A restarting node loads its peers
    // from system.peers before gossip starts and holds each of them with generation 0 until it
    // hears from it (see gms::gossiper::add_saved_endpoint()), and a node not heard from yet is
    // not unreachable either, so `node_unreachable_timeout` doesn't cover it. We can't tell
    // which incarnation of such a node is running, so no incarnation of it may be given up on.
    std::unordered_set<sstring> unknown_incarnation;

    // Tells whether `node_id` may still be running an export.
    bool contains(const live_node_identifier& node_id) const {
        return incarnations.contains(node_id) || unknown_incarnation.contains(node_id.host_id);
    }
};

// Returns the nodes which may still be running an export, see running_node_ids.
static future<running_node_ids> get_running_nodes(gms::gossiper& gossiper) {
    running_node_ids result;
    auto timeout = std::chrono::duration_cast<std::chrono::microseconds>(node_unreachable_timeout).count();
    // The callback may not yield - and doesn't.
    gossiper.for_each_endpoint_state([&] (const gms::endpoint_state& ep_state) {
        auto host_id = ep_state.get_host_id();
        // get_endpoint_downtime() reports microseconds, and 0 for a node which is up.
        if (gossiper.get_endpoint_downtime(host_id) > timeout) {
            return;
        }
        auto generation = ep_state.get_heart_beat_state().get_generation();
        if (generation.value() <= 0) {
            // A loaded endpoint state, not one this node gossiped about - the generation is a
            // placeholder, not the generation the node is running with.
            result.unknown_incarnation.insert(fmt::to_string(host_id));
            return;
        }
        result.incarnations.insert(live_node_identifier{ .host_id = fmt::to_string(host_id), .gossip_generation = std::uint32_t(generation.value()) });
    });
    co_return result;
}

// Returns a deep copy of `value` with members of every object sorted
// alphabetically by name. The order of array elements is preserved.
static rjson::value sorted_copy(const rjson::value& value) {
    if (value.IsArray()) {
        rjson::value result = rjson::empty_array();
        for (const auto& element : value.GetArray()) {
            rjson::push_back(result, sorted_copy(element));
        }
        return result;
    }
    if (!value.IsObject()) {
        return rjson::copy(value);
    }
    std::vector<std::string_view> names;
    names.reserve(value.MemberCount());
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
        names.push_back(rjson::to_string_view(it->name));
    }
    std::sort(names.begin(), names.end());

    rjson::value result = rjson::empty_object();
    for (auto& name : names) {
        rjson::add_with_string_name(result, name, sorted_copy(*rjson::find(value, name)));
    }
    return result;
}

sstring canonicalize_request(const rjson::value& request) {
    return rjson::print(sorted_copy(request));
}

// Generate random alphanumeric string of the specified length (in characters).
static sstring generate_random_alphanumeric(size_t length) {
    static thread_local std::mt19937 rng(std::random_device{}());
    static constexpr std::string_view characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    sstring result;
    result.resize(length);
    for (size_t i = 0; i < length; ++i) {
        result[i] = characters[rng() % characters.size()];
    }
    return result;
}

// Generate a unique export ARN.
// Format: arn:scylla:alternator:{keyspace_name}:scylla:table/{table_name}/export/<export id>,
// where the export id has the shape DynamoDB mints it in - see is_valid_export_id().
static sstring generate_export_arn(std::string_view keyspace_name, std::string_view table_name, db_clock::time_point accepted_at) {
    static thread_local std::mt19937 rng(std::random_device{}());
    auto epoch_millis = std::chrono::duration_cast<std::chrono::milliseconds>(accepted_at.time_since_epoch()).count();
    return fmt::format("arn:scylla:alternator:{}:scylla:table/{}/export/{:014d}-{:08x}",
        keyspace_name, table_name, epoch_millis, std::uint32_t(rng()));
}

static std::string_view get_table_arn_from_export_arn(std::string_view export_arn) {
    // Expected format: arn:scylla:alternator:<keyspace>:scylla:table/<name>/export/<id>
    // or AWS format: arn:aws:dynamodb:<region>:<account>:table/<name>/export/<id>
    auto pos = export_arn.find("/export/");
    if (pos == std::string::npos) {
        throw std::invalid_argument(fmt::format("Invalid Export ARN format: {}", export_arn));
    }
    return export_arn.substr(0, pos);
}

// Build the ExportDescription of an export row, the way both DescribeExport and
// ExportTableToPointInTime report it.
static rjson::value build_export_description_response(const export_row& exp) {
    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ExportArn", rjson::from_string(exp.export_arn));
    // DynamoDB reports no ExportStatus but these three, so any other stored status (e.g. FAILING)
    // is reported as IN_PROGRESS.
    rjson::add(export_desc, "ExportStatus", rjson::from_string(exp.export_status == "COMPLETED" || exp.export_status == "FAILED" ? exp.export_status : "IN_PROGRESS"));
    rjson::add(export_desc, "StartTime", rjson::value(to_epoch_seconds(exp.accepted_at)));

    // The fields describing the request are echoed back from the request that started the export,
    // with the ExportFormat ExportTableToPointInTime defaults to. That request was validated when it
    // was accepted, so one that cannot be read back is a corrupt row, like a row missing a column,
    // and not the caller's error.
    try {
        auto exported_request = rjson::parse(exp.request);
        rjson::add(export_desc, "TableArn", rjson::copy(rjson::get(exported_request, "TableArn")));
        rjson::add(export_desc, "S3Bucket", rjson::copy(rjson::get(exported_request, "S3Bucket")));
        if (const rjson::value* s3_prefix = rjson::find(exported_request, "S3Prefix")) {
            rjson::add(export_desc, "S3Prefix", rjson::copy(*s3_prefix));
        }
        const rjson::value* export_format = rjson::find(exported_request, "ExportFormat");
        rjson::add(export_desc, "ExportFormat", export_format ? rjson::copy(*export_format) : rjson::from_string("DYNAMODB_JSON"));
        if (const rjson::value* export_type = rjson::find(exported_request, "ExportType")) {
            rjson::add(export_desc, "ExportType", rjson::copy(*export_type));
        }
    } catch (const rjson::error& e) {
        throw std::runtime_error(fmt::format("Export '{}' has a malformed request: {}", exp.export_arn, e.what()));
    }

    // TableId identifies the table the export was taken from, not whatever table now answers to
    // the export's name - names can be reused, ids cannot. A row whose id is nil doesn't know which
    // table that was - the garbage collector writes such a row when it has to fail an export whose
    // table it cannot find - so it is described without a TableId rather than with a nil one.
    if (exp.table_id) {
        rjson::add(export_desc, "TableId", rjson::from_string(exp.table_id.to_sstring()));
    }
    rjson::add(export_desc, "ExportTime", rjson::value(to_epoch_seconds(exp.export_time)));
    rjson::add(export_desc, "ClientToken", rjson::from_string(exp.client_token));

    // Everything below describes a finished export, so it is only there once one has run.
    if (exp.export_manifest) {
        rjson::add(export_desc, "ExportManifest", rjson::from_string(*exp.export_manifest));
    }
    if (exp.failure_code) {
        rjson::add(export_desc, "FailureCode", rjson::from_string(*exp.failure_code));
    }
    if (exp.failure_message) {
        rjson::add(export_desc, "FailureMessage", rjson::from_string(*exp.failure_message));
    }
    if (exp.item_count) {
        rjson::add(export_desc, "ItemCount", rjson::value(*exp.item_count));
    }
    if (exp.billed_size_bytes) {
        rjson::add(export_desc, "BilledSizeBytes", rjson::value(*exp.billed_size_bytes));
    }
    if (exp.completed_at) {
        rjson::add(export_desc, "EndTime", rjson::value(to_epoch_seconds(*exp.completed_at)));
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    return response;
}

// Does garbage collection for S3 exports and exits.
// A round scans both export tables and then reads and rewrites the rows it takes over, which can
// take a while, so it checks `as` as it goes - stop() waits for the round, and
// shouldn't have to wait for all of it. An abort throws out of here, ending the round between two
// rows rather than in the middle of taking one over; whatever is left is collected in a round of
// some other node, or of this one after a restart.
static future<> garbage_collect_s3_exports(service::storage_proxy& proxy, db::system_distributed_keyspace& sdks,
        gms::gossiper& gossiper, abort_source& as) {
    auto client_tokens = co_await get_all_client_tokens(sdks, &as);
    auto exports = co_await get_all_exports(proxy, service::client_state::for_internal_calls(), nullptr, empty_service_permit(), &as);
    auto running_nodes = co_await get_running_nodes(gossiper);
    auto self_node_id_opt = get_self_node_id(gossiper);
    if (!self_node_id_opt) {
        // This node isn't gossiping, so it can't take over the rows of dead nodes.
        // Another node will do it, or we will retry in the next round.
        xlogger.debug("Garbage collection: this node has no gossip state, skipping the round");
        co_return;
    }
    auto self_node_id = std::move(*self_node_id_opt);

    xlogger.debug("Garbage collection: {} client tokens, {} exports, {} running nodes ({} of unknown incarnation), self node_id={}",
        client_tokens.size(), exports.size(), running_nodes.incarnations.size(), running_nodes.unknown_incarnation.size(), self_node_id);

    // Tells whether whatever `node_id` was running has certainly stopped, so its rows may be
    // taken over. This node's own previous incarnations always qualify - each incarnation
    // gossips under a different generation, so an older one is never among `running_nodes`
    // (and this node is never one of the nodes of unknown incarnation: it knows its own).
    auto is_node_dead = [&](const live_node_identifier& node_id) {
        return !running_nodes.contains(node_id);
    };

    auto node_id_of = [](const export_summary_row& exp) {
        return live_node_identifier::from_string(exp.node_id.value_or(sstring()));
    };

    std::unordered_set<sstring> dead_node_export_arns;
    // Every ARN an export row was scanned for, no matter who owns the row. A client token row keeps
    // the ownership of the node which wrote it even after the export it reserved the ARN for has
    // been taken over, so the "node died before writing the export row" check below can't tell a
    // missing export row from a taken over one by `dead_node_export_arns` alone.
    std::unordered_set<std::string_view> existing_export_arns;
    for (auto& exp : exports) {
        existing_export_arns.insert(exp.export_arn);
        // Only the export rows left behind by a dead node are worth collecting - a row still owned
        // by a running node is being driven by that node.
        if (auto node_id = node_id_of(exp); is_node_dead(node_id)) {
            xlogger.debug("Export row {} is under control of dead node {}", exp.export_arn, node_id);
            dead_node_export_arns.insert(exp.export_arn);
        }
    }
    for(auto &ct : client_tokens) {
        as.check();
        // Likewise, a client token row still owned by a running node is that node's business.
        if (!is_node_dead(ct.node_id)) continue;

        // Handle special case, where node died before writing export row (so it won't show up in `exports`)
        xlogger.debug("Client token row {} is under control of dead node {}", ct.client_token, ct.node_id);
        if (!existing_export_arns.contains(ct.export_arn)) {
            // Node died before writing the export row, let's fill in the missing row with `FAILED` status.
            // The export itself never happened, so there's nothing to clean up here on S3.

            xlogger.debug("Client token row {} has no corresponding export row, creating a FAILED export row", ct.client_token);
            // Parse user request from client token row.
            rjson::value request;
            sstring stored_request = ct.request;
            try {
                request = rjson::parse(ct.request);
            } catch (...) {
                xlogger.debug("Failed to parse request from client token row {}: {}", ct.client_token, ct.request);
                // This can't happen in normal operation.
                // If we can't parse the request, we will invent data on the fly - and store the
                // invented request instead of the unreadable one, so that the FAILED export we
                // are about to write can still be described.
                request = rjson::empty_object();
                std::string_view table_arn;
                try {
                    table_arn = get_table_arn_from_export_arn(ct.export_arn);
                } catch (...) {
                    xlogger.debug("Failed to get the table ARN of client token row {}: {}", ct.client_token, std::current_exception());
                }
                // TableArn and S3Bucket are the two fields an export description can't be built
                // without, so both have to be there even if all we can say is that we don't know.
                rjson::add(request, "TableArn", rjson::from_string(table_arn));
                rjson::add(request, "S3Bucket", rjson::from_string(""));
                stored_request = canonicalize_request(request);
            }

            // The client token row doesn't hold the table id, so look the table up - it may be gone
            // by now, in which case the row keeps a nil id and is described without a TableId.
            ::table_id table_id;
            try {
                const rjson::value* table_arn = rjson::find(request, "TableArn");
                if (table_arn && table_arn->IsString()) {
                    auto parts = parse_arn(rjson::to_string_view(*table_arn), "TableArn", "table", "");
                    if (auto table = proxy.data_dictionary().try_find_table(parts.keyspace_name, parts.table_name)) {
                        table_id = table->schema()->id();
                    }
                }
            } catch (...) {
                xlogger.debug("Failed to find the table of client token row {}: {}", ct.client_token, std::current_exception());
            }

            xlogger.debug("Creating FAILED export row for client token {}: export_arn={}", ct.client_token, ct.export_arn);
            auto now = db_clock::now();
            co_await insert_export(sdks, {
                .export_arn = ct.export_arn,
                .client_token = ct.client_token,
                .request = std::move(stored_request),
                .table_id = table_id,
                .export_status = "FAILED",
                .export_id_token = "",
                .export_time = now,
                .accepted_at = now,
                .node_id = self_node_id,
                .failure_code = "NodeFailure",
                .failure_message = "The node that initiated the export has failed before the export could start.",
                .completed_at = now,
            });
        }
    }

    for (auto& summary : exports) {
        as.check();
        // We're interested in exports on dead nodes or exports on our node with status FAILING
        // The latter case we will check later once we read full export row
        if (dead_node_export_arns.contains(summary.export_arn) || summary.status == "FAILING") {
            auto status = summary.status.value_or(sstring());
            if (status == "COMPLETED" || status == "FAILED") {
                // The export completed - nothing to collect.
                xlogger.debug("Export row {} is already in terminal state {}, skipping", summary.export_arn, status);
                continue;
            }

            // The scan above reads only the columns needed to pick the exports to take over, but
            // update_export() rewrites the whole row, so it has to start from the full one.
            auto row = co_await get_export(proxy, summary.export_arn, empty_service_permit());
            if (!row) {
                // The row expired in the meantime.
                continue;
            }
            const export_row& exp = *row;

            if (!(exp.node_id == self_node_id && exp.export_status == "FAILING")) {
                // update_export() below is conditional on the row's current owner and status, so they have
                // to be checked again - another node may have taken the export over since the scan.
                if (!is_node_dead(exp.node_id) || exp.export_status == "COMPLETED" || exp.export_status == "FAILED") {
                    xlogger.debug("Export row {} changed since the scan (node {}, status {}), skipping", exp.export_arn, exp.node_id, exp.export_status);
                    continue;
                }
            }
            // We get here if node handling the row is dead or it's our row with already set FAILING state.
            // In both cases we take over by first updating `node_id` and `export_status` to show our ownership and a new state,
            // even thought in second case the operation is redundant.

            auto new_export_row = exp;
            new_export_row.export_status = "FAILING";
            new_export_row.node_id = self_node_id;
            xlogger.debug("Marking export row {} as FAILING", exp.export_arn);

            if (!co_await update_export(sdks, new_export_row, exp.export_status, exp.node_id)) {
                // Other node is already handling it and is quicker, so we just skip this.
                xlogger.debug("Failed to update export row {} to FAILING, it may have been updated by another node", exp.export_arn);
                continue;
            }

            // Note: we purposely don't remove whatever the dead node had already written to S3. A failed export never
            // deletes the data it uploaded, so the partial data is left for the user to clean up on their own.

            xlogger.debug("Marking export row {} as FAILED", exp.export_arn);
            new_export_row.failure_code = "NodeFailure";
            new_export_row.failure_message = "The node that was performing the export has failed before the export could finish.";
            new_export_row.completed_at = db_clock::now();
            auto failing_export_status = std::exchange(new_export_row.export_status, "FAILED");
            if (!co_await update_export(sdks, new_export_row, failing_export_status, new_export_row.node_id, export_row_ttl)) {
                // This should never happen:
                // - either there was some write issue and node_id / export_status were not correctly updated, or
                // - another node took over and updated the row to a different status.
                // In both cases we can't do anything about it, so we just skip - either the other node will handle it (case 2) or
                // we will try again on next time of garbage collection.
                xlogger.debug("Failed to update export row {} to FAILED, it may have been updated by another node", exp.export_arn);
            }
        }
    }
    xlogger.debug("Garbage collection: finished");
}

static constexpr std::chrono::hours s3_exports_gc_period{4};

// Error injection, which lets tests decide when the next garbage collection round runs.
// When enabled, the garbage collector waits for a message instead of sleeping for `s3_exports_gc_period`.
static constexpr std::string_view s3_exports_gc_wakeup_injection = "alternator_export_to_s3_gc_wakeup";
// Error injection, which is entered after every garbage collection round. Tests use its
// enter count (`/v2/error_injection/injection/{injection}/enters`) to wait for the round to finish.
static constexpr std::string_view s3_exports_gc_finished_injection = "alternator_export_to_s3_gc_finished";

// Waits until the next garbage collection round of S3 exports should run.
// Returns true if gc was triggered by error injection (ie. we're in a test)
// false otherwise.
// If dont_wait is true, we skip waiting and the round runs immediately - unless the
// wakeup error injection is enabled, in which case we wait for its message as usual:
// a test which took over the garbage collection schedule decides when even this
// round runs.
static future<bool> wait_for_next_s3_exports_gc_round(abort_source& as, bool dont_wait) {
    auto& injector = utils::get_local_injector();
    auto deadline = lowres_clock::now() + (dont_wait ? std::chrono::hours{0} : s3_exports_gc_period);
    for (;;) {
        if (injector.is_enabled(s3_exports_gc_wakeup_injection)) {
            // Messages are not shared, so every message triggers exactly one round
            // (and a message sent before we started waiting is not lost).
            co_await injector.inject(s3_exports_gc_wakeup_injection,
                    utils::wait_for_message(s3_exports_gc_period, &as), false);
            co_return true;
        }
        auto now = lowres_clock::now();
        if (now >= deadline) {
            co_return false;
        }
        // A garbage collector, which is already sleeping, has to notice
        // `s3_exports_gc_wakeup_injection` being enabled, so a test can take over the
        // garbage collection schedule at any moment. Rather than waking up periodically
        // to check, we have the injector abort the sleep for us. Without error injection
        // compiled in registering the callback is a no-op, so the whole period is slept
        // through at once.
        abort_source wakeup_injection_enabled;
        auto abort_subscription = utils::chain_abort_source(wakeup_injection_enabled, as);
        // The name is materialized up front, so that unregistering (which runs while the scope
        // unwinds, from a destructor) doesn't have to allocate and can't throw.
        sstring wakeup_injection_name(s3_exports_gc_wakeup_injection);
        injector.register_on_enable(wakeup_injection_name, [&wakeup_injection_enabled] () noexcept {
            wakeup_injection_enabled.request_abort();
        });
        auto unregister = defer([&injector, &wakeup_injection_name] () noexcept {
            injector.unregister_on_enable(wakeup_injection_name);
        });
        try {
            co_await seastar::sleep_abortable(deadline - now, wakeup_injection_enabled);
        } catch (const abort_requested_exception&) {
            // Either we are stopping, in which case the exception is rethrown here,
            // or the wakeup injection has just been enabled and the next iteration
            // starts waiting for its messages.
            as.check();
        }
    }
}

future<> executor::s3_exports::stop() {
    _as.request_abort();
    co_await _gate.close();
}

future<> executor::s3_exports::run_garbage_collector() {
    auto holder = _gate.try_hold();
    if (!holder) {
        // The executor is stopping.
        co_return;
    }
    bool first_time = true;
    while (!_as.abort_requested()) {
        try {
            auto triggered_by_injection = co_await wait_for_next_s3_exports_gc_round(_as, first_time);
            auto notify = defer([triggered_by_injection] () noexcept {
                if (triggered_by_injection) {
                    utils::get_local_injector().enter(s3_exports_gc_finished_injection);
                }
            });
            co_await garbage_collect_s3_exports(_e._proxy, _e._sdks, _e._gossiper, _as);
        } catch (const abort_requested_exception&) {
            break;
        } catch (...) {
            xlogger.warn("Garbage collection of S3 exports failed: {}", std::current_exception());
        }
        first_time = false;
    }
}

future<executor::request_return_type> executor::export_table_to_point_in_time(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.export_table_to_point_in_time++;

    // Required parameter
    auto table_arn = get_non_empty_string_attribute(request, "TableArn");

    // Validate that the table exists
    arn_parts parts;
    try {
        parts = parse_arn_reporting_validation(table_arn, "TableArn", "table", "");
    }
    catch (api_error& e) {
        co_return std::move(e);
    }
    maybe_audit(audit_info, audit::statement_category::QUERY, parts.keyspace_name, parts.table_name, "ExportTableToPointInTime", request);

    if (!parts.keyspace_name.starts_with(executor::KEYSPACE_NAME_PREFIX)) {
        co_return api_error::table_not_found(
                fmt::format("TableArn: Invalid table ARN `{}` - not found", table_arn));
    }

    schema_ptr schema;
    try {
        schema = _proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
    } catch (const data_dictionary::no_such_column_family&) {
        co_return api_error::table_not_found(
                fmt::format("TableArn: Invalid table ARN `{}` - not found", table_arn));
    }
    get_stats_from_schema(_proxy, *schema)->api_operations.export_table_to_point_in_time++;

    // Required parameter
    auto s3_bucket = get_non_empty_string_attribute(request, "S3Bucket");

    // Optional parameters
    auto s3_prefix = get_non_empty_string_attribute(request, "S3Prefix", "");

    // AWS checks client token duplication first, so we will validate those later
    auto export_format = get_non_empty_string_attribute(request, "ExportFormat", "DYNAMODB_JSON");
    auto export_type = get_non_empty_string_attribute(request, "ExportType", "");

    // ExportTime - only "now" (or close to now) is supported
    // If not specified, use current time. If specified, must be within 5 minutes of now.
    auto now = to_epoch_seconds(db_clock::now());
    auto export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    auto user_client_token = get_non_empty_string_attribute(request, "ClientToken", "");

    auto node_id_opt = get_self_node_id(_gossiper);
    if (!node_id_opt) {
        // Without a node identifier we can't claim the ownership of the export.
        co_return api_error::internal("This node is not gossiping, so it can't start an export. Please retry the operation.");
    }
    auto node_id = std::move(*node_id_opt);

    // Canonicalize the request for idempotency checking. The ClientToken is the key the request is
    // reserved under rather than a parameter of the export, so it is left out of the canonical
    // form: a request which carried none is answered with the token generated for it, and a retry
    // which passes that token back has to compare equal to the request which minted it.
    auto request_to_compare = rjson::copy(request);
    rjson::remove_member(request_to_compare, "ClientToken");
    auto canonical_request = canonicalize_request(request_to_compare);

    // A request without a ClientToken gets a random one, reported back to the caller just like a
    // supplied one - and usable to retry the request, so it is stored exactly as a supplied token
    // is. A supplied token colliding with a generated one is as unlikely as two callers picking
    // the same token, and is answered the same way: as a retry of the request which got there first.
    sstring client_token = user_client_token.empty()
        ? generate_random_alphanumeric(16)
        : sstring(user_client_token);

    // Generate export_id_token (random identifier used in S3 key paths)
    auto export_id_token = generate_random_alphanumeric(16);

    auto accepted_at = db_clock::now();

    // Generate a unique export ARN
    auto export_arn = generate_export_arn(parts.keyspace_name, parts.table_name, accepted_at);

    // Build the S3 key prefix for this export
    auto s3_key_prefix = s3_prefix.empty()
        ? fmt::format("AWSDynamoDB/{}/", export_id_token)
        : fmt::format("{}/AWSDynamoDB/{}/", s3_prefix, export_id_token);

    auto build_export_row = [&](const sstring& arn) {
        return export_row{
            .export_arn = arn,
            .client_token = client_token,
            .request = canonical_request,
            .table_id = schema->id(),
            .export_status = "IN_PROGRESS",
            .export_id_token = export_id_token,
            .export_time = db_clock::time_point(std::chrono::round<db_clock::duration>(std::chrono::duration<double>(export_time))),
            .accepted_at = accepted_at,
            .node_id = node_id,
        };
    };
    // The row a failure below is reported with. It is built again, with the final ExportTime, once
    // the parameters validated after the ClientToken check are known to be good - but a failure can
    // happen before that, so a row to fail with has to exist from the first write on.
    auto erow = build_export_row(export_arn);

    // Reserve a slot for the export fiber before anything is written: an export scans the whole
    // table and holds an S3 multipart upload open for as long as it runs, so only so many of them
    // may run at once - DynamoDB caps concurrent exports too. Taking the slot here, ahead of the
    // first write, keeps a rejected request from leaving a client token row or an export row
    // behind; the slot is given back by every path below which does not reach start_export().
    // It also means that while this shard is full, a retry of a ClientToken whose export is
    // already running elsewhere is answered with the error rather than with that export's
    // description - the client retries this error just the same.
    auto export_slot = _s3_exports.try_reserve_export_slot();
    if (!export_slot) {
        co_return api_error::limit_exceeded(fmt::format(
                "Too many exports are already running - at most {} of them may run at once on one shard "
                "(configuration variable alternator_max_concurrent_exports_per_shard). Please retry the operation later.",
                _proxy.local_db().get_config().alternator_max_concurrent_exports_per_shard()));
    }

    // We have all we need, let's publish the export request to the database.
    // We need to do this in two steps - first insert a client token row, then insert the export metadata row.
    // The order matters, because it's possible that another node is running the export with the same client token,
    // in which case we need to handle one of such requests as duplicate (we can't start two exports with the same client token).
    // We check for client token in client token table first (and we insert the row first as well),
    // only one node can succeed in inserting the client token row (insert-if-not-exists mechanic) and that node will
    // continue to run the export.
    //
    // Everything from the client token insert on runs in a try: a client token row reserving
    // `export_arn` with no export row for it is answered with IN_PROGRESS by every retry of this
    // ClientToken until the row expires, while DescribeExport knows nothing about that ARN. The
    // garbage collector writes the missing export row only for the client token rows of dead nodes,
    // so as long as this node lives, it is this request which has to write the FAILED export row
    // for the ARN it reserved.
    // `insert_client_row()` itself is inside, because a write which times out may still apply: when
    // it throws, we have to assume the reservation is there.
    //
    // `own_export_arn` says whether a failure below has to be answered that way, i.e. whether
    // `erow`'s ARN is this request's to fail. That is not the same as having inserted the client
    // token row: when the insert loses to a row which has expired by the time we read it, this
    // request goes on to insert the export row for the ARN it generated, and from that insert on
    // the ARN is again ours to report as FAILED - with no client token row to make the garbage
    // collector ever do it for us.
    bool own_export_arn = true;
    bool client_token_inserted = true;
    std::optional<api_error> error;

    try {
        client_token_inserted = co_await insert_client_row(_sdks, client_row{
            .client_token = client_token,
            .export_arn = export_arn,
            .request = canonical_request,
            .node_id = node_id,
        });

        utils::get_local_injector().inject("alternator_export_to_s3_after_client_token_insertion",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_after_client_token_insertion"); });

        if (!client_token_inserted) {
            // Until the export row below is inserted, the only reservation of an ARN around is
            // somebody else's, for an ARN of their own.
            own_export_arn = false;
            // Export with this client token already exists. We need to check if request is the same or different.
            // If the same - return the export is in progress (or completed or failed - the state is in the row itself) as if DescribeExport was called.
            // If not the same - return export conflict error as Amazon requires us to do.

            xlogger.debug("ClientToken {} already exists", client_token);
            auto existing_client_row = co_await get_client_row(_proxy, client_token, permit);

            // if existing_client_row is false, it means the row is gone now (probably TTL collected),
            // so we behave as if it never existed and continue normally.
            if (existing_client_row) {
                auto& client_row = *existing_client_row;
                if (canonical_request != client_row.request) {
                    // Easy - different request, so we fail with export conflict as Amazon requires.
                    xlogger.debug("Export conflict: different parameters");
                    throw api_error::export_conflict("Duplicate request detected - an export with this ClientToken already exists with different parameters");
                }

                // We need an export row for this client token - the export might be in progress or completed or failed.
                auto export_row = co_await get_export(_proxy, client_row.export_arn, permit);
                xlogger.debug("Export row for ClientToken {}: {}", client_token, export_row ? "found" : "not found");
                if (!export_row) {
                    // Export row doesn't exist. Two possibilities:
                    // - the other node is doing the export in the same moment and hasn't inserted the export row yet (but managed
                    //   to insert client token row first), or
                    // - the other node doing the export died before inserting the export row, leaving a dangling client token row
                    //   (it's possible the node is gone but we don't know it yet, so we can't be sure).
                    // In both cases we will return IN_PROGRESS status - the dangling client token row will be taken care of by
                    // garbage collection thread.
                    // The reported ARN must be the one from the client token row, not the one we generated for this
                    // request: the latter will never get an export row, so a later DescribeExport on it would fail and
                    // every retry would report a different ARN, defeating the purpose of the ClientToken. The ARN from
                    // the client token row is the one the owner (or, if it died, the garbage collector) writes the
                    // export row with.
                    xlogger.debug("Returning IN_PROGRESS status for ClientToken {}", client_token);
                    export_row = build_export_row(client_row.export_arn);
                }
                else {
                    // Export row exists, other node is (or was) running the export. Return as if DescribeExport was called.
                    xlogger.debug("Returning existing export description for ClientToken {}", client_token);
                }
                auto response = build_export_description_response(*export_row);
                co_return rjson::print(std::move(response));
            }
        }

        // We need to validate those here (after ClientToken check), because that replicates order of errors
        // returned by DynamoDB (in other words in such case DynamoDB will first check if ClientToken is being
        // reused or not and then check `export_format` and `export_type` fields).
        if (export_format != "DYNAMODB_JSON") {
            throw api_error::validation(
                    fmt::format("ExportFormat attribute: must be DYNAMODB_JSON, not `{}`", export_format));
        }

        if (!export_type.empty() && export_type != "FULL_EXPORT") {
            throw api_error::validation(
                    fmt::format("ExportType attribute: must be FULL_EXPORT, not `{}`", export_type));
        }

        constexpr std::array unsupported_parameters{
                "IncrementalExportSpecification",
                "S3BucketOwner",
                "S3SseAlgorithm",
                "S3SseKmsKeyId",
        };
        for (auto name : unsupported_parameters) {
            if (rjson::find(request, name)) {
                throw api_error::validation(fmt::format("{} attribute is not supported", name));
            }
        }

        if (export_time_v) {
            if (!export_time_v->IsNumber()) {
                throw api_error::validation("Expected a number attribute ExportTime");
            }
            export_time = export_time_v->GetDouble();
            if (std::isnan(export_time) || std::isinf(export_time) || export_time < 0) {
                throw api_error::invalid_export_time("ExportTime number is out of range of valid values for this field");
            }
            auto diff = export_time > now ? export_time - now : now - export_time;
            if (diff > 300) {
                throw api_error::invalid_export_time(fmt::format("ExportTime must be within 5 minutes of current time. "
                                    "ExportTime: {} s , current time: {} s", export_time, now));
            }
            // DynamoDB rounds a requested ExportTime to the nearest millisecond.
            export_time = std::chrono::duration<double>(std::chrono::round<std::chrono::milliseconds>(
                    std::chrono::duration<double>(export_time))).count();
        }

        // ExportTime is final only now, after the validation above, so build the row to insert (and,
        // should anything below fail, to report the failure with) again.
        erow = build_export_row(export_arn);

        // The export row written below is this request's own, whatever became of the client token
        // row, so from the insert on a failure is ours to report as FAILED. The insert is already
        // inside, because a write which times out may still apply.
        own_export_arn = true;
        auto export_row_inserted = co_await insert_export(_sdks, erow);

        utils::get_local_injector().inject("alternator_export_to_s3_after_export_row_insertion",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_after_export_row_insertion"); });

        if (!export_row_inserted) {
            // An export row for this ARN already exists, even though the ARN was just minted for this
            // request and the client token row reserving it was inserted by us. The garbage collector
            // is the only other writer of such a row: it reads the client token rows, the export rows
            // and the liveness of the nodes one after another, so if gossip marks this node down in
            // that window, it takes the client token row we just wrote for one left behind by a dead
            // node and writes a `FAILED` export row for the ARN reserved in it.
            // The row is now owned by the collector, not by us - running the export would write to S3
            // behind a row saying the export failed, and every update of the row would be rejected
            // anyway (the status and the owner no longer match what we expect). So report the row as
            // it stands, exactly as DescribeExport on this ARN would.
            xlogger.debug("Export row for ClientToken {} and ExportArn {} already exists, not starting the export",
                    client_token, export_arn);
            auto existing_export_row = co_await get_export(_proxy, export_arn, permit);
            if (!existing_export_row) {
                // The row is gone again (its TTL expired between the failed insert and this read), so
                // there is nothing to describe. Nothing was exported, so the request can be retried.
                throw api_error::internal(fmt::format(
                        "Failed to insert the export row for ExportArn {}. Please retry the operation.", export_arn));
            }
            auto response = build_export_description_response(*existing_export_row);
            co_return rjson::print(std::move(response));
        }

        xlogger.debug("Launching background export fiber for ClientToken {}", client_token);
        _s3_exports.start_export(std::move(*export_slot), schema, erow, table_arn, s3_bucket, s3_prefix, s3_key_prefix, export_id_token);

        // Build the ExportDescription response
        xlogger.debug("Returning IN_PROGRESS response for ClientToken {}", client_token);
        auto response = build_export_description_response(erow);
        co_return rjson::print(std::move(response));
    }
    catch(const api_error &e) {
        if (!own_export_arn) throw;
        // We will rethrow this later on, but we have an export row already inserted
        // and now we need to update it with FAILED state.
        error = e;
        erow.failure_code = "Exception";
        erow.failure_message = e.what();
    }
    catch(std::exception &e) {
        if (!own_export_arn) {
            // The client token row is somebody else's reservation of a different ARN, and `erow` is
            // an ARN nothing points to - there is no export of ours to report as failed.
            throw;
        }
        erow.failure_code = "Exception";
        erow.failure_message = e.what();
    } catch(...) {
        if (!own_export_arn) {
            throw;
        }
        erow.failure_code = "Exception";
        erow.failure_message = "Unknown error";
    }
    erow.export_status = "FAILED";
    erow.completed_at = db_clock::now();
    try {
        auto updated = co_await update_export(_sdks, erow, "IN_PROGRESS", node_id, export_row_ttl);
        if (!updated) {
            // The failure may have come before the export row was written (the insert threw without
            // writing anything, or never ran), in which case there is no row to update and the update
            // call will fail. In this case we will insert row ourselves.
            updated = co_await insert_export(_sdks, erow);
        }
        if (!updated) {
            xlogger.debug("Export {} failed to update status to FAILED after completion", erow.export_arn);
        }
        else {
            xlogger.debug("Export {} failed during starting up phase", erow.export_arn);
        }
    } catch (...) {
        // Marking the export FAILED is the one write which cannot be given up on - see
        // write_final_export_row(). This request can't wait for it, having its answer already, so
        // hand the row over to a background fiber which keeps retrying it.
        xlogger.warn("Export {}: failed to write the FAILED state, retrying in the background: {}",
                erow.export_arn, std::current_exception());
        _s3_exports.finish_export_in_background(erow);
    }
    if (error) throw std::move(*error);
    auto response = build_export_description_response(erow);
    co_return rjson::print(std::move(response));
}

// DynamoDB mints an export id as the start time of the export in milliseconds since the epoch,
// padded to 14 digits, a dash, and 8 hex digits (e.g. `01695353076000-06e2188f`), and answers an
// id of any other shape with ValidationException instead of looking it up. generate_export_arn()
// mints ids of the same shape, so Alternator can check them the same way.
static bool is_valid_export_id(std::string_view export_id) {
    static constexpr size_t digit_count = 14;
    static constexpr size_t hex_digit_count = 8;
    if (export_id.size() != digit_count + 1 + hex_digit_count || export_id[digit_count] != '-') {
        return false;
    }
    auto is_digit = [] (char c) { return c >= '0' && c <= '9'; };
    auto is_hex_digit = [&] (char c) { return is_digit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'); };
    return std::ranges::all_of(export_id.substr(0, digit_count), is_digit)
            && std::ranges::all_of(export_id.substr(digit_count + 1), is_hex_digit);
}

// An export ARN is a table ARN followed by `/export/<id>`.
static arn_parts parse_export_arn(std::string_view export_arn, std::string_view arn_field_name) {
    static constexpr std::string_view export_infix = "/export/";
    auto parts = parse_arn_reporting_validation(export_arn, arn_field_name, "Export", export_infix);
    // parse_arn accepts any export id, including an empty one.
    auto export_id = parts.postfix.substr(export_infix.size());
    if (!is_valid_export_id(export_id)) {
        throw api_error::validation(fmt::format("{}: Invalid Export ARN `{}` - `{}` after `{}` is not an export id",
                arn_field_name, export_arn, export_id, export_infix));
    }
    return parts;
}

future<executor::request_return_type> executor::describe_export(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_export++;

    // Parsing the export ARN also tells us which table the export belongs to.
    auto export_arn = get_non_empty_string_attribute(request, "ExportArn");
    auto parts = parse_export_arn(export_arn, "ExportArn");

    maybe_audit(audit_info, audit::statement_category::QUERY, parts.keyspace_name, parts.table_name, "DescribeExport", request);

    // The exported table may have been dropped after the export was accepted, and DynamoDB keeps
    // describing such exports. Per-table metrics live on the table object, so they only exist for
    // as long as it does, and only Alternator's own tables get them.
    if (parts.keyspace_name.starts_with(executor::KEYSPACE_NAME_PREFIX)) {
        if (auto table = _proxy.data_dictionary().try_find_table(parts.keyspace_name, parts.table_name)) {
            get_stats_from_schema(_proxy, *table->schema())->api_operations.describe_export++;
        }
    }

    auto exp = co_await get_export(_proxy, sstring(export_arn), std::move(permit));
    // DynamoDB answers ExportNotFoundException also when the ARN names a table it does not have,
    // but ValidationException when no table can have the name the ARN gives.
    if (!exp) {
        if (parts.keyspace_name.empty()) {
            co_return api_error::validation(fmt::format("ExportArn: Invalid Export ARN `{}` - empty keyspace name", export_arn));
        }
        validate_table_name(parts.table_name, "ExportArn: Invalid Export ARN - table name");
        co_return api_error::export_not_found(fmt::format("ExportArn: Export `{}` not found", export_arn));
    }
    // The statement creating the export row must fill all of these in, so a row missing any of them is corrupt.
    // Note: table_id is not among them - it's possible in rare cases for it to be nil (garbage collector
    // can insert a FAILED row with such table_id if it cannot find a table) - this is fine, because the row
    // is "completed" and build_export_description_response() describes it without a TableId.
    for (auto [column, missing] : {
            std::pair{"client_token", exp->client_token.empty()},
            std::pair{"request", exp->request.empty()},
            std::pair{"export_status", exp->export_status.empty()},
            std::pair{"export_time", exp->export_time == db_clock::time_point{}},
            std::pair{"accepted_at", exp->accepted_at == db_clock::time_point{}}}) {
        if (missing) {
            throw std::runtime_error(format("Export '{}' has no {} in {}.{}", export_arn, column,
                    db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS));
        }
    }

    co_return rjson::print(build_export_description_response(*exp));
}

// Consistency level of the scan: LOCAL_QUORUM for NetworkTopologyStrategy, QUORUM otherwise
// (other strategies may have no replicas in the local DC). With a single token owner in the
// local DC / cluster, LOCAL_ONE / ONE.
//
// FIXME: based on expiration_consistency_level() from #32200. Merge the two as a followup.
static db::consistency_level scan_consistency_level(const locator::token_metadata& tm,
        const locator::effective_replication_map& erm) {
    const auto& rs = erm.get_replication_strategy();
    if (rs.get_type() != locator::replication_strategy_type::network_topology) {
        return tm.count_normal_token_owners() > 1
            ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    }
    const auto& local_dc = tm.get_topology().get_datacenter();
    const auto dc_token_owners = tm.get_datacenter_token_owners();
    const auto it = dc_token_owners.find(local_dc);
    if (it != dc_token_owners.end() && it->second.size() == 1) {
        return db::consistency_level::LOCAL_ONE;
    }
    return db::consistency_level::LOCAL_QUORUM;
}

seastar::future<> export_scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    abort_source& as,
    service_permit permit,
    seastar::noncopyable_function<seastar::future<>(rjson::value)> cb)
{
    as.check();

    // Run as a maintenance job, so the scan doesn't compete with user traffic. Remote replicas
    // serve it via the `$maintenance` RPC tenant (in the streaming group).
    // A group of its own next to streaming in the maintenance supergroup: with equal CPU shares,
    // an hours-long scan cannot starve streaming on this shard.
    // Switch before the first read - the pager computes the page byte limit from the current group.
    co_await coroutine::switch_to(proxy.local_db().get_export_import_scheduling_group());

    // Build a wildcard selection (SELECT *) for all columns.
    auto selection = cql3::selection::selection::wildcard(schema);

    // The wildcard selection expects every column, static ones included.
    auto regular_columns =
        schema->regular_columns()
        | std::views::transform(&column_definition::id)
        | std::ranges::to<query::column_id_vector>();
    auto static_columns =
        schema->static_columns()
        | std::views::transform(&column_definition::id)
        | std::ranges::to<query::column_id_vector>();

    // `allow_short_read` lets the byte and tombstone limits end a page early (the pager sets
    // it too, but the scan relies on it). `bypass_cache` avoids evicting user data from cache.
    // Same as the TTL scanner (`ttl.cc`).
    query::partition_slice::option_set opts = selection->get_query_options();
    opts.set<query::partition_slice::option::allow_short_read>();
    opts.set<query::partition_slice::option::bypass_cache>();

    // Scan all clustering ranges (no restriction).
    std::vector<query::clustering_range> ck_bounds{
        query::clustering_range::make_open_ended_both_sides()};

    auto partition_slice = query::partition_slice(
        std::move(ck_bounds), std::move(static_columns), std::move(regular_columns), opts);

    // No authorization check here - the caller must do it.
    auto& client_state = service::client_state::for_internal_calls();
    tracing::trace_state_ptr trace_state;
    service::query_state query_state(client_state, trace_state, std::move(permit));

    // The scan runs for hours, so don't hold the token metadata / replication map past this.
    const db::consistency_level cl = [&] {
        auto tm = proxy.get_token_metadata_ptr();
        auto erm = schema->table().get_effective_replication_map();
        // With RF=0 a read silently returns an empty result (quorum of zero replicas),
        // so the export would "succeed" with no items.
        if (erm->get_replication_strategy().get_type() == locator::replication_strategy_type::network_topology) {
            const auto& local_dc = tm->get_topology().get_datacenter();
            if (db::local_quorum_for(*erm, local_dc) == 0) {
                throw api_error::validation(fmt::format(
                        "Cannot export {}.{}: keyspace has no replicas in the local datacenter {}",
                        schema->ks_name(), schema->cf_name(), local_dc));
            }
        } else if (db::quorum_for(*erm) == 0) {
            throw api_error::validation(fmt::format(
                    "Cannot export {}.{}: keyspace has no replicas (replication factor 0)",
                    schema->ks_name(), schema->cf_name()));
        }
        return scan_consistency_level(*tm, *erm);
    }();

    // Without a tombstone limit a long run of tombstones could time out every attempt and
    // the scan would never progress. `storage_proxy::get_tombstone_limit()` disables the limit
    // for internal groups (ours included), so read the config directly. Read per pager, so
    // live config updates apply.
    auto tombstone_limit = [&proxy] {
        return proxy.features().empty_replica_pages
                ? query::tombstone_limit(proxy.local_db().get_config().query_tombstone_page_limit())
                : query::tombstone_limit::max;
    };

    // Every page (and every retry) uses a new pager, resumed from the last good paging state:
    //  - a pager consumes its ranges while fetching, so a retry on it could query an empty
    //    range, be reported as "exhausted" and silently skip the rest of the table,
    //  - a pager keeps its command's `timestamp`, so on a long scan rows with expired TTL
    //    would be exported as live.
    // The pager refers to the query options, so it's destroyed first.
    std::unique_ptr<cql3::query_options> query_options;
    std::unique_ptr<service::pager::query_pager> pager;
    auto make_pager = [&] (lw_shared_ptr<const service::pager::paging_state> paging_state) {
        pager.reset();
        query_options = std::make_unique<cql3::query_options>(
            std::make_unique<cql3::query_options>(cl, std::vector<cql3::raw_value>{}),
            std::move(paging_state));

        // Fresh command with a fresh `timestamp`. `max_result_size` is recomputed by the pager
        // before every page.
        auto command = ::make_lw_shared<query::read_command>(
            schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice),
            tombstone_limit());

        // Scan the full token ring.
        dht::partition_range_vector partition_ranges{
            dht::partition_range::make_open_ended_both_sides()};

        pager = service::pager::query_pagers::pager(
            proxy, schema, selection, query_state, *query_options,
            std::move(command), std::move(partition_ranges), nullptr);
    };

    // Paging state of the last successfully read page (null - start from the beginning).
    lw_shared_ptr<const service::pager::paging_state> last_page_state = nullptr;
    bool exhausted = false;

    // Row limit of a page. Doubles after each fast page (up to `unlimited_page_size`, where only
    // the 1 MiB byte limit applies) and halves after each failed attempt (down to one row).
    // Kept across pages, as the cluster's condition carries over.
    static constexpr uint32_t unlimited_page_size = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t min_page_size = 1;
    // Start small - a too large first page would just cost a failed attempt.
    static constexpr uint32_t initial_page_size = 128;
    uint32_t page_size = initial_page_size;
    // With `unlimited_page_size`, halving starts from the row count of the last page instead
    // (or from `assumed_page_rows` if that page was empty).
    uint32_t last_page_rows = 0;
    static constexpr uint32_t assumed_page_rows = 1000;

    // Give up after this many read failures of a single-row page (several minutes of retries).
    static constexpr int max_smallest_page_failures = 10;

    // Failed attempts are logged as warnings only once the page shrank this much (a run of
    // failures), otherwise at debug level.
    static constexpr uint32_t warn_page_size = 16;
    auto retry_log_level = [&page_size] {
        return page_size <= warn_page_size ? logging::log_level::warn : logging::log_level::debug;
    };

    while (!exhausted) {
        as.check();
        std::unique_ptr<cql3::result_set> rs;

        make_pager(last_page_state);

        // Exponential backoff between attempts at the same page.
        static constexpr auto initial_retry_delay = std::chrono::seconds(1);
        static constexpr auto max_retry_delay = std::chrono::seconds(30);
        auto retry_delay = initial_retry_delay;
        // Read failures of this page at `min_page_size`.
        int smallest_page_failures = 0;

        // Transient errors are retried without limit, as a failed scan can't be resumed - stop
        // it via `as`. The only exception is a read failure persisting at `min_page_size`.
        // Errors not caught below fail the scan.
        for (int attempts = 1; ; ++attempts) {
            // Short deadline (`alternator_timeout_in_ms`) - it bounds how long an abort can go
            // unnoticed, and a retry is cheap.
            auto timeout = executor::default_timeout();
            auto started = db::timeout_clock::now();
            try {
                rs = co_await pager->fetch_page(page_size, gc_clock::now(), timeout);
                last_page_rows = static_cast<uint32_t>(rs->size());
                // Fast read - grow the page.
                if (page_size != unlimited_page_size
                        && (db::timeout_clock::now() - started) * 4 < timeout - started) {
                    page_size = page_size > unlimited_page_size / 2
                            ? unlimited_page_size : page_size * 2;
                }
                break;
            } catch(exceptions::read_timeout_exception&) {
                xlogger.log(retry_log_level(),
                        "S3 export scanner read timed out (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            } catch(exceptions::read_failure_exception_with_timeout&) {
                // A closed connection, not a replica failure - treat it as a timeout.
                xlogger.log(retry_log_level(),
                        "S3 export scanner lost the connection to a replica (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            } catch(exceptions::read_failure_exception&) {
                // A smaller page may still succeed. Failures are counted, because reaching
                // `min_page_size` alone doesn't mean a run of failures (e.g. one halving from
                // a 2-row page of 400 KB items gets there).
                if (page_size <= min_page_size
                        && ++smallest_page_failures >= max_smallest_page_failures) {
                    xlogger.error("S3 export scanner read failed on a replica {} times (attempt "
                            "{}) with the smallest possible page, giving up: {}",
                            smallest_page_failures, attempts, std::current_exception());
                    // Prefer reporting an abort requested during the read.
                    as.check();
                    throw;
                }
                xlogger.log(retry_log_level(),
                        "S3 export scanner read failed on a replica (attempt {}), "
                        "retrying with a smaller page: {}",
                        attempts, std::current_exception());
            } catch(exceptions::unavailable_exception&) {
                xlogger.log(retry_log_level(),
                        "S3 export scanner read found too few replicas available (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            } catch(exceptions::overloaded_exception&) {
                xlogger.log(retry_log_level(),
                        "S3 export scanner read was rejected, the node is overloaded (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            }
            // Prefer reporting an abort requested during the read.
            as.check();
            std::chrono::milliseconds delay = retry_delay;
            // Tests shorten the backoff to reach the give-up path quickly.
            if (auto ms = utils::get_local_injector().inject_parameter<uint32_t>("export_scan_retry_delay_ms")) {
                delay = std::chrono::milliseconds(*ms);
            }
            try {
                co_await seastar::sleep_abortable(delay, as);
            } catch (const seastar::sleep_aborted&) {
                // report the abort source's exception, not sleep_abortable()'s own one
                as.check();
                throw;
            }
            retry_delay = std::min(retry_delay * 2, max_retry_delay);
            // Halve the page size (see `last_page_rows`).
            uint32_t halve_from = page_size;
            if (page_size == unlimited_page_size) {
                halve_from = last_page_rows > 0 ? last_page_rows : assumed_page_rows;
            }
            page_size = std::max(min_page_size, halve_from / 2);
            // A failed pager can't be reused (see `make_pager`).
            make_pager(last_page_state);
        }
        last_page_state = pager->state(std::nullopt);
        exhausted = pager->is_exhausted();

        for (const auto& row : rs->rows()) {
            co_await coroutine::maybe_yield();
            as.check();
            rjson::value item = rjson::empty_object();
            describe_single_item(*selection, row, std::nullopt, item);
            co_await cb(std::move(item));
        }
    }
}

// Throws std::runtime_error for an incomplete row, or for a row whose request is not valid JSON.
static rjson::value make_export_summary(const export_summary_row& exp) {
    if (!exp.status || !exp.request) {
        throw std::runtime_error(fmt::format("Export '{}' has no {}", exp.export_arn, exp.status ? "request" : "export_status"));
    }
    rjson::value summary = rjson::empty_object();
    rjson::add(summary, "ExportArn", rjson::from_string(exp.export_arn));
    // DynamoDB's ExportStatus is IN_PROGRESS, COMPLETED or FAILED; report any other value as IN_PROGRESS.
    rjson::add(summary, "ExportStatus", rjson::from_string(*exp.status == "COMPLETED" || *exp.status == "FAILED" ? *exp.status : "IN_PROGRESS"));
    // ListExports always reports ExportType, also when the ExportTableToPointInTime request did not
    // specify ExportType. Then report the default value, FULL_EXPORT.
    try {
        auto exported_request = rjson::parse(*exp.request);
        const rjson::value* export_type = rjson::find(exported_request, "ExportType");
        rjson::add(summary, "ExportType", export_type ? rjson::copy(*export_type) : rjson::from_string("FULL_EXPORT"));
    } catch (const rjson::error& e) {
        throw std::runtime_error(fmt::format("Export '{}' has a malformed request: {}", exp.export_arn, e.what()));
    }
    return summary;
}

future<executor::request_return_type> executor::list_exports(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_exports++;

    // DynamoDB returns at most 25 summaries per page, and 10 when the request has no MaxResults.
    static constexpr int max_results_limit = 25;
    static constexpr int default_max_results = 10;

    // DynamoDB ignores a null parameter and reports a parameter of the wrong JSON type as
    // SerializationException.
    int max_results = default_max_results;
    if (const rjson::value* max_results_v = rjson::find(request, "MaxResults"); max_results_v && !max_results_v->IsNull()) {
        if (!max_results_v->IsNumber()) {
            co_return api_error::serialization("MaxResults must be an integer");
        }
        // DynamoDB truncates a fractional MaxResults.
        auto requested = std::trunc(max_results_v->GetDouble());
        if (requested < 1 || requested > max_results_limit) {
            co_return api_error::validation(fmt::format("MaxResults must be between 1 and {} inclusive", max_results_limit));
        }
        max_results = static_cast<int>(requested);
    }

    // TableArn is optional - without it every table's exports are listed.
    std::optional<arn_parts> table_parts;
    if (const rjson::value* table_arn_v = rjson::find(request, "TableArn"); table_arn_v && !table_arn_v->IsNull()) {
        if (!table_arn_v->IsString()) {
            co_return api_error::serialization("tableArn parameter: failed to parse - must be a string");
        }
        auto table_arn = rjson::to_string_view(*table_arn_v);
        if (table_arn.empty() || table_arn.size() > 1024) {
            co_return api_error::validation("tableArn parameter: failed to parse - must be between 1 and 1024 characters");
        }
        table_parts = parse_arn_reporting_validation(table_arn, "TableArn", "table", "");
    }

    // NextToken is the ARN of the last export in the previous page, which is where the ExportArn order
    // resumes. DynamoDB documents its NextToken only as a string copied from the previous response.
    std::string_view next_token;
    if (const rjson::value* next_token_v = rjson::find(request, "NextToken"); next_token_v && !next_token_v->IsNull()) {
        if (!next_token_v->IsString()) {
            co_return api_error::serialization("NextToken must be a string");
        }
        next_token = rjson::to_string_view(*next_token_v);
        auto next_token_parts = parse_export_arn(next_token, "NextToken"); // throw when non-conformant
        // DynamoDB rejects a NextToken that a listing of another table handed out.
        if (table_parts && (next_token_parts.keyspace_name != table_parts->keyspace_name || next_token_parts.table_name != table_parts->table_name)) {
            co_return api_error::validation(fmt::format("NextToken: Provided nextToken is invalid: `{}` comes from a listing of another table", next_token));
        }
    }

    maybe_audit(audit_info, audit::statement_category::QUERY,
                table_parts ? table_parts->keyspace_name : "", table_parts ? table_parts->table_name : "",
                "ListExports", request);

    // For a TableArn of a table that does not exist, DynamoDB returns an empty body without
    // ExportSummaries, also when the table was deleted after an export, but ValidationException
    // when no table can have the name the ARN gives. As in ExportTableToPointInTime, a table outside
    // Alternator counts as one that does not exist.
    if (table_parts && (!is_alternator_keyspace(table_parts->keyspace_name) || !_proxy.data_dictionary().try_find_table(table_parts->keyspace_name, table_parts->table_name))) {
        if (table_parts->keyspace_name.empty()) {
            co_return api_error::validation("TableArn: Invalid Table ARN - empty keyspace name");
        }
        validate_table_name(table_parts->table_name, "TableArn: Invalid Table ARN - table name");
        co_return rjson::print(rjson::empty_object());
    }

    auto exports = co_await get_all_exports(_proxy, client_state, std::move(trace_state), std::move(permit));

    // The exports table is partitioned by export ARN, so the TableArn filter is applied here, to the
    // table part of each export ARN. A row with a malformed ARN is skipped with a warning.
    std::vector<const export_summary_row*> listed;
    for (const auto& exp : exports) {
        try {
            auto parts = parse_export_arn(exp.export_arn, "ExportArn");
            if (!table_parts || (parts.keyspace_name == table_parts->keyspace_name && parts.table_name == table_parts->table_name)) {
                listed.push_back(&exp);
            }
        } catch (const api_error& e) {
            xlogger.warn("ListExports: leaving out export '{}' with a malformed ARN: {}", exp.export_arn, e.what());
        }
        co_await coroutine::maybe_yield();
    }

    // DynamoDB documents this order, ExportArn descending. Within one table it is also newest first,
    // because an export id starts with its zero-padded start time.
    std::ranges::sort(listed, [] (const export_summary_row* lhs, const export_summary_row* rhs) {
        return lhs->export_arn > rhs->export_arn;
    });

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportSummaries", rjson::empty_array());
    auto& export_summaries = response["ExportSummaries"];

    int emitted = 0;
    bool has_more = false;
    std::optional<sstring> last_export_arn;
    // Fill the page from the first export after NextToken. Only the exports on the page are turned
    // into summaries; an incomplete or malformed row is skipped with a warning.
    auto after_next_token = std::ranges::partition_point(listed, [&] (const export_summary_row* exp) {
        return !next_token.empty() && exp->export_arn >= next_token;
    });
    for (const export_summary_row* exp : std::ranges::subrange(after_next_token, listed.end())) {
        rjson::value summary;
        try {
            summary = make_export_summary(*exp);
        } catch (const std::runtime_error& e) {
            xlogger.warn("ListExports: leaving out export '{}': {}", exp->export_arn, e.what());
            continue;
        }
        if (emitted == max_results) {
            has_more = true;
            break;
        }
        rjson::push_back(export_summaries, std::move(summary));
        last_export_arn = exp->export_arn;
        ++emitted;
    }

    if (has_more && last_export_arn) {
        rjson::add(response, "NextToken", rjson::from_string(*last_export_arn));
    }

    co_return rjson::print(std::move(response));
}

// Manifest files report times as ISO 8601 UTC with millisecond precision,
// e.g. "2020-11-04T07:28:34.028Z".
static sstring format_manifest_time(db_clock::time_point tp) {
    auto millis = tp.time_since_epoch().count() % 1000;
    return fmt::format("{:%FT%T}.{:03}Z", fmt::gmtime(db_clock::to_time_t(tp)), millis);
}

/*
Main export fiber, running export functionality for a single request.
The export's algorithm goes as follows:
- first validate parameters, this is done in export_table_to_point_in_time function
- next the export_table_to_point_in_time function will start a run_export fiber
- the fiber constructs output pipeline (export_pipeline_interface) and runs
  full table scan (export_scan_table) writing data to pipeline
- once all data is written, metadata files are written as well and export concludes with success
- if there's a detected error, the export will finish with failure.
- if there's an interruption in execution (node going down, node crashed) then the export will stop being executed.
  In such case (after some time) a garbage collector fibers on other nodes will detect node missing
  (even if node will come back it's live_node_identifier will be different), (only) one of them will pick it up and clean up
  marking the export as failed. A node which comes back runs its first round as soon as it starts, without waiting for
  the period to pass, so the exports of its own previous incarnations - known to be dead for certain - are marked as
  failed promptly. That first round is an ordinary one, so it may just as well take over the exports of any other node
  gossip doesn't list as live at that moment.

To make sure the algorithm continues as required, the export row is used a current state for the algorithm - `export_status` and `node_id` fields
(together) determine, what is going on currently with the export and on which node. To avoid issues with races between other nodes, all writes
to export row are done using `update-if` LWT semantic (the writer must provide a current value of `export_status` and `node_id` fields and write
will go through only if those values match current values in the database). It is expected (but not strictly required), that as long as `node_id`
points to live node only that node will do changes to export row (assumption is that node either is doing the export or garbage collecting).
If other node "steals" export row, the now previous export executor might or might not cease to process, but can't modify export row.
In current version the "steal" of export row will only be executed by garbage collection fiber (which will mark the export as completed with failure).

The `export_status` state machine consists of following states:
- IN_PROGRESS - the export is running,
- FAILED - the export is completed, it failed,
- COMPLETED - the export is completed successfully,
- FAILING - the export is taken over by garbage collector and in progress of being marked as failed.
  Note - technically describe_export / list_exports should report such export as IN_PROGRESS,
  because not all fields with failure reason might yet be set.

`IN_PROGRESS` will be set by export_table_to_point_in_time when export row will be created. Then - as long as node is alive -
the state will not change until it either completes with success or failure (going straight to COMPLETED / FAILED respectively).
If the garbage collection fiber takes over - it will change state to FAILING (and node_id to itself) and then to FAILED.
*/
static future<> run_export(
        service::storage_proxy& proxy,
        db::system_distributed_keyspace& sdks,
        abort_source& as,
        seastar::gate::holder gate_holder,
        schema_ptr schema,
        export_row row,
        std::string table_arn,
        std::string s3_bucket,
        std::string s3_prefix,
        std::string s3_key_prefix,
        std::string export_id_token) {
    int64_t item_count = 0;

    auto [ error_msg, error_code ] = co_await ([&]() -> future<std::tuple<std::string, std::string>> {
        try {
            auto client = get_s3_client(proxy, s3_bucket);
            // s3::client addresses objects as "/bucket/key".
            auto s3_object_name = [&s3_bucket](std::string_view key) {
                return fmt::format("/{}/{}", s3_bucket, key);
            };
            // Each manifest file is accompanied by a ".md5" object holding the base64 encoded md5 of its content.
            auto put_object_with_md5 = [&client, &s3_object_name, &as](std::string_view json_key, std::string_view md5_key, std::string content) -> future<> {
                md5_hasher hasher;
                hasher.update(content.data(), content.size());
                auto md5_content = base64_encode(hasher.finalize());
                co_await client->put_object(s3_object_name(json_key), temporary_buffer<char>(content.data(), content.size()), s3::object_metadata{}, &as);
                co_await client->put_object(s3_object_name(md5_key), temporary_buffer<char>(md5_content.data(), md5_content.size()), s3::object_metadata{}, &as);
            };

            // Create the data file object key
            auto data_object_key = fmt::format("{}data/{}.json.gz", s3_key_prefix, export_id_token);

            try {
                // Create empty _started object
                co_await client->put_object(s3_object_name(fmt::format("{}_started", s3_key_prefix)), temporary_buffer<char>(), s3::object_metadata{}, &as);
            }
            catch(std::exception &e) {
                auto msg = std::string_view{ e.what() };
                // we are wild guessing here, but the message checks with S3Mock from Adobe and (hopefully) with Amazon
                // (tested by running put-object into non-existing bucket).
                if (msg.find("bucket") != std::string_view::npos && msg.find("exist") != std::string_view::npos) {
                    co_return std::make_tuple("The specified bucket does not exist", "S3NoSuchBucket");
                }
                co_return std::make_tuple(e.what(), "Exception");
            }

            // Create S3 sink pipeline
            std::exception_ptr exception;
            export_pipeline_interface::result export_result;

            // pipeline handling code is fragile in respect to clean up - the `flush_and_close()` method MUST BE called in all cases and
            // CANNOT be called via RAII (the method is asynchronous).
            // To ensure this we create and use pipeline wrapped in a try-catch block and straight after it we call `flush_and_close()`.
            std::unique_ptr<export_pipeline_interface> pipeline;

            try {
                pipeline = co_await create_sink_pipeline(s3_target_config{ client, s3_object_name(data_object_key), &as }, gzip_compression{});

                // Scan the table and feed items through the pipeline
                co_await export_scan_table(proxy, schema, as, empty_service_permit(), [&pipeline, &item_count](rjson::value item) -> future<> {
                    utils::get_local_injector().inject("alternator_export_to_s3_in_item_handler",
                            [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_item_handler"); });

                    rjson::value wrapper = rjson::empty_object();
                    rjson::add(wrapper, "Item", std::move(item));
                    co_await pipeline->process(wrapper);
                    ++item_count;
                });
            }
            catch(...) {
                exception = std::current_exception();
            }

            if (pipeline) {
                try {
                    export_result = co_await pipeline->flush_and_close();
                }
                catch(...) {
                    if (!exception) exception = std::current_exception();
                }
                pipeline.reset();
            }
            if (exception) std::rethrow_exception(std::move(exception));

            row.completed_at = db_clock::now();
            row.item_count = item_count;
            // ScyllaDB has nothing to bill for an export.
            row.billed_size_bytes = 0;

            // Generate and upload manifest files

            // manifest-files.json - JSON lines, one object per data file.
            auto manifest_files_key = fmt::format("{}manifest-files.json", s3_key_prefix);
            rjson::value data_file_entry = rjson::empty_object();
            rjson::add(data_file_entry, "itemCount", rjson::value(item_count));
            rjson::add(data_file_entry, "md5Checksum", rjson::from_string(export_result.md5));
            rjson::add(data_file_entry, "etag", rjson::from_string(export_result.etag));
            rjson::add(data_file_entry, "dataFileS3Key", rjson::from_string(data_object_key));
            auto manifest_files_content = rjson::print(data_file_entry) + "\n";
            co_await put_object_with_md5(manifest_files_key, fmt::format("{}manifest-files.md5", s3_key_prefix), std::move(manifest_files_content));

            // manifest-summary.json
            auto manifest_summary_key = fmt::format("{}manifest-summary.json", s3_key_prefix);
            rjson::value summary = rjson::empty_object();
            rjson::add(summary, "version", "2020-06-30");
            rjson::add(summary, "exportArn", rjson::from_string(row.export_arn));
            rjson::add(summary, "startTime", rjson::from_string(format_manifest_time(row.accepted_at)));
            rjson::add(summary, "endTime", rjson::from_string(format_manifest_time(*row.completed_at)));
            rjson::add(summary, "tableArn", rjson::from_string(table_arn));
            rjson::add(summary, "tableId", rjson::from_string(fmt::to_string(schema->id())));
            // We don't support point-in-time exports from the past - the table is scanned
            // as of the moment the export was accepted.
            rjson::add(summary, "exportTime", rjson::from_string(format_manifest_time(row.export_time)));
            rjson::add(summary, "s3Bucket", rjson::from_string(s3_bucket));
            rjson::add(summary, "s3Prefix", rjson::from_string(s3_prefix));
            rjson::add(summary, "s3SseAlgorithm", rjson::from_string(""));
            rjson::add(summary, "s3SseKmsKeyId", rjson::from_string(""));
            rjson::add(summary, "manifestFilesS3Key", rjson::from_string(manifest_files_key));
            rjson::add(summary, "billedSizeBytes", rjson::value(int64_t(0)));
            rjson::add(summary, "itemCount", rjson::value(item_count));
            xlogger.debug("Processed item count: {}", item_count);
            rjson::add(summary, "outputFormat", "DYNAMODB_JSON");
            auto manifest_summary_content = rjson::print(summary);
            co_await put_object_with_md5(manifest_summary_key, fmt::format("{}manifest-summary.md5", s3_key_prefix), std::move(manifest_summary_content));

            auto export_manifest = fmt::format("{}manifest-summary.json", s3_key_prefix);
            row.export_status = "COMPLETED";
            row.export_manifest = export_manifest;
            // The data and the manifests are already in S3, so this write must not be lost to a
            // transient failure - a retried, conditional write keeps a completed export from being
            // reported as FAILED just because one LWT timed out.
            auto updated = co_await write_final_export_row(sdks, row, as);
            if (!updated) {
                xlogger.debug("Export {} failed to update status to COMPLETED after completion", row.export_arn);
                co_return std::make_tuple("Export completed, but failed to update state", "Exception");
            }
            co_return std::make_tuple("", "" );
        } catch (std::exception &e) {
            co_return std::make_tuple(e.what(), "Exception");
        } catch(...) {
            co_return std::make_tuple("Unknown error", "Exception");
        }
    }());
    if (!error_code.empty()) {
        // We have failed with an exception, let's report it.
        row.completed_at = db_clock::now();
        xlogger.debug("Export {} failed with exception: {}", row.export_arn, error_msg);
        row.export_status = "FAILED";
        row.failure_code = std::move(error_code);
        row.failure_message = std::move(error_msg);
        // The failure may have happened after the successful path already filled in the fields
        // describing a completed export - most notably when the update to COMPLETED itself failed.
        // update_export() rewrites every column, so these have to be cleared here, or the stored
        // row would be FAILED while still carrying a manifest and item counts.
        row.export_manifest = std::nullopt;
        row.item_count = std::nullopt;
        row.billed_size_bytes = std::nullopt;
        bool updated;
        try {
            updated = co_await write_final_export_row(sdks, row, as);
        }
        catch(...) {
            // Only an abort gets here: the node is stopping, so the row is left to the garbage
            // collector of its next incarnation.
            xlogger.debug("Export {}: failed to update status to FAILED: {}", row.export_arn, std::current_exception());
            co_return;
        }
        if (!updated) {
            xlogger.debug("Export {} failed to update status to FAILED after completion", row.export_arn);
            co_return;
        }
        xlogger.debug("Export {} failed with {} items", row.export_arn, item_count);
    }
    else {
        xlogger.debug("Export {} completed successfully with {} items", row.export_arn, item_count);
    }
}

auto executor::s3_exports::try_reserve_export_slot() -> std::optional<slot> {
    if (_running_exports >= _e._proxy.local_db().get_config().alternator_max_concurrent_exports_per_shard()) {
        return std::nullopt;
    }
    return slot(*this);
}

void executor::s3_exports::start_export(slot export_slot, schema_ptr schema, export_row row, std::string table_arn, std::string s3_bucket,
        std::string s3_prefix, std::string s3_key_prefix, std::string export_id_token) {
    // Fire-and-forget, gate-guarded. `run_export()` reports failures by updating the export row, so
    // an exception escaping it is unexpected (e.g. a bad_alloc outside its own try/catch) - log it
    // instead of dropping an exceptional future on the floor.
    // The continuation at the end holds the reserved export slot, so that it is given back only
    // once the fiber is done - the fiber itself cannot hold it, `slot` being a type it cannot name.
    // That continuation runs after the fiber released its own gate holder, so it needs one of its
    // own: without it _gate.close() could return, and the executor be destroyed, while the slot -
    // which points back into this object - is still alive.
    auto export_arn = row.export_arn;
    auto slot_holder = _gate.hold();
    (void)run_export(_e._proxy, _e._sdks, _as, _gate.hold(), std::move(schema), std::move(row), std::move(table_arn),
            std::move(s3_bucket), std::move(s3_prefix), std::move(s3_key_prefix), std::move(export_id_token))
            .handle_exception([export_arn = std::move(export_arn)] (std::exception_ptr ep) {
        xlogger.error("Background export fiber for {} failed unexpectedly: {}", export_arn, ep);
    }).finally([export_slot = std::move(export_slot), slot_holder = std::move(slot_holder)] {});
}

// The fiber started by finish_export_in_background() below. It owns `row`, which outlives the
// caller, and holds the gate for as long as it keeps retrying.
static future<> finish_export_fiber(db::system_distributed_keyspace& sdks, export_row row, abort_source& as,
        seastar::gate::holder gate_holder) {
    if (!co_await write_final_export_row(sdks, row, as)) {
        xlogger.debug("Export {} failed to update status to {} in the background - the row is no longer ours",
                row.export_arn, row.export_status);
    }
}

void executor::s3_exports::finish_export_in_background(export_row row) {
    auto holder = _gate.try_hold();
    if (!holder) {
        // The executor is stopping, so there is no point in starting anything - the row is left to
        // the garbage collector of the next incarnation of this node.
        return;
    }
    // Fire-and-forget like start_export() above, and like it, `write_final_export_row()` is not
    // expected to throw anything but the abort of a stopping node - log whatever else comes out
    // instead of dropping an exceptional future on the floor.
    auto export_arn = row.export_arn;
    (void)finish_export_fiber(_e._sdks, std::move(row), _as, std::move(*holder))
            .handle_exception([export_arn = std::move(export_arn)] (std::exception_ptr ep) {
        xlogger.warn("Background fiber finishing export {} gave up: {}", export_arn, ep);
    });
}

} // namespace alternator
