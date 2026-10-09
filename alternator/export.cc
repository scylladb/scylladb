/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "db/config.hh"
#include "db/consistency_level.hh"
#include "db/timeout_clock.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"
#include "query/query-request.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "service/client_state.hh"
#include "service/pager/paging_state.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "utils/rjson.hh"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

namespace alternator {

static logging::logger xlogger("alternator-export");

// Interfaces for `sink` / `source` pipelines.
// The `sink` pipeline consists of 3 stages:
//   - formatter (see `export_pipeline_interface` interface in header) - serializes rjson::value as-is to simple binary format (JSON lines, Ion, CSV are required by Amazon specs),
//   - compressor - optionally compresses the data - Amazon S3 requires support for gzip compression,
//   - writer - writes the data to the storage (e.g. S3 or in-memory).
// After construction user is expected to call `export_pipeline_interface::process()` with an item to export,
// which will call a compressor and a writer for that item. After that the future will complete and user is free to call
// `export_pipeline_interface::process()` with another item.

struct storage_sink_interface {
    virtual seastar::future<> write(std::span<const std::byte>) = 0;
    virtual seastar::future<> flush_and_close() = 0;
    virtual ~storage_sink_interface() = default;
};

struct compression_interface {
    virtual seastar::future<> compress(std::span<const std::byte>) = 0;
    virtual seastar::future<> flush_and_close() = 0;
    virtual ~compression_interface() = default;
};

// The `source` pipeline consists of 3 stages:
//   - reader (see `import_pipeline_interface` interface in header) - reads the data from the storage (e.g. S3 or in-memory).
//   - decompressor - optionally decompresses the data - Amazon S3 requires support for gzip compression,
//   - parser - deserializes binary data to rjson::value as-is (JSON lines, Ion, CSV are required by Amazon specs),
// After pipeline construction (see `create_**` family of factory functions in header `export.hh`) user is expected to
// call `import_pipeline_interface::read()`. This will read some data from the source and
// call `decompression_interface::decompress()` with it, which will - optionally - decompress it,
// then call `parsing_interface::parse()` with the decompressed data. The parser invokes the `on_item` (passed to the pipeline factory function) callback
// for each parsed item.
struct parsing_interface {
    virtual seastar::future<> parse(std::span<const std::byte>) = 0;
    virtual seastar::future<> flush_and_close() = 0;
    virtual ~parsing_interface() = default;
};

struct decompression_interface {
    virtual seastar::future<> decompress(std::span<const std::byte>) = 0;
    virtual seastar::future<> flush_and_close() = 0;
    virtual ~decompression_interface() = default;
};

// In memory sink - stores data to a caller owned in_memory_test_storage buffer object.
// Single threaded, single "file" use only. Caller must ensure in_memory_test_storage object lives long enough.
class in_memory_storage_sink : public storage_sink_interface {
    in_memory_test_storage& _storage;
public:
    explicit in_memory_storage_sink(in_memory_test_storage& storage)
        : _storage(storage) {}

    seastar::future<> write(std::span<const std::byte> data) override {
        _storage.append(data);
        co_return;
    }

    seastar::future<> flush_and_close() override {
        _storage.flush_write();
        co_return;
    }
};

// No compression compressor - passes data further down the pipeline.
class noop_compressor : public compression_interface {
    std::unique_ptr<storage_sink_interface> _sink;

public:
    noop_compressor(std::unique_ptr<storage_sink_interface> sink) : _sink(std::move(sink)) {}

    seastar::future<> compress(std::span<const std::byte> data) override {
        co_await _sink->write(data);
    }

    seastar::future<> flush_and_close() override {
        co_await _sink->flush_and_close();
    }
};

// Formatter that converts rjson::value item to single JSON line and passes it to the compressor.
// The line is terminated with a newline character, so that the source pipeline can parse it line by line.
class json_formatter : public export_pipeline_interface {
    std::unique_ptr<compression_interface> _sink;
public:
    json_formatter(std::unique_ptr<compression_interface> sink) : _sink(std::move(sink)) {}

    seastar::future<> process(const rjson::value &item) override {
        // TODO(rcybulski): this is extremely slow and naive - we need a streaming version of `rjson::print` here.
        auto line = rjson::print(item);
        line += "\n";
        co_await _sink->compress(std::as_bytes(std::span<const char>(line)));
    }

    seastar::future<> flush_and_close() override {
        co_await _sink->flush_and_close();
    }
};

// In memory source object - reads data from a caller owned in_memory_test_storage buffer object and
// feeds it through a decompressor and parsing pipeline, which parses each line back into an rjson::value, and invokes the on_item callback.
// Single threaded, single "file" use only. Caller must ensure in_memory_test_storage object lives long enough, and that on_item callback remains valid until flush_and_close() is called.
// Due to a low chunk size this is test only class.
class in_memory_source : public import_pipeline_interface {
    in_memory_test_storage& _storage;
    std::unique_ptr<decompression_interface> _decompressor;
public:
    in_memory_source(in_memory_test_storage& storage,
                     std::unique_ptr<decompression_interface> decompressor)
        : _storage(storage)
        , _decompressor(std::move(decompressor)) {}

    seastar::future<> read() override {
        auto data = _storage.data();
        auto* ptr = reinterpret_cast<const char*>(data.data());
        constexpr size_t chunk_size = 16;
        // We feed the data in small chunks here to test the pipeline.
        size_t position = 0;
        while(position < data.size()) {
            auto n = std::min(chunk_size, data.size() - position);
            co_await _decompressor->decompress(std::as_bytes(std::span<const char>(ptr + position, n)));
            position += n;
        }
    }

    seastar::future<> flush_and_close() override {
        _storage.flush_read();
        return _decompressor->flush_and_close();
    }
};

// No compression decompressor - passes data further up the pipeline.
class noop_decompressor : public decompression_interface {
    std::unique_ptr<parsing_interface> _parser;
public:
    noop_decompressor(std::unique_ptr<parsing_interface> parser) : _parser(std::move(parser)) {}

    seastar::future<> decompress(std::span<const std::byte> data) override {
        co_await _parser->parse(data);
    }

    seastar::future<> flush_and_close() override {
        co_await _parser->flush_and_close();
    }
};

// Json parser that accumulates incoming data until it sees a newline character,
// then parses the accumulated line as JSON and invokes the on_item callback with the parsed rjson::value.
// The last line is parsed and sent to callback even if it doesn't end with a newline.
class json_parser : public parsing_interface {
    std::function<seastar::future<>(rjson::value)> _on_item;
    std::string _buffer;
public:
    json_parser(std::function<seastar::future<>(rjson::value)> on_item) : _on_item(std::move(on_item)) {}
    seastar::future<> parse(std::span<const std::byte> data) override {
        auto sv = std::string_view(reinterpret_cast<const char*>(data.data()), data.size());
        size_t pos = 0;
        while (pos < sv.size()) {
            auto nl = sv.find('\n', pos);
            if (nl == std::string_view::npos) {
                _buffer.append(sv.substr(pos));
                break;
            }
            _buffer.append(sv.substr(pos, nl - pos));
            if (!_buffer.empty()) {
                co_await _on_item(rjson::parse(_buffer));
                _buffer.clear();
            }
            pos = nl + 1;
        }
    }

    seastar::future<> flush_and_close() override {
        if (!_buffer.empty()) {
            // Process any remaining data as a final line (even if it doesn't end with a newline).
            co_await _on_item(rjson::parse(_buffer));
            _buffer.clear();
        }
        co_return;
    }
};

// Factory function to create in-memory sink pipeline for testing (no compression, JSON formatter).
std::unique_ptr<export_pipeline_interface> create_in_memory_sink_pipeline(in_memory_test_storage& storage) {
    auto sink = std::make_unique<in_memory_storage_sink>(storage);
    auto compressor = std::make_unique<noop_compressor>(std::move(sink));
    return std::make_unique<json_formatter>(std::move(compressor));
}

// Factory function to create in-memory source pipeline for testing (no compression, JSON parser).
std::unique_ptr<import_pipeline_interface> create_in_memory_source_pipeline(in_memory_test_storage& storage, std::function<seastar::future<>(rjson::value)> on_item) {
    auto parser = std::make_unique<json_parser>(std::move(on_item));
    auto decompressor = std::make_unique<noop_decompressor>(std::move(parser));
    return std::make_unique<in_memory_source>(storage, std::move(decompressor));
}

future<executor::request_return_type> executor::export_table_to_point_in_time(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.export_table_to_point_in_time++;

    // Required parameter
    auto table_arn = get_non_empty_string_attribute(request, "TableArn");

    // Validate that the table exists
    arn_parts parts;
    try {
        parts = parse_arn(table_arn, "TableArn", "table", "");
    }
    catch (api_error& e) {
        if (e._type == "AccessDeniedException") {
            // Unfortunately AWS returns ValidationException here, so we convert error type, leaving message as is.
            e._type = "ValidationException";
        }
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

    auto export_format = get_non_empty_string_attribute(request, "ExportFormat", "DYNAMODB_JSON");
    if (export_format != "DYNAMODB_JSON") {
        co_return api_error::validation(
                fmt::format("ExportFormat attribute: must be DYNAMODB_JSON, not `{}`", export_format));
    }

    auto export_type = get_non_empty_string_attribute(request, "ExportType", "FULL_EXPORT");
    if (export_type != "FULL_EXPORT") {
        co_return api_error::validation(
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
            co_return api_error::validation(fmt::format("{} attribute is not supported", name));
        }
    }

    // ExportTime - only "now" (or close to now) is supported
    // If not specified, use current time. If specified, must be within 5 minutes of now.
    auto now = (double)std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    if (export_time_v) {
        if (!export_time_v->IsNumber()) {
            co_return api_error::validation("Expected a number attribute ExportTime");
        }
        export_time = export_time_v->GetDouble();
        if (std::isnan(export_time) || std::isinf(export_time) || export_time < 0) {
            co_return api_error::invalid_export_time("ExportTime number is out of range of valid values for this field");
        }
        auto diff = export_time > now ? export_time - now : now - export_time;
        if (diff > 300) {
            co_return api_error::invalid_export_time(fmt::format("ExportTime must be within 5 minutes of current time. "
                                "ExportTime: {} s , current time: {} s", export_time, now));
        }
    }

    auto client_token = get_non_empty_string_attribute(request, "ClientToken", "");

    // Build the ExportDescription response
    // The actual export functionality is not implemented yet - this just returns
    // a FAILED status to indicate the export has been accepted, but immediately failed.
    rjson::value export_desc = rjson::empty_object();

    // FIXME: Currently, when export is called without a client token, we use
    // the fixed name "<empty>". This is wrong - we should return a uniquely
    // generated client token, like AWS does, not a fixed one.
    if (client_token.empty()) {
        client_token = "<empty>";
    }
    rjson::add(export_desc, "ClientToken", rjson::from_string(client_token));

    // FIXME: We create fake arn here, the content up to `/export/` will be likely the same in future,
    // the last part (after `/export/`) will change - we need to encode a unique identifier of some sort there to
    // recognise the export in future (we don't have it yet so we don't do it now).
    rjson::add(export_desc, "ExportArn",
            rjson::from_string(fmt::format("arn:aws:dynamodb:us-east-1:000000000000:table/{}@{}/export/export-placeholder",
                    parts.keyspace_name, parts.table_name)));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(export_format));
    rjson::add(export_desc, "ExportStatus", "FAILED");
    rjson::add(export_desc, "FailureCode", "NotImplemented");
    rjson::add(export_desc, "FailureMessage", "Not yet implemented - this is a placeholder response for testing the export API.");
    rjson::add(export_desc, "ExportTime", rjson::value(export_time));
    rjson::add(export_desc, "ExportType", rjson::from_string(export_type));
    rjson::add(export_desc, "S3Bucket", rjson::from_string(s3_bucket));
    if (!s3_prefix.empty()) {
        rjson::add(export_desc, "S3Prefix", rjson::from_string(s3_prefix));
    }
    rjson::add(export_desc, "TableArn", rjson::from_string(table_arn));

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    co_return rjson::print(std::move(response));
}

// Note: copied from #30221, where the TTL expiration scanner picks its consistency
// level the same way. Will merge both versions together as followup.
//
// The consistency level used to read the table in this background scan:
// LOCAL_QUORUM for NetworkTopologyStrategy tables and QUORUM for other
// strategies - except that when the DC or the cluster has fewer token owners
// than a quorum would need, LOCAL_ONE or ONE is used instead.
//
// For NetworkTopologyStrategy, if the local DC has only a single token-owning
// replica, a quorum may be impossible to achieve. In that case that single
// replica holds all the data, so LOCAL_ONE gives the same guarantees as
// LOCAL_QUORUM would on a table with RF=1.
//
// For other strategies (e.g., the SimpleStrategy used by the
// system_distributed keyspace) the replicas of a range may all live in
// remote DCs, so a DC-local consistency level can be unachievable no matter
// how many nodes the local DC has. We use QUORUM over all token-owning
// replicas instead, just as the CDC tables in that keyspace do, accepting a
// cross-DC round trip in this background scan. As with those tables, a single
// token-owning replica in the whole cluster means ONE is enough.
//
// Throws if the table cannot be scanned DC-locally at all - see below.
static db::consistency_level scan_consistency_level(const locator::token_metadata& tm,
        const schema& s, const locator::effective_replication_map& erm) {
    const auto& rs = erm.get_replication_strategy();
    if (rs.get_type() != locator::replication_strategy_type::network_topology) {
        return tm.count_normal_token_owners() > 1
            ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    }
    const auto& local_dc = tm.get_topology().get_datacenter();
    // A keyspace replicated with RF=0 in the local DC has no local replicas at all, so a
    // DC-local consistency level ends up with an empty target list. That is not an error for
    // a read - `abstract_read_executor::execute()` returns an empty result for it - so the
    // pager would immediately report the scan as exhausted and the export would "succeed"
    // with no items at all. There is nothing here to read, so fail instead.
    if (db::local_quorum_for(erm, local_dc) == 0) {
        throw api_error::validation(fmt::format(
                "Cannot export {}.{}: keyspace has no replicas in the local datacenter {}",
                s.ks_name(), s.cf_name(), local_dc));
    }
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

    // A full table scan is a bulk background job, it must not compete with user traffic.
    // Run it in the streaming scheduling group - the same low-priority group the TTL
    // expiration scanner runs in - so the CPU, IO and the reader concurrency semaphore it
    // uses are the maintenance ones, not the ones serving user queries. The switch is done
    // here rather than left to the caller, so no caller can get it wrong, and it has to
    // happen before the first read is set up: the page size the scan gets
    // (`get_max_result_size()` below) is also picked by the scheduling group we run in.
    co_await coroutine::switch_to(proxy.local_db().get_streaming_scheduling_group());

    // Build a wildcard selection (SELECT *) for all columns.
    auto selection = cql3::selection::selection::wildcard(schema);

    // Collect all column IDs to read. The selection above is a wildcard, so it expects
    // a value for every column of the schema, static ones included - asking the query
    // for fewer columns than the selection reads would leave the result rows misaligned.
    auto regular_columns =
        schema->regular_columns()
        | std::views::transform(&column_definition::id)
        | std::ranges::to<query::column_id_vector>();
    auto static_columns =
        schema->static_columns()
        | std::views::transform(&column_definition::id)
        | std::ranges::to<query::column_id_vector>();

    // Set up query options: allow short reads and bypass cache.
    query::partition_slice::option_set opts = selection->get_query_options();
    opts.set<query::partition_slice::option::allow_short_read>();
    opts.set<query::partition_slice::option::bypass_cache>();

    // Scan all clustering ranges (no restriction).
    std::vector<query::clustering_range> ck_bounds{
        query::clustering_range::make_open_ended_both_sides()};

    auto partition_slice = query::partition_slice(
        std::move(ck_bounds), std::move(static_columns), std::move(regular_columns), opts);

    // Use an internal client state - no authorization checks needed for
    // this internal scan operation. The caller should verify that the user has permission to
    // perform the scan.
    auto& client_state = service::client_state::for_internal_calls();
    tracing::trace_state_ptr trace_state;
    service::query_state query_state(client_state, trace_state, std::move(permit));

    // See scan_consistency_level() above for how the level is picked.
    db::consistency_level cl = scan_consistency_level(
        *proxy.get_token_metadata_ptr(),
        *schema,
        *schema->table().get_effective_replication_map()
    );

    // The tombstone limit the scan's pages are read with - "stop the page after this many
    // tombstones, even if it is still empty". It is the only mechanism which lets a page end
    // somewhere inside a long run of tombstones: the row limit cannot do it, as a page which
    // has not reached a live row yet has no rows to limit. Without it, a range holding more
    // tombstones than one page deadline can scan through has no page which can get past it -
    // every attempt times out, the retry restarts from the same paging state, and a scan which
    // retries forever never moves on.
    // It is deliberately not taken from `storage_proxy::get_tombstone_limit()`: that one
    // returns `tombstone_limit::max` - i.e. no limit - for every query which is not a user
    // query, and "not a user query" means any scheduling group other than the statement one,
    // so the switch to the streaming group above would silently disable the limit for this
    // scan. Read the configured limit directly instead, keeping the one check which is about
    // the cluster rather than about who is asking: replicas which cannot return an empty page
    // must not be given a tombstone limit, as a page cut by it may well be empty.
    // Read on every pager, not once, so that a live update of the option reaches a scan which
    // is already running.
    auto tombstone_limit = [&proxy] {
        return proxy.features().empty_replica_pages
                ? query::tombstone_limit(proxy.local_db().get_config().query_tombstone_page_limit())
                : query::tombstone_limit::max;
    };

    // A pager which failed to fetch a page must not be asked for that page again:
    // while preparing a page the pager consumes its partition ranges and read command
    // in place, so a second attempt on the same pager would query the wrong (possibly
    // empty) range. An empty range yields no rows, which the pager reports as
    // "exhausted" - the scan would then finish successfully, silently skipping the
    // rest of the table. Instead, every retry starts a brand new pager, resumed from
    // the paging state saved after the last successfully fetched page.
    // Declaration order matters: the pager refers to the query options, so it has to be
    // destroyed first - and for the same reason the two are always replaced together.
    std::unique_ptr<cql3::query_options> query_options;
    std::unique_ptr<service::pager::query_pager> pager;
    auto make_pager = [&] (lw_shared_ptr<const service::pager::paging_state> paging_state) {
        pager.reset();
        query_options = std::make_unique<cql3::query_options>(
            std::make_unique<cql3::query_options>(cl, std::vector<cql3::raw_value>{}),
            std::move(paging_state));

        // The pager modifies the read command it is given, so each pager gets its own copy.
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

    // Paging state of the last successfully fetched page. A null state means
    // "start from the beginning", which is where a retry of the very first page
    // has to restart from.
    lw_shared_ptr<const service::pager::paging_state> last_page_state = nullptr;
    make_pager(last_page_state);

    // The number of rows one page may hold. `unlimited_page_size` means no row limit at all:
    // a page is then bounded only by its size in bytes - the page limit which the
    // `max_result_size` that `make_pager` gives each read command carries, which for this
    // scan is the hard-coded `query::result_memory_limiter::maximum_result_size` (1 MiB),
    // as the streaming scheduling group we switched to makes this a maintenance request and
    // those do not take their page size from the configurable `query_page_size_in_bytes`.
    // That is the limit which bounds the memory a page costs us, and it is where we want to be
    // while the cluster keeps up, as a row limit on top of it would only make pages of small rows
    // smaller than they need to be. A page which fails is, however, quite likely to be a page which
    // is simply too much data to read for one deadline, so every
    // failed attempt halves the row limit, down to a single row, and every page which comes
    // back well within its deadline doubles it back, until the row limit stops binding and
    // the byte limit alone decides again. These live outside the loop on purpose: a cluster
    // which could not serve a big page for the page we are on will not be able to serve one
    // for the next page either, and a cluster which recovered has to be given its page size
    // back no matter which page we have reached by then.
    static constexpr uint32_t unlimited_page_size = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t min_page_size = 1;
    uint32_t page_size = unlimited_page_size;
    // How many rows the last successfully fetched page held. The first halving starts from
    // it: while the row limit is not binding, halving it would change nothing.
    uint32_t last_page_rows = 0;
    // What the first halving starts from when there is no such number to start from - the
    // very first page of the scan failed, so no page has come back yet, or the last page
    // which did come back held no rows at all. Halving from zero would jump straight to
    // `min_page_size`, which is both far more drastic than the one failure we have seen
    // warrants (it would take a good handful of fast pages before the doubling brings the
    // limit back above the rows a 1 MiB page holds and it stops binding) and, for a read failure,
    // the point at which the scan gives up - after only two attempts. The exact value only
    // has to be a page clearly smaller than an unlimited one yet still worth asking for:
    // if it is still too much, the next failures halve it further.
    static constexpr uint32_t assumed_page_rows = 1000;

    while (!pager->is_exhausted()) {
        as.check();
        std::unique_ptr<cql3::result_set> rs;

        // How the wait between two attempts at the same page grows: doubling after every
        // failed attempt, up to a cap, so that a disruption which lasts is not hammered
        // with reads while a one-off hiccup still costs us only a second.
        static constexpr auto initial_retry_delay = std::chrono::seconds(1);
        static constexpr auto max_retry_delay = std::chrono::seconds(30);
        auto retry_delay = initial_retry_delay;

        // A page is retried for as long as it keeps failing with a transient error - there
        // is deliberately no limit on the number of attempts or on the time spent on them.
        // A scan which gives up cannot be resumed: it loses everything it has read so far,
        // so the whole export has to start over. A limit here would do exactly that for
        // reasons which do not mean the export cannot succeed - the scan runs in a
        // low-priority scheduling group, so user traffic saturating the cluster can make
        // page reads time out for as long as that traffic lasts, and a replica restarting
        // or a topology change moving the data around routinely takes minutes. An export
        // which really has to stop is stopped through `as`. The only error we do give up on
        // is a read failure which survives shrinking the page down to a single row, see its
        // handler below. We abort on any other error: the ones caught below are the
        // transient ones, anything else is not worth waiting for.
        for (int attempts = 1; ; ++attempts) {
            // A single page read gets the same deadline an interactive Alternator request
            // gets (`alternator_timeout_in_ms`, 10 s by default), which is also what the TTL
            // scanner's page reads use. It is deliberately short: a read already in flight
            // cannot be cancelled, so this deadline is also the longest an abort arriving
            // mid-read can go unnoticed. Timing out costs us little - the page is simply read
            // again below, from the position the previous page ended at, so no work done so
            // far is thrown away.
            auto timeout = executor::default_timeout();
            auto started = db::timeout_clock::now();
            try {
                rs = co_await pager->fetch_page(page_size, gc_clock::now(), timeout);
                last_page_rows = static_cast<uint32_t>(rs->size());
                // The read used only a small part of the time it was allowed to take, so
                // whatever forced the pages to shrink is over - let them grow back.
                if (page_size != unlimited_page_size
                        && (db::timeout_clock::now() - started) * 4 < timeout - started) {
                    page_size = page_size > unlimited_page_size / 2
                            ? unlimited_page_size : page_size * 2;
                }
                break;
            } catch(exceptions::read_timeout_exception&) {
                xlogger.warn("S3 export scanner read timed out (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            } catch(exceptions::read_failure_exception&) {
                // Unlike the other errors caught here, this one means a replica actively
                // reported a failure for this very read rather than being late or too busy
                // to serve it, and the usual reasons for that - too many tombstones in the
                // range, an sstable which cannot be read - are reasons a smaller page can
                // still get past. Once even the smallest page we can ask for keeps failing,
                // waiting longer will not help: this is the one error the scan gives up on.
                if (page_size <= min_page_size) {
                    xlogger.error("S3 export scanner read failed on a replica (attempt {}) "
                            "with the smallest possible page, giving up: {}",
                            attempts, std::current_exception());
                    throw;
                }
                xlogger.warn("S3 export scanner read failed on a replica (attempt {}), "
                        "retrying with a smaller page: {}",
                        attempts, std::current_exception());
            } catch(exceptions::unavailable_exception&) {
                xlogger.warn("S3 export scanner read found too few replicas available (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            } catch(exceptions::overloaded_exception&) {
                xlogger.warn("S3 export scanner read was rejected, the node is overloaded (attempt {}), retrying: {}",
                        attempts, std::current_exception());
            }
            // A read already in flight cannot be cancelled, so an abort requested while we
            // were reading is only noticed now. Check it before anything else can turn the
            // read's failure into the scan's result: the caller asked us to stop, and that
            // is what the scan has to report, not the read error we happened to get or the
            // sleep_aborted from the backoff below.
            as.check();
            // We don't want to retry too fast, so we wait a bit before retrying.
            try {
                co_await seastar::sleep_abortable(retry_delay, as);
            } catch (const seastar::sleep_aborted&) {
                // sleep_abortable() reports a plain request_abort() as its own sleep_aborted;
                // rethrow the abort source's exception instead, as every other abort point
                // here does.
                as.check();
                throw;
            }
            retry_delay = std::min(retry_delay * 2, max_retry_delay);
            // Ask for less next time. For most of the errors retried here a page which failed
            // is quite likely to be a page which was too much work for the deadline it was
            // given, and halving is how we find out how much this cluster can do right now.
            // `unavailable_exception` is the one which does not fit: too few live replicas for
            // the consistency level says nothing about how much work the page was, and a
            // smaller page will not be served any better. We shrink on it anyway instead of
            // special-casing it - the shrinking only costs the handful of fast pages it takes
            // to double back once enough replicas are up again, which is cheaper than the
            // waiting we are already doing, and one rule for every retried error is one rule
            // less to get wrong. The first halving starts from the number of rows the last
            // page actually held, as until then the row limit is not binding and halving it
            // would change nothing - or from `assumed_page_rows`, if no page has come back
            // with rows in it yet.
            uint32_t halve_from = page_size;
            if (page_size == unlimited_page_size) {
                halve_from = last_page_rows > 0 ? last_page_rows : assumed_page_rows;
            }
            page_size = std::max(min_page_size, halve_from / 2);
            // The pager which failed cannot be reused, start a fresh one resumed
            // from the page we last read successfully.
            make_pager(last_page_state);
        }
        last_page_state = pager->state(std::nullopt);

        for (const auto& row : rs->rows()) {
            co_await coroutine::maybe_yield();
            as.check();
            rjson::value item = rjson::empty_object();
            describe_single_item(*selection, row, std::nullopt, item);
            co_await cb(std::move(item));
        }
    }
}

} // namespace alternator
