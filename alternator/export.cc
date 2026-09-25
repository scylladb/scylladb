/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "alternator/serialization.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "db/consistency_level.hh"
#include "db/timeout_clock.hh"
#include "exceptions/exceptions.hh"
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

// How long a single page read is allowed to take. Deliberately not
// `executor::default_timeout()` (the `alternator_timeout_in_ms` deadline of an
// interactive request, 10 s by default): this is a bulk background job which reads
// `scan_table_page_size` rows at a time bypassing the cache, so its pages are far more
// expensive than those of a user-facing Query or Scan. With the interactive deadline, a
// page which consistently needs more than it could never be read at all - every attempt
// would raise a read timeout, the retry loop below would spend its whole budget on that
// one page and the export would fail permanently instead of making progress.
// The value is a compromise: long enough to leave the slowest pages a chance, yet short
// enough that a page which really is stuck is retried (against a possibly healthier set
// of replicas) several times within the retry budget, and that aborting a scan with a
// read in flight does not wait for long - see `fetch_page_abortable()` below.
static constexpr auto scan_table_page_timeout = std::chrono::minutes(1);

// A pager together with everything it only borrows: it keeps referring to its query
// options and to the query state for as long as a read it started is in flight.
// Holding them in one reference counted object lets a read which outlived the scan
// (see `fetch_page_abortable()` below) keep them alive until it completes.
// Declaration order matters: the pager is destroyed first, before the objects it uses.
struct scan_pager {
    lw_shared_ptr<service::query_state> query_state;
    std::unique_ptr<cql3::query_options> query_options;
    std::unique_ptr<service::pager::query_pager> pager;
};

// Fetches a single page, but stops waiting for it as soon as `as` is aborted.
// Neither the pager nor the storage proxy accept an abort source, so a read which is
// already in flight cannot be cancelled - on abort we merely stop waiting for it and
// leave it to complete (or time out) in the background, discarding its result. That
// background continuation holds `sp`, because the read keeps using the pager and the
// objects the pager borrows until it finishes.
// Such an abandoned read also keeps using the caller's `service_permit` - the pager
// hands a copy of it to the storage proxy - so it must not outlive the scan the caller
// is waiting for. It is therefore registered in `read_gate`, which `scan_table()` closes
// before its future resolves.
static seastar::future<std::unique_ptr<cql3::result_set>> fetch_page_abortable(lw_shared_ptr<scan_pager> sp, abort_source& as, seastar::gate& read_gate) {
    // Whoever comes first - the read or the abort - resolves the page.
    struct page_race {
        promise<std::unique_ptr<cql3::result_set>> page;
        bool resolved = false;
    };
    auto race = make_lw_shared<page_race>();

    auto sub = as.subscribe([race, &as] () noexcept {
        if (!std::exchange(race->resolved, true)) {
            race->page.set_exception(as.abort_requested_exception_ptr());
        }
    });
    if (!sub) {
        // `as` was aborted before we even started reading.
        return make_exception_future<std::unique_ptr<cql3::result_set>>(as.abort_requested_exception_ptr());
    }

    // Taken before the read starts, so that a read is never in flight outside the gate.
    auto holder = read_gate.hold();
    (void)sp->pager->fetch_page(scan_table_page_size, gc_clock::now(), db::timeout_clock::now() + scan_table_page_timeout)
        .then_wrapped([race, sp, holder = std::move(holder)] (seastar::future<std::unique_ptr<cql3::result_set>> f) {
            if (std::exchange(race->resolved, true)) {
                // The scan was aborted while this read was in flight - nobody is
                // waiting for the page any more, so just drop it.
                if (f.failed()) {
                    xlogger.debug("S3 export scanner page read failed after the scan was aborted: {}", f.get_exception());
                } else {
                    f.ignore_ready_future();
                }
                return;
            }
            f.forward_to(std::move(race->page));
        });

    return race->page.get_future().finally([sub = std::move(sub)] {});
}

// Whether the coordinator could ever assemble the replica set `cl` asks for, no
// matter how long we wait. `db::block_for()` returns how many replicas a read has to
// hear from, and it returns 0 exactly when there is none to hear from: with the
// datacenter-local level used below, a table whose keyspace is not replicated to this
// node's datacenter at all - but it only notices that for NetworkTopologyStrategy.
// For any other strategy `block_for()` falls back to the keyspace-wide replication
// factor, which says nothing about how the replicas are spread over the datacenters,
// so a keyspace with no replica here at all still asks for a non-zero number of them.
// Hence the second test: a datacenter-local level only ever counts replicas local to
// the coordinator, so it is doomed whenever it needs more of them than this node's
// datacenter has nodes to begin with. Every node of the datacenter is counted, not
// just the ones replicating this table, which keeps the answer on the safe side - we
// only call a level unsatisfiable when no node coming back could possibly help. That
// is the permanent flavour of `unavailable_exception`, as opposed to the usual
// transient one (a replica down or restarting), and it is a realistic one here
// because the scan hardcodes its consistency level - the caller cannot ask for a
// non-local one. Returns false if the replication cannot be examined at all (the
// table was dropped from under us, say), because then the scan has no future either.
// Called from an exception handler, so it swallows whatever it runs into rather than
// replacing the failure being handled.
static bool consistency_level_satisfiable(service::storage_proxy& proxy, const schema& s, db::consistency_level cl) {
    try {
        auto erm = proxy.get_db().local().find_column_family(s.id()).get_effective_replication_map();
        auto need = db::block_for(*erm, cl);
        if (need == 0) {
            return false;
        }
        if (db::is_datacenter_local(cl)) {
            const auto& topo = erm->get_topology();
            const auto& dc_endpoints = topo.get_datacenter_endpoints();
            auto it = dc_endpoints.find(topo.get_datacenter());
            size_t local_nodes = it == dc_endpoints.end() ? 0 : it->second.size();
            if (local_nodes < need) {
                return false;
            }
        }
        return true;
    } catch (...) {
        xlogger.warn("S3 export scanner could not check the replication of {}.{}: {}",
                s.ks_name(), s.cf_name(), std::current_exception());
        return false;
    }
}

static seastar::future<> do_scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    abort_source& as,
    service_permit permit,
    seastar::noncopyable_function<seastar::future<>(rjson::value)> cb,
    seastar::gate& read_gate)
{
    as.check();

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
    // this internal scan operation. The caller should verify, if user has permission to perform the scan.
    auto& client_state = service::client_state::for_internal_calls();
    tracing::trace_state_ptr trace_state;
    auto query_state = make_lw_shared<service::query_state>(
        client_state, trace_state, std::move(permit));

    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;

    // A pager which failed to fetch a page must not be asked for that page again:
    // while preparing a page the pager consumes its partition ranges and read command
    // in place, so a second attempt on the same pager would query the wrong (possibly
    // empty) range. An empty range yields no rows, which the pager reports as
    // "exhausted" - the scan would then finish successfully, silently skipping the
    // rest of the table. Instead, every retry starts a brand new pager, resumed from
    // the paging state saved after the last successfully fetched page.
    lw_shared_ptr<scan_pager> sp;
    auto make_pager = [&] (lw_shared_ptr<const service::pager::paging_state> paging_state) {
        // A fresh holder for every pager: the previous one may still be referenced by
        // a read left running in the background, so it must not be modified in place.
        auto new_sp = make_lw_shared<scan_pager>();
        new_sp->query_state = query_state;
        new_sp->query_options = std::make_unique<cql3::query_options>(
            std::make_unique<cql3::query_options>(cl, std::vector<cql3::raw_value>{}),
            std::move(paging_state));

        // The pager modifies the read command it is given, so each pager gets its own copy.
        auto command = ::make_lw_shared<query::read_command>(
            schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice),
            query::tombstone_limit(proxy.get_tombstone_limit()));

        // Scan the full token ring.
        dht::partition_range_vector partition_ranges{
            dht::partition_range::make_open_ended_both_sides()};

        new_sp->pager = service::pager::query_pagers::pager(
            proxy, schema, selection, *new_sp->query_state, *new_sp->query_options,
            std::move(command), std::move(partition_ranges), nullptr);
        sp = std::move(new_sp);
    };

    // Paging state of the last successfully fetched page. A null state means
    // "start from the beginning", which is where a retry of the very first page
    // has to restart from.
    lw_shared_ptr<const service::pager::paging_state> last_page_state = nullptr;
    make_pager(last_page_state);

    while (!sp->pager->is_exhausted()) {
        as.check();
        std::unique_ptr<cql3::result_set> rs;

        // How long we are willing to keep retrying the same page, and how the wait
        // between the attempts grows. The budget is a wall-clock one on purpose: what
        // makes a failure transient is that the cluster needs some time to recover, and
        // the things we retry for - a replica restarting, a topology change moving the
        // data around - routinely take minutes rather than seconds, so a budget counted
        // in attempts would have to guess how long an attempt takes. The backoff doubles
        // up to a cap so that a short hiccup costs us a second, while a long one is not
        // hammered with a read every second for the whole budget.
        static constexpr auto max_retry_time = std::chrono::minutes(10);
        static constexpr auto initial_retry_delay = std::chrono::seconds(1);
        static constexpr auto max_retry_delay = std::chrono::seconds(30);
        auto retry_delay = initial_retry_delay;
        auto give_up_at = seastar::lowres_clock::now() + max_retry_time;

        // A read failure gets a much shorter leash than the wall-clock budget above,
        // see the `read_failure_exception` handler below for why.
        static constexpr int max_read_failures = 3;
        int read_failures = 0;

        // We will try to read a page several times to avoid an accidental transient
        // failure aborting whole scan. A full table scan runs for minutes or hours, so
        // a replica restarting or briefly falling behind - reported as an unavailable
        // or a read failure error, not only as a timeout - is just as likely as a
        // timeout and just as transient. The same goes for a node being momentarily
        // overloaded: the scan bypasses the cache and pages as fast as the callback
        // allows, so it is a likely victim of that, and it goes away once the load does.
        // Per-partition rate limiting is not a concern here - it only ever applies to
        // single-partition reads, and this is a scan of the whole token ring - so a
        // rate_limit_exception cannot reach us. We will still abort on any other error.
        // Unavailable and read failure errors are the ones which are not always
        // transient - an unsatisfiable consistency level, a page of data a replica
        // cannot read - so both handlers below cut the retrying short once the failure
        // looks permanent, rather than spending the whole budget on a doomed page.
        for (int attempts = 1; ; ++attempts) {
            std::exception_ptr failure;
            try {
                rs = co_await fetch_page_abortable(sp, as, read_gate);
                break;
            } catch(exceptions::read_timeout_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read timed out: {}", failure);
            } catch(exceptions::read_failure_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read failed on a replica: {}", failure);
                // Unlike the errors above, this one means a replica actively reported a
                // failure for this very read, and the usual reasons for that - too many
                // tombstones in the range, an sstable which cannot be read - are a
                // property of the data, so the identical query we would retry with gets
                // the identical answer no matter how long we wait. Some of them are
                // transient after all (a replica losing its connection mid-read is
                // reported the same way), so we do retry, but only a couple of times:
                // spending the whole budget, and the ~20 full page reads it buys, on an
                // already degraded cluster is not worth it for a page which most likely
                // fails again anyway.
                if (++read_failures >= max_read_failures) {
                    xlogger.error("S3 export scanner giving up after {} attempts, {} of which were replica read failures - "
                            "such a failure is usually caused by the data being read and waiting does not fix it: {}",
                            attempts, read_failures, failure);
                    std::rethrow_exception(std::move(failure));
                }
            } catch(exceptions::unavailable_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read found too few replicas available: {}", failure);
                // Waiting only helps if the missing replicas can come back; if `cl`
                // cannot be satisfied here at all, they cannot.
                if (!consistency_level_satisfiable(proxy, *schema, cl)) {
                    xlogger.error("S3 export scanner giving up after {} attempts, the failure cannot be fixed by waiting "
                            "(consistency level {} is unsatisfiable on this node for {}.{}): {}",
                            attempts, cl, schema->ks_name(), schema->cf_name(), failure);
                    std::rethrow_exception(std::move(failure));
                }
            } catch(exceptions::overloaded_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read was rejected, the node is overloaded: {}", failure);
            }
            // If we didn't break out of this loop, wait a bit and try again.
            if (seastar::lowres_clock::now() + retry_delay >= give_up_at) {
                // Don't get stuck forever asking the same page, maybe there's
                // a bug or a real problem in several replicas. We're giving up here and
                // rethrow the last failure - it names the coordinator and the replicas
                // involved, which is what explains why the scan could not go on. The
                // caller must catch and handle the exception.
                xlogger.error("S3 export scanner giving up after {} failed attempts to read the same page over {} seconds: {}",
                        attempts, std::chrono::duration_cast<std::chrono::seconds>(max_retry_time).count(), failure);
                std::rethrow_exception(std::move(failure));
            }
            // Such a failure happens for a reason - we don't want to retry too fast, so we wait a bit before retrying.
            co_await seastar::sleep_abortable(retry_delay, as);
            retry_delay = std::min(retry_delay * 2, max_retry_delay);
            // The pager which failed cannot be reused, start a fresh one resumed
            // from the page we last read successfully.
            make_pager(last_page_state);
        }
        last_page_state = sp->pager->state(std::nullopt);

        co_await coroutine::maybe_yield();
        for (const auto& row : rs->rows()) {
            as.check();
            rjson::value item = rjson::empty_object();
            // Convert each row to a DynamoDB-style JSON item using the
            // same logic as describe_single_item() from executor_util.
            describe_single_item(*selection, row, std::nullopt, item);
            co_await cb(std::move(item));
            co_await coroutine::maybe_yield();
        }
    }
}

seastar::future<> scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    abort_source& as,
    service_permit permit,
    seastar::noncopyable_function<seastar::future<>(rjson::value)> cb)
{
    // A read abandoned on abort (see `fetch_page_abortable()` above) keeps running in
    // the background and keeps using what the caller lent us - the permit above all,
    // which the pager passes on to the storage proxy. Closing the gate here before we
    // resolve makes the scan's future a join point for those reads too: once it
    // resolves, nothing of the scan is left running and the permit is gone. Aborting a
    // scan with a read in flight therefore still takes until that read completes or
    // times out (`scan_table_page_timeout`).
    seastar::named_gate read_gate("alternator::scan_table");
    std::exception_ptr ex;
    try {
        co_await do_scan_table(proxy, std::move(schema), as, std::move(permit), std::move(cb), read_gate);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await read_gate.close();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
}

} // namespace alternator
