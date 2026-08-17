/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include <boost/regex/v5/regex_fwd.hpp>
#include <seastar/core/coroutine.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "auth/permission.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/storage_proxy.hh"
#include "utils/rjson.hh"
#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <boost/regex.hpp>

namespace alternator {

static std::optional<api_error> reject_unsupported_parameter(const rjson::value& request, std::string_view name) {
    if (rjson::find(request, name)) {
        return api_error::validation(fmt::format("{} attribute is not supported", name));
    }
    return std::nullopt;
}

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

    // Required parameters
    auto table_arn = get_non_empty_string_attribute(request, "TableArn");
    auto s3_bucket = get_non_empty_string_attribute(request, "S3Bucket");
    static boost::regex s3_bucket_regex("^[a-z0-9][a-z0-9\\-\\.]{1,61}[a-z0-9]$");
    if (!boost::regex_match(s3_bucket, s3_bucket_regex)) {
        co_return api_error::validation(fmt::format("S3Bucket attribute: must match regex `^[a-z0-9][a-z0-9\\-\\.]{{1,61}}[a-z0-9]$`, not `{}`", s3_bucket));
    }

    // Validate that the table exists
    auto parts = parse_arn(table_arn, "TableArn", "table", "");
    maybe_audit(audit_info, audit::statement_category::QUERY, parts.keyspace_name, parts.table_name, "ExportTableToPointInTime", request);
    try {
        auto schema = _proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
        get_stats_from_schema(_proxy, *schema)->api_operations.export_table_to_point_in_time++;
        co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::SELECT, _stats);
    } catch (const data_dictionary::no_such_column_family&) {
        co_return api_error::table_not_found(
                fmt::format("TableArn: Invalid table ARN `{}` - not found", table_arn));
    }

    // Optional parameters
    auto s3_prefix = get_non_empty_string_attribute(request, "S3Prefix", "");
    if (s3_prefix.size() > 255) {
        co_return api_error::validation(fmt::format("S3Prefix attribute: must not exceed 255 characters, got {}", s3_prefix.size()));
    }

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
        if (auto error = reject_unsupported_parameter(request, name)) {
            co_return std::move(*error);
        }
    }

    // ExportTime - only "now" (or close to now) is supported
    // If not specified, use current time. If specified, must be within 5 minutes of now.
    int64_t now = db_clock::to_time_t(db_clock::now());
    int64_t export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    if (export_time_v) {
        if (!export_time_v->IsInt64() && !export_time_v->IsUint64()) {
            co_return api_error::validation("Expected integer attribute ExportTime");
        }
        if (export_time_v->IsUint64()) {
            uint64_t value = export_time_v->GetUint64();
            if (value > uint64_t(std::numeric_limits<int64_t>::max())) {
                co_return api_error::validation("ExportTime is out of range");
            }
            export_time = value;
        } else {
            export_time = export_time_v->GetInt64();
        }
        auto diff = export_time > now ? export_time - now : now - export_time;
        if (diff > 300) {
            co_return api_error::invalid_export_time(fmt::format("ExportTime must be within 5 minutes of current time. "
                                "ExportTime: {}, current time: {}", export_time, now));
        }
    }

    auto client_token = get_non_empty_string_attribute(request, "ClientToken", "");

    // Build the ExportDescription response
    // The actual export functionality is not implemented yet, so the export is reported as FAILED.
    rjson::value export_desc = rjson::empty_object();

    // AWS, when export is called without client token, will return uniquely generated client token in response.
    rjson::add(export_desc, "ClientToken", client_token.empty() ? rjson::from_string("<empty>") : rjson::from_string(client_token));

    // We create fake arn here, the content up to `/export/` will be likely the same in future,
    // the last part (after `/export/`) will change - we need to encode a unique identifier of some sort there to
    // recognise the export in future (we don't have it yet so we don't do it now).
    rjson::add(export_desc, "ExportArn",
            rjson::from_string(fmt::format("arn:aws:dynamodb:us-east-1:000000000000:table/{}@{}/export/export-placeholder",
                    parts.keyspace_name, parts.table_name)));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(export_format));
    rjson::add(export_desc, "ExportStatus", "FAILED");
    rjson::add(export_desc, "ExportTime", rjson::value(export_time));
    rjson::add(export_desc, "ExportType", rjson::from_string(export_type));
    rjson::add(export_desc, "S3Bucket", rjson::from_string(s3_bucket));
    rjson::add(export_desc, "S3Prefix", rjson::from_string(s3_prefix));
    rjson::add(export_desc, "TableArn", rjson::from_string(table_arn));

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::describe_export(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_export++;

    // An export ARN is the ARN of the exported table with `/export/<id>` appended, so parsing it
    // also tells us which table the export belongs to.
    auto export_arn = get_non_empty_string_attribute(request, "ExportArn");
    auto parts = parse_arn(export_arn, "ExportArn", "Export", "/export/");

    maybe_audit(audit_info, audit::statement_category::QUERY, parts.keyspace_name, parts.table_name, "DescribeExport", request);
    if (auto table = _proxy.data_dictionary().try_find_table(parts.keyspace_name, parts.table_name)) {
        // Per-table metrics live on the table object, so they only exist for as long as it does.
        get_stats_from_schema(_proxy, *table->schema())->api_operations.describe_export++;
    }
    // The exported table may have been dropped after the export was accepted. Yet, DynamoDB keeps describing such exports.
    // Permissions are attached to the table's name, so they can be checked whether or not the table is still there.
    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, parts.keyspace_name, parts.table_name, auth::permission::SELECT, _stats);

    auto normal_token_owners = _proxy.get_token_metadata_ptr()->count_normal_token_owners();
    auto exp = co_await _sdks.get_alternator_export(export_arn, { normal_token_owners });
    // metadata_expires_at is set to completed_at + 90 days once an export reaches COMPLETED or
    // FAILED, matching DynamoDB's 90-day retention of export metadata, and it stays unset while the
    // export is still running, so a live export never expires. Removing the export row is left entirely
    // to the per-row TTL, which deletes it on its next sweep.
    if (!exp) {
        co_return api_error::export_not_found(fmt::format("ExportArn: Export `{}` not found", export_arn));
    }

    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));
    rjson::add(export_desc, "ExportStatus", rjson::from_string(exp->status));
    // DynamoDB reports the timestamps in ExportDescription as seconds since the epoch.
    rjson::add(export_desc, "StartTime", rjson::value(int64_t(db_clock::to_time_t(exp->accepted_at))));

    // The rest of the request is echoed back from the request that started the export,
    // with the same defaults which ExportTableToPointInTime applies to a request that omits them.
    auto exported_request = rjson::parse(exp->request);
    rjson::add(export_desc, "TableArn", rjson::from_string(get_non_empty_string_attribute(exported_request, "TableArn")));
    rjson::add(export_desc, "S3Bucket", rjson::from_string(get_non_empty_string_attribute(exported_request, "S3Bucket")));
    rjson::add(export_desc, "S3Prefix", rjson::from_string(get_non_empty_string_attribute(exported_request, "S3Prefix", "")));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(get_non_empty_string_attribute(exported_request, "ExportFormat", "DYNAMODB_JSON")));
    // ExportType is reported by DynamoDB only if ExportTableToPointInTime request had it
    if (rjson::find(exported_request, "ExportType")) {
        rjson::add(export_desc, "ExportType", rjson::from_string(get_non_empty_string_attribute(exported_request, "ExportType")));
    }

    // TableId identifies the table the export was taken from, not whatever table now answers to its name
    rjson::add(export_desc, "TableId", rjson::from_string(exp->table_id.to_sstring()));
    rjson::add(export_desc, "ExportTime", rjson::value(int64_t(db_clock::to_time_t(exp->export_time))));
    rjson::add(export_desc, "ClientToken", rjson::from_string(exp->client_token));

    // Everything below describes a finished export, so it is only present once one has run.
    if (exp->manifest && !exp->manifest->empty()) {
        rjson::add(export_desc, "ExportManifest", rjson::from_string(*exp->manifest));
    }
    if (exp->failure_code && !exp->failure_code->empty()) {
        rjson::add(export_desc, "FailureCode", rjson::from_string(*exp->failure_code));
    }
    if (exp->failure_message && !exp->failure_message->empty()) {
        rjson::add(export_desc, "FailureMessage", rjson::from_string(*exp->failure_message));
    }
    if (exp->item_count) {
        rjson::add(export_desc, "ItemCount", rjson::value(*exp->item_count));
    }
    if (exp->billed_size_bytes) {
        rjson::add(export_desc, "BilledSizeBytes", rjson::value(*exp->billed_size_bytes));
    }
    if (exp->completed_at) {
        rjson::add(export_desc, "EndTime", rjson::value(int64_t(db_clock::to_time_t(*exp->completed_at))));
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    co_return rjson::print(std::move(response));
}
} // namespace alternator
