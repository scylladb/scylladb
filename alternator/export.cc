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
#include "db/system_distributed_keyspace.hh"
#include "partition_slice_builder.hh"
#include "query/query-result-set.hh"
#include "service/storage_proxy.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>
#include "bytes.hh"
#include "utils/assert.hh"
#include "utils/base64.hh"
#include "utils/hashers.hh"
#include "utils/rjson.hh"
#include "utils/overloaded_functor.hh"
#include "utils/s3/client.hh"
#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace alternator {

static logging::logger xlogger("alternator-export");

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
    // For sinks which know the etag the storage assigned to the object - the S3 sink has to ask S3
    // for it, because an object uploaded in several parts gets an etag which is not the md5 of its content.
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
    noop_compressor(std::unique_ptr<storage_sink_interface> sink) : _sink(std::move(sink)) {}

    future<> compress(std::span<const std::byte> data) override {
        co_await _sink->write(data);
    }
    future<export_pipeline_interface::result> flush_and_close() override {
        return _sink->flush_and_close();
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

// TODO: uncomment this function after #31703 is merged
#if 0
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
#endif

/// Writes data to an S3 object. The data is passed to `s3::client` object, which will upload it to S3 asynchronously.
/// The upload is completed when `flush_and_close()` is called.
class s3_storage_sink : public storage_sink_interface {
    shared_ptr<s3::client> _client;
    sstring _object_name;
    abort_source* _as;
    std::unique_ptr<output_stream<char>> _upload_stream;
    sink_result_builder _result;
    bool _closed = false;

public:
    s3_storage_sink(shared_ptr<s3::client> client, sstring object_name, abort_source *as)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _as(as)
        , _upload_stream(std::make_unique<output_stream<char>>(_client->make_upload_jumbo_sink(_object_name, s3::object_metadata{}, std::nullopt, as)))
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
        co_await _upload_stream->close();
        // The sink uploads the object in multiple parts, so its S3 etag is not the md5 of the
        // content that sink_result_builder computed - it is the md5 of the concatenated part
        // md5s with the part count appended. Only S3 knows how the data was split into parts,
        // so ask it for the etag instead of trying to reproduce it here.
        auto info = co_await _client->get_object_info(_object_name, _as);
        
        // TODO: replace default return with this line after #31703 is merged
#if 0
            co_return _result.finalize(sstring(strip_etag_quotes(info.etag)));
#endif

        co_return _result.finalize(sstring(""));
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

static std::unique_ptr<export_pipeline_interface> create_export_pipeline(std::unique_ptr<storage_sink_interface> sink) {
    auto compressor = std::make_unique<noop_compressor>(std::move(sink));
    return std::make_unique<json_formatter>(std::move(compressor));
}

static std::unique_ptr<decompression_interface> create_decompression_pipeline(std::function<future<>(rjson::value)> on_item) {
    auto parser = std::make_unique<json_parser>(std::move(on_item));
    return std::make_unique<noop_decompressor>(std::move(parser));
}

// Factory function to create sink pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory sink pipeline for testing.
// - s3_target_config - pipeline that will write to S3 object.
std::unique_ptr<export_pipeline_interface> create_sink_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config) {
    return std::visit(overloaded_functor{
        [](in_memory_target_config &cfg) -> std::unique_ptr<export_pipeline_interface> {
            if (!cfg.storage) {
                on_internal_error(xlogger, "in_memory_target_config::storage is null");
            }
            return create_export_pipeline(std::make_unique<in_memory_storage_sink>(std::move(cfg.storage)));
        },
        [](s3_target_config &cfg) -> std::unique_ptr<export_pipeline_interface> {
            if (!cfg.client) {
                on_internal_error(xlogger, "s3_target_config::client is null");
            }
            return create_export_pipeline(std::make_unique<s3_storage_sink>(std::move(cfg.client), std::move(cfg.object_name), cfg.as));
        }
    }, target_config);
}

// Factory function to create source pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory source pipeline for testing.
// - s3_target_config - pipeline that will read from S3 object.
// Note: `on_item` callback must be valid until `import_pipeline_interface::close()` is resolved.
future<std::unique_ptr<import_pipeline_interface>> create_source_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, std::function<future<>(rjson::value)> on_item) {
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
        decompressor = create_decompression_pipeline(std::move(on_item));

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

// An export ARN is a table ARN followed by `/export/<id>`.
static arn_parts parse_export_arn(std::string_view export_arn, std::string_view arn_field_name) {
    static constexpr std::string_view export_infix = "/export/";
    auto parts = parse_arn_reporting_validation(export_arn, arn_field_name, "Export", export_infix);
    // parse_arn accepts an empty export id.
    if (parts.postfix.size() == export_infix.size()) {
        throw api_error::validation(fmt::format("{}: Invalid Export ARN `{}` - no export id after `{}`",
                arn_field_name, export_arn, export_infix));
    }
    return parts;
}

// An export reduced to what ListExports reports about it. The optional members are unset in a row
// which lacks the column.
struct export_summary_row {
    sstring export_arn;
    // The export's status, as stored. ListExports reports it as ExportStatus.
    std::optional<sstring> status;
    // The original ExportTableToPointInTime request, as JSON. ExportType is read back from it.
    std::optional<sstring> request;
};

// Returns every export there is, in no particular order.
static future<std::vector<export_summary_row>> get_all_exports(service::storage_proxy& proxy, service::client_state& client_state, service_permit permit) {
    using sdks = db::system_distributed_keyspace;
    schema_ptr schema = proxy.data_dictionary().find_schema(sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    dht::partition_range_vector partition_ranges{dht::partition_range::make_open_ended_both_sides()};
    auto partition_slice = partition_slice_builder(*schema)
            .with_regular_column(to_bytes("export_status"))
            .with_regular_column(to_bytes("request"))
            .build();
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice), query::tombstone_limit(proxy.get_tombstone_limit()));
    // system_distributed has RF=3, and QUORUM of that cannot be reached with a single token owner.
    auto cl = proxy.get_token_metadata_ptr()->count_normal_token_owners() > 1 ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    service::storage_proxy::result<service::storage_proxy::coordinator_query_result> rqr =
            co_await proxy.query_result(schema, std::move(command), std::move(partition_ranges), cl,
                    service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state));
    if (!rqr) {
        co_await coroutine::return_exception(create_api_error_from_coordinators_exception(std::move(rqr).assume_error()));
    }
    query::result_set rs = query::result_set::from_raw_result(schema, partition_slice, *rqr.assume_value().query_result);
    std::vector<export_summary_row> exports;
    exports.reserve(rs.rows().size());
    for (const query::result_set_row& row : rs.rows()) {
        exports.push_back(export_summary_row {
            .export_arn = row.get_nonnull<sstring>("export_arn"),
            .status = row.get<sstring>("export_status"),
            .request = row.get<sstring>("request"),
        });
    }
    co_return exports;
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

future<executor::request_return_type> executor::list_exports(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
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

    // NextToken is the ARN of the last export in the previous page. DynamoDB's NextToken has a
    // different format, but the API requires the client to copy NextToken from the previous response.
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
    // when no table can have the name the ARN gives.
    if (table_parts && !_proxy.data_dictionary().try_find_table(table_parts->keyspace_name, table_parts->table_name)) {
        if (table_parts->keyspace_name.empty()) {
            co_return api_error::validation("TableArn: Invalid Table ARN - empty keyspace name");
        }
        validate_table_name(table_parts->table_name, "TableArn: Invalid Table ARN - table name");
        co_return rjson::print(rjson::empty_object());
    }

    auto exports = co_await get_all_exports(_proxy, client_state, std::move(permit));

    // The exports table is partitioned by export ARN, so the TableArn filter is applied here, to the
    // table part of each export ARN. An incomplete or malformed row is skipped with a warning, and
    // the other exports are still listed.
    std::vector<std::pair<sstring, rjson::value>> summaries;
    for (const auto& exp : exports) {
        try {
            auto parts = parse_export_arn(exp.export_arn, "ExportArn");
            if (table_parts && (parts.keyspace_name != table_parts->keyspace_name || parts.table_name != table_parts->table_name)) {
                continue;
            }
            summaries.emplace_back(exp.export_arn, make_export_summary(exp));
        } catch (const api_error& e) {
            xlogger.warn("ListExports: leaving out export '{}' with a malformed ARN: {}", exp.export_arn, e.what());
        } catch (const std::runtime_error& e) {
            xlogger.warn("ListExports: leaving out export '{}': {}", exp.export_arn, e.what());
        }
    }

    // DynamoDB lists exports newest first. ExportArn descending gives the same order within one
    // table, but not across tables, because the table name comes before the export id in the ARN.
    std::ranges::sort(summaries, [] (const auto& lhs, const auto& rhs) {
        return lhs.first > rhs.first;
    });

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportSummaries", rjson::empty_array());
    auto& export_summaries = response["ExportSummaries"];

    int emitted = 0;
    bool has_more = false;
    std::optional<sstring> last_export_arn;
    // Skip the exports up to and including NextToken, then fill the page.
    for (auto& [export_arn, summary] : summaries) {
        if (!next_token.empty() && export_arn >= next_token) {
            continue;
        }
        if (emitted == max_results) {
            has_more = true;
            break;
        }
        rjson::push_back(export_summaries, std::move(summary));
        last_export_arn = export_arn;
        ++emitted;
    }

    if (has_more && last_export_arn) {
        rjson::add(response, "NextToken", rjson::from_string(*last_export_arn));
    }

    co_return rjson::print(std::move(response));
}
} // namespace alternator
