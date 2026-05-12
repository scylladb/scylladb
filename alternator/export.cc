/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include <seastar/core/coroutine.hh>
#include "utils/rjson.hh"
#include "utils/s3/client.hh"
#include "abseil/absl/functional/overload.h"
#include <seastar/coroutine/as_future.hh>
#include <algorithm>
#include <string>
#include <string_view>

namespace alternator {

// Interfaces for `sink` / `source` pipelines.
// The `sink` pipeline consists of 3 stages:
//   - formatter (implements public interface `export_pipeline_interface`) - serializes rjson::value as-is to simple binary format (JSON lines, Ion, CSV are required by Amazon specs),
//   - compressor (implements `compression_interface`) - optionally compresses the data - Amazon S3 requires support for gzip compression,
//   - writer (implements `storage_sink_interface`) - writes the data to the storage (e.g. S3 or in-memory).
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
//   - reader (implements public interface `import_pipeline_interface`) - reads the data from the storage (e.g. S3 or in-memory).
//   - decompressor (implements `decompression_interface`) - optionally decompresses the data - Amazon S3 requires support for gzip compression,
//   - parser (implements `parsing_interface`) - deserializes binary data to rjson::value as-is (JSON lines, Ion, CSV are required by Amazon specs),
// After pipeline construction (see `create_**` family of factory functions in header `export.hh`) user is expected to
// call `import_pipeline_interface::read_all()`. This will read some data from the source and
// call `decompression_interface::decompress()` with it, which will - optionally - decompress it,
// then call `parsing_interface::parse()` with the decompressed data. The parser invokes the `on_item` (passed to the pipeline factory function) callback
// for each parsed item. Once all data is read, `read_all()` future will complete. After that user is expected to call `import_pipeline_interface::close()` to finalize the pipeline.
struct parsing_interface {
    virtual seastar::future<> parse(std::span<const std::byte>) = 0;
    virtual seastar::future<> close() = 0;
    virtual ~parsing_interface() = default;
};

struct decompression_interface {
    virtual seastar::future<> decompress(std::span<const std::byte>) = 0;
    virtual seastar::future<> close() = 0;
    virtual ~decompression_interface() = default;
};

// In memory sink - stores data to a caller owned in_memory_test_storage buffer object.
// Single threaded, single "file" use only. Caller must ensure in_memory_test_storage object lives long enough.
class in_memory_storage_sink : public storage_sink_interface {
    std::shared_ptr<in_memory_test_storage> _storage;
public:
    explicit in_memory_storage_sink(std::shared_ptr<in_memory_test_storage> storage)
        : _storage(std::move(storage)) {}

    seastar::future<> write(std::span<const std::byte> data) override {
        _storage->append(data);
        co_return;
    }

    seastar::future<> flush_and_close() override {
        _storage->flush_write();
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
    std::shared_ptr<in_memory_test_storage> _storage;
    std::unique_ptr<decompression_interface> _decompressor;
public:
    in_memory_source(std::shared_ptr<in_memory_test_storage> storage,
                     std::unique_ptr<decompression_interface> decompressor)
        : _storage(std::move(storage))
        , _decompressor(std::move(decompressor)) {}

    seastar::future<> read_all(seastar::abort_source *abort_source) override {
        auto data = _storage->data();
        auto* ptr = reinterpret_cast<const char*>(data.data());
        constexpr size_t chunk_size = 16;
        // We feed the data in small chunks here to test the pipeline.
        size_t position = 0;
        while(position < data.size()) {
            if (abort_source && abort_source->abort_requested()) {
                break;
            }
            auto n = std::min(chunk_size, data.size() - position);
            co_await _decompressor->decompress(std::as_bytes(std::span<const char>(ptr + position, n)));
            position += n;
        }
    }

    seastar::future<> close() override {
        _storage->flush_read();
        return _decompressor->close();
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

    seastar::future<> close() override {
        co_await _parser->close();
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

    seastar::future<> close() override {
        if (!_buffer.empty()) {
            // Process any remaining data as a final line (even if it doesn't end with a newline).
            co_await _on_item(rjson::parse(_buffer));
            _buffer.clear();
        }
        co_return;
    }
};

/// Writes data to an S3 object. The data is passed to `s3::client` object, which will upload it to S3 asynchronously.
/// The upload is completed when `flush_and_close()` is called.
class s3_storage_sink : public storage_sink_interface {
    seastar::shared_ptr<s3::client> _client;
    seastar::sstring _object_name;
    seastar::output_stream<char> _upload_stream;

public:
    s3_storage_sink(seastar::shared_ptr<s3::client> client, seastar::sstring object_name)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _upload_stream(seastar::output_stream<char>(_client->make_upload_sink(_object_name)))
    {
    }

    seastar::future<> write(std::span<const std::byte> data) override {
        // we will return the future from write() directly, no need to add `co_await` here.
        return _upload_stream.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    seastar::future<> flush_and_close() override {
        return _upload_stream.close();
    }
};

/// Reads data from an S3 object and feeds it through a decompression_interface.
/// read_all() streams the entire object, calling decompress() for each chunk.
class s3_storage_source : public import_pipeline_interface {
    seastar::shared_ptr<s3::client> _client;
    seastar::sstring _object_name;
    std::unique_ptr<decompression_interface> _decompressor;

public:
    s3_storage_source(seastar::shared_ptr<s3::client> client, seastar::sstring object_name,
                      std::unique_ptr<decompression_interface> decompressor)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _decompressor(std::move(decompressor))
    {
    }

    seastar::future<> read_all(seastar::abort_source *abort_source) override {
        auto src = seastar::input_stream<char>{ _client->make_chunked_download_source(_object_name, s3::full_range, abort_source) };
        std::exception_ptr exception;

        try {
            while (!abort_source ||!abort_source->abort_requested()) {
                auto buf = co_await src.read();
                if (buf.empty()) {
                    break;
                }
                co_await _decompressor->decompress(
                    std::span<const std::byte>(reinterpret_cast<const std::byte*>(buf.get()), buf.size()));
            }
        }
        catch(...) {
            exception = std::current_exception();
        }
        co_await src.close();
        if (exception) {
            throw exception;
        }
    }
    seastar::future<> close() override {
        co_await _decompressor->close();
    }
};

static std::unique_ptr<export_pipeline_interface>  create_export_pipeline(std::unique_ptr<storage_sink_interface> sink) {
    auto compressor = std::make_unique<noop_compressor>(std::move(sink));
    return std::make_unique<json_formatter>(std::move(compressor));
}

static std::unique_ptr<decompression_interface> create_decompression_pipeline(std::function<seastar::future<>(rjson::value)> on_item) {
    auto parser = std::make_unique<json_parser>(std::move(on_item));
    return std::make_unique<noop_decompressor>(std::move(parser));
}

// Factory function to create sink pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory sink pipeline for testing.
// - s3_target_config - pipeline that will write to S3 object.
std::unique_ptr<export_pipeline_interface> create_sink_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config) {
    return std::visit(absl::Overload{
        [](in_memory_target_config &cfg) -> std::unique_ptr<export_pipeline_interface> {
            return create_export_pipeline(std::make_unique<in_memory_storage_sink>(cfg.storage));
        },
        [](s3_target_config &cfg) -> std::unique_ptr<export_pipeline_interface> {
            return create_export_pipeline(std::make_unique<s3_storage_sink>(std::move(cfg.client), std::move(cfg.object_name)));
        }
    }, target_config);
}

// Factory function to create source pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory source pipeline for testing.
// - s3_target_config - pipeline that will read from S3 object.
std::unique_ptr<import_pipeline_interface> create_source_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, std::function<seastar::future<>(rjson::value)> on_item) {
    return std::visit(absl::Overload{
        [&](in_memory_target_config &cfg) -> std::unique_ptr<import_pipeline_interface> {
            auto decompressor = create_decompression_pipeline(std::move(on_item));
            return std::make_unique<in_memory_source>(cfg.storage, std::move(decompressor));
        },
        [&](s3_target_config &cfg) -> std::unique_ptr<import_pipeline_interface> {
            auto decompressor = create_decompression_pipeline(std::move(on_item));
            return std::make_unique<s3_storage_source>(std::move(cfg.client), std::move(cfg.object_name), std::move(decompressor));
        }
    }, target_config);
}

} // namespace alternator
