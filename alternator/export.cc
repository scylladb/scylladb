/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include <exception>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>
#include "utils/rjson.hh"
#include "utils/overloaded_functor.hh"
#include "utils/s3/client.hh"
#include <algorithm>
#include <string>
#include <string_view>

namespace alternator {

static logging::logger elogger("alternator-export");

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
//   - reader (implements `source_interface`) - reads the data from the storage (e.g. S3 or in-memory).
//   - decompressor (implements `decompression_interface`) - optionally decompresses the data - Amazon S3 requires support for gzip compression,
//   - parser (implements `parsing_interface`) - deserializes binary data to rjson::value as-is (JSON lines, Ion, CSV are required by Amazon specs),
// After pipeline construction (see `create_**` family of factory functions in header `export.hh`) user is expected to
// call `import_pipeline_interface::read_all()`. This will read all data from the source (implementing `source_interface`), calling `read_some()` in a loop, for each blob
// calling `decompression_interface::decompress()` with it, which will - optionally - decompress it,
// then calling `parsing_interface::parse()` with the decompressed data. The `parse()` call will invoke the `on_item` (passed to the pipeline factory function) callback
// for each parsed item. Once all data is read, `read_all()` future will complete. After that user is expected to call `import_pipeline_interface::close()` to finalize the pipeline.
struct source_interface {
    virtual seastar::future<std::span<const std::byte>> read_some() = 0;
    virtual seastar::future<> close() = 0;
    virtual void cancel() noexcept = 0;
    virtual ~source_interface() = default;
};

struct parsing_interface {
    virtual seastar::future<> parse(std::span<const std::byte>) = 0;
    virtual seastar::future<> close() = 0;
    virtual void cancel() noexcept = 0;
    virtual ~parsing_interface() = default;
};

struct decompression_interface {
    virtual seastar::future<> decompress(std::span<const std::byte>) = 0;
    virtual seastar::future<> close() = 0;
    virtual void cancel() noexcept = 0;
    virtual ~decompression_interface() = default;
};

// In memory sink - stores data to a caller owned in_memory_test_storage buffer object.
// Single threaded, single "file" use only.
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

// In memory source object - reads data from a caller owned in_memory_test_storage buffer object.
// Single threaded, single "file" use only.
// Due to a low chunk size this is test only class.
class in_memory_source : public source_interface {
    std::shared_ptr<in_memory_test_storage> _storage;
    size_t _position = 0;

public:
    in_memory_source(std::shared_ptr<in_memory_test_storage> storage)
        : _storage(std::move(storage)) {}

    seastar::future<std::span<const std::byte>> read_some() override {
        auto data = _storage->data();
        auto* ptr = reinterpret_cast<const char*>(data.data());
        constexpr size_t chunk_size = 16;
        // We feed the data in small chunks here to test the pipeline.
        const auto n = std::min(chunk_size, data.size() - _position);
        const auto result = std::span<const std::byte>(reinterpret_cast<const std::byte*>(ptr + _position), n);
        _position += n;
        co_return result;
    }

    void cancel() noexcept override {
    }
    seastar::future<> close() override {
        _storage->flush_read();
        return make_ready_future();
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
    void cancel() noexcept override {
        _parser->cancel();
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
                auto _ = seastar::defer([&]() noexcept {
                    _buffer.clear();
                });
                co_await _on_item(rjson::parse(_buffer));
            }
            pos = nl + 1;
        }
    }
    void cancel() noexcept override {
        _buffer.clear();
    }
    seastar::future<> close() override {
        if (!_buffer.empty()) {
            // Process any remaining data as a final line (even if it doesn't end with a newline).
            auto _ = seastar::defer([&]() noexcept {
                _buffer.clear();
            });
            co_await _on_item(rjson::parse(_buffer));
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
    bool closed_ = false;

public:
    s3_storage_sink(seastar::shared_ptr<s3::client> client, seastar::sstring object_name)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _upload_stream(seastar::output_stream<char>(_client->make_upload_sink(_object_name)))
    {
    }
    ~s3_storage_sink() {
        SCYLLA_ASSERT(closed_ && "s3_storage_sink destroyed without calling flush_and_close().");
    }

    seastar::future<> write(std::span<const std::byte> data) override {
        // we will return the future from write() directly, no need to add `co_await` here.
        return _upload_stream.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    seastar::future<> flush_and_close() override {
        co_await _upload_stream.close();
        closed_ = true;
    }
};

/// Reads some data from an S3 object. Returns empty span when the S3 object is fully read.
class s3_storage_source : public source_interface {
    seastar::shared_ptr<s3::client> _client;
    seastar::sstring _object_name;
    temporary_buffer<char> _buffer;
    std::optional<seastar::input_stream<char>> _src;
public:
    s3_storage_source(seastar::shared_ptr<s3::client> client, seastar::sstring object_name, seastar::abort_source *abort_source)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _src(_client->make_chunked_download_source(_object_name, s3::full_range, abort_source))
    {
    }

    seastar::future<std::span<const std::byte>> read_some() override {
        if (!_src) {
            on_internal_error(elogger, "s3_storage_source already closed");
        }
        _buffer = co_await _src->read();
        co_return std::span<const std::byte>(reinterpret_cast<const std::byte*>(_buffer.get()), _buffer.size());
    }
    void cancel() noexcept override {
    }
    seastar::future<> close() override {
        if (_src) {
            auto _ = seastar::defer([&]() noexcept {
                _src = std::nullopt;
            });
            co_await _src->close();
        }
    }
};


/// Reads data from an S3 object and feeds it through a decompression_interface.
/// read_all() streams the entire object, calling decompress() for each chunk.
class import_pipeline_impl : public import_pipeline_interface {
    std::unique_ptr<source_interface> _source;
    std::unique_ptr<decompression_interface> _decompressor;

public:
    import_pipeline_impl(std::unique_ptr<source_interface> source, std::unique_ptr<decompression_interface> decompressor)
        : _source(std::move(source))
        , _decompressor(std::move(decompressor))
    {
    }

    seastar::future<> read_all() override {
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
            _source->cancel();
            _decompressor->cancel();
            throw;
        }
    }
    seastar::future<> close() override {
        std::exception_ptr exception;

        try {
            co_await _source->close();
        } catch(...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
        try {
            co_await _decompressor->close();
        } catch(...) {
            if (!exception) {
                exception = std::current_exception();
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

static std::unique_ptr<decompression_interface> create_decompression_pipeline(std::function<seastar::future<>(rjson::value)> on_item) {
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
                on_internal_error(elogger, "in_memory_target_config::storage is null");
            }
            return create_export_pipeline(std::make_unique<in_memory_storage_sink>(std::move(cfg.storage)));
        },
        [](s3_target_config &cfg) -> std::unique_ptr<export_pipeline_interface> {
            if (!cfg.client) {
                on_internal_error(elogger, "s3_target_config::client is null");
            }
            return create_export_pipeline(std::make_unique<s3_storage_sink>(std::move(cfg.client), std::move(cfg.object_name)));
        }
    }, target_config);
}

// Factory function to create source pipeline. Depending on target_config it will be either
// - in_memory_target_config - in-memory source pipeline for testing.
// - s3_target_config - pipeline that will read from S3 object.
std::unique_ptr<import_pipeline_interface> create_source_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, std::function<seastar::future<>(rjson::value)> on_item) {
    auto source = std::visit(overloaded_functor{
        [&](in_memory_target_config &cfg) -> std::unique_ptr<source_interface> {
            if (!cfg.storage) {
                on_internal_error(elogger, "in_memory_target_config::storage is null");
            }
            return std::make_unique<in_memory_source>(cfg.storage);
        },
        [&](s3_target_config &cfg) -> std::unique_ptr<source_interface> {
            if (!cfg.client) {
                on_internal_error(elogger, "s3_target_config::client is null");
            }
            return std::make_unique<s3_storage_source>(std::move(cfg.client), std::move(cfg.object_name), cfg.abort_source);
        }
    }, target_config);
    auto decompressor = create_decompression_pipeline(std::move(on_item));
    return std::make_unique<import_pipeline_impl>(std::move(source), std::move(decompressor));
}

} // namespace alternator
