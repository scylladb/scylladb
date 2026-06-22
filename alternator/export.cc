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
#include <algorithm>
#include <array>
#include <cstring>
#include <string>
#include <string_view>
#include <zlib.h>

namespace alternator {

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

// Gzip compressor - compresses data using gzip format and writes compressed chunks to the storage sink.
// Not every call to compress() produces output; zlib may buffer data internally.
// All data is guaranteed to be flushed when flush_and_close() is called.
class gzip_compressor : public compression_interface {
    std::unique_ptr<storage_sink_interface> _sink;
    z_stream _zs;
    static constexpr size_t _buf_size = 4096;

public:
    explicit gzip_compressor(std::unique_ptr<storage_sink_interface> sink)
        : _sink(std::move(sink)) {
        memset(&_zs, 0, sizeof(_zs));
        if (deflateInit2(&_zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            throw std::bad_alloc();
        }
    }

    ~gzip_compressor() {
        deflateEnd(&_zs);
    }

    seastar::future<> compress(std::span<const std::byte> data) override {
        _zs.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(data.data()));
        _zs.avail_in = static_cast<uInt>(data.size());

        do {
            std::array<std::byte, _buf_size> output;
            _zs.next_out = reinterpret_cast<Bytef*>(output.data());
            _zs.avail_out = _buf_size;

            int ret = deflate(&_zs, Z_NO_FLUSH);
            if (ret < Z_OK) {
                throw std::runtime_error("gzip compression error");
            }

            auto produced = _buf_size - _zs.avail_out;
            if (produced > 0) {
                co_await _sink->write(std::span<const std::byte>(output.data(), produced));
            }
        } while (_zs.avail_in > 0 || _zs.avail_out == 0);
    }

    seastar::future<> flush_and_close() override {
        int ret;
        do {
            std::array<std::byte, _buf_size> output;
            _zs.next_out = reinterpret_cast<Bytef*>(output.data());
            _zs.avail_out = _buf_size;
            _zs.next_in = nullptr;
            _zs.avail_in = 0;

            ret = deflate(&_zs, Z_FINISH);
            if (ret < Z_OK) {
                throw std::runtime_error("gzip compression flush error");
            }

            auto produced = _buf_size - _zs.avail_out;
            if (produced > 0) {
                co_await _sink->write(std::span<const std::byte>(output.data(), produced));
            }
        } while (ret != Z_STREAM_END);

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

// Gzip decompressor - decompresses gzip data and passes decompressed chunks to the parser.
class gzip_decompressor : public decompression_interface {
    std::unique_ptr<parsing_interface> _parser;
    z_stream _zs;
    static constexpr size_t _buf_size = 4096;

public:
    explicit gzip_decompressor(std::unique_ptr<parsing_interface> parser)
        : _parser(std::move(parser)) {
        memset(&_zs, 0, sizeof(_zs));
        if (inflateInit2(&_zs, 16 + MAX_WBITS) != Z_OK) {
            throw std::bad_alloc();
        }
    }

    ~gzip_decompressor() {
        inflateEnd(&_zs);
    }

    seastar::future<> decompress(std::span<const std::byte> data) override {
        _zs.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(data.data()));
        _zs.avail_in = static_cast<uInt>(data.size());

        do {
            std::array<std::byte, _buf_size> output;
            _zs.next_out = reinterpret_cast<Bytef*>(output.data());
            _zs.avail_out = _buf_size;

            int ret = inflate(&_zs, Z_NO_FLUSH);
            if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
                throw std::runtime_error("gzip decompression error");
            }

            auto produced = _buf_size - _zs.avail_out;
            if (produced > 0) {
                co_await _parser->parse(std::span<const std::byte>(output.data(), produced));
            }

            if (ret == Z_STREAM_END) {
                co_return;
            }
        } while (_zs.avail_in > 0 || _zs.avail_out == 0);
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

/// Writes data to an S3 object. Each write() call uploads data immediately.
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
        co_await _upload_stream.flush();
        co_await _upload_stream.close();
    }
};

/// Reads data from an S3 object and feeds it through a decompression_interface.
/// read() streams the entire object, calling decompress() for each chunk,
/// then calls decompression_interface::flush_and_close().
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

    seastar::future<> read() override {
        auto input = seastar::input_stream<char>(
            _client->make_download_source(_object_name));
        while (true) {
            auto buf = co_await input.read();
            if (buf.empty()) {
                break;
            }
            co_await _decompressor->decompress(
                std::span<const std::byte>(reinterpret_cast<const std::byte*>(buf.get()), buf.size()));
        }
        co_await input.close();
    }
    seastar::future<> flush_and_close() override {
        co_await _decompressor->flush_and_close();
    }
};

static std::unique_ptr<compression_interface> make_compressor(compression_type compression, std::unique_ptr<storage_sink_interface> sink) {
    return std::visit([&](auto&& c) -> std::unique_ptr<compression_interface> {
        using T = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<T, gzip_compression>) {
            return std::make_unique<gzip_compressor>(std::move(sink));
        } else {
            return std::make_unique<noop_compressor>(std::move(sink));
        }
    }, compression);
}

static std::unique_ptr<decompression_interface> make_decompressor(compression_type compression, std::unique_ptr<parsing_interface> parser) {
    return std::visit([&](auto&& c) -> std::unique_ptr<decompression_interface> {
        using T = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<T, gzip_compression>) {
            return std::make_unique<gzip_decompressor>(std::move(parser));
        } else {
            return std::make_unique<noop_decompressor>(std::move(parser));
        }
    }, compression);
}

static std::unique_ptr<export_pipeline_interface> create_export_pipeline(std::unique_ptr<storage_sink_interface> sink, compression_type compression) {
    auto compressor = make_compressor(compression, std::move(sink));
    return std::make_unique<json_formatter>(std::move(compressor));
}

static std::unique_ptr<decompression_interface> create_decompression_pipeline(std::function<seastar::future<>(rjson::value)> on_item, compression_type compression) {
    auto parser = std::make_unique<json_parser>(std::move(on_item));
    return make_decompressor(compression, std::move(parser));
}

// Factory function to create in-memory sink pipeline for testing.
std::unique_ptr<export_pipeline_interface> create_in_memory_sink_pipeline(in_memory_test_storage& storage, compression_type compression) {
    auto sink = std::make_unique<in_memory_storage_sink>(storage);
    return create_export_pipeline(std::move(sink), compression);
}

// Factory function to create in-memory source pipeline for testing.
std::unique_ptr<import_pipeline_interface> create_in_memory_source_pipeline(in_memory_test_storage& storage, std::function<seastar::future<>(rjson::value)> on_item, compression_type compression) {
    auto decompressor = create_decompression_pipeline(std::move(on_item), compression);
    return std::make_unique<in_memory_source>(storage, std::move(decompressor));
}

// Create s3 sink pipeline for a single file.
std::unique_ptr<export_pipeline_interface> create_s3_sink_pipeline(seastar::shared_ptr<s3::client> client, seastar::sstring object_name, compression_type compression) {
    auto sink = std::make_unique<s3_storage_sink>(std::move(client), std::move(object_name));
    return create_export_pipeline(std::move(sink), compression);
}

// Create s3 source pipeline for a single file.
std::unique_ptr<import_pipeline_interface> create_s3_source_pipeline(seastar::shared_ptr<s3::client> client, seastar::sstring object_name, std::function<seastar::future<>(rjson::value)> on_item, compression_type compression) {
    auto decompressor = create_decompression_pipeline(std::move(on_item), compression);
    return std::make_unique<s3_storage_source>(std::move(client), std::move(object_name), std::move(decompressor));
}

} // namespace alternator
