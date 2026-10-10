/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "alternator/serialization.hh"
#include "alternator/system_distributed_helper.hh"
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
#include "utils/error_injection.hh"
#include "utils/s3/client.hh"
#include "service_permit.hh"
#include "utils/rjson.hh"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>
#include <zlib.h>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

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
    // The sink is taken by reference and moved in by the constructor - see gzip_compressor below
    // for why the stages of the sink pipeline take over the stage below them this way. Nothing
    // here can fail, so it is taken over right away.
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
    z_stream _zs;
    static constexpr size_t _buf_size = 4096;
    // Upper bound on the input handed to a single deflate() call - see compress().
    static constexpr size_t _max_input_size = 16 * 1024;

public:
    // Takes the sink over only once zlib is initialized. Initialization can fail - deflateInit2()
    // reports Z_MEM_ERROR and zlib's ~256 KB of internal state can fail to allocate - and a storage
    // sink destroyed without flush_and_close() is not something a destructor can deal with: the S3
    // one leaks its upload stream together with a started multipart upload (see ~s3_storage_sink()).
    // Taking the parameter by reference rather than by value leaves the sink - and with it the
    // responsibility to close it asynchronously - with the caller until that can no longer happen.
    explicit gzip_compressor(std::unique_ptr<storage_sink_interface>&& sink) {
        memset(&_zs, 0, sizeof(_zs));
        auto ret = deflateInit2(&_zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY);
        if (ret != Z_OK) {
            throw std::runtime_error(fmt::format("gzip compressor initialization error (deflateInit2 returned {}): {}", ret, _zs.msg ? _zs.msg : "<no message>"));
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

    seastar::future<> compress(std::span<const std::byte> data) override {
        // A single deflate() call is CPU bound and runs until it either consumes all of its input
        // or fills its output buffer, so its cost is bounded only by the smaller of the two. The
        // output buffer is small, but on compressible data - and serialized items are - a 4 KB
        // output can absorb megabytes of input, which is milliseconds of reactor stall in one
        // call. Feeding the input in slices bounds the work of each call regardless of the
        // compression ratio; a preemption point between the slices is what actually breaks the
        // stall up, and it has to be here rather than next to the sink write below, because a
        // slice may produce no output at all.
        while (!data.empty()) {
            auto chunk = data.first(std::min(data.size(), _max_input_size));
            data = data.subspan(chunk.size());
            _zs.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(chunk.data()));
            _zs.avail_in = static_cast<uInt>(chunk.size());

            do {
                std::array<std::byte, _buf_size> output;
                _zs.next_out = reinterpret_cast<Bytef*>(output.data());
                _zs.avail_out = _buf_size;

                int ret = deflate(&_zs, Z_NO_FLUSH);
                if (ret < Z_OK) {
                    throw std::runtime_error(fmt::format("gzip compression error (deflate returned {}): {}", ret, _zs.msg ? _zs.msg : "<no message>"));
                }

                auto produced = _buf_size - _zs.avail_out;
                if (produced > 0) {
                    co_await _sink->write(std::span<const std::byte>(output.data(), produced));
                }
            } while (_zs.avail_in > 0 || _zs.avail_out == 0);

            co_await coroutine::maybe_yield();
        }
    }

    seastar::future<export_pipeline_interface::result> flush_and_close() override {
        int ret;
        std::exception_ptr exception = nullptr;
        try {
            utils::get_local_injector().inject("alternator_export_to_s3_in_s3_gzip_compressor_flush_and_close",
                    [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_s3_gzip_compressor_flush_and_close"); });

            do {
                std::array<std::byte, _buf_size> output;
                _zs.next_out = reinterpret_cast<Bytef*>(output.data());
                _zs.avail_out = _buf_size;
                _zs.next_in = nullptr;
                _zs.avail_in = 0;

                ret = deflate(&_zs, Z_FINISH);
                if (ret < Z_OK) {
                    throw std::runtime_error(fmt::format("gzip compression flush error (deflate returned {}): {}", ret, _zs.msg ? _zs.msg : "<no message>"));
                }

                auto produced = _buf_size - _zs.avail_out;
                if (produced > 0) {
                    co_await _sink->write(std::span<const std::byte>(output.data(), produced));
                }
                else {
                    co_await coroutine::maybe_yield();
                }
            } while (ret != Z_STREAM_END);
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
    z_stream _zs;
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
            throw std::runtime_error(fmt::format("gzip decompression error (inflateReset returned {}): {}", res, _zs.msg ? _zs.msg : "<no message>"));
        }
        _stream_ended = false;
    }

public:
    explicit gzip_decompressor(std::unique_ptr<parsing_interface> parser)
        : _parser(std::move(parser)) {
        memset(&_zs, 0, sizeof(_zs));
        auto res = inflateInit2(&_zs, 16 + MAX_WBITS);
        if (res != Z_OK) {
            throw std::runtime_error(fmt::format("gzip decompression error (inflateInit2 returned {}): {}", res, _zs.msg ? _zs.msg : "<no message>"));
        }
    }
    gzip_decompressor(const gzip_decompressor&) = delete;
    gzip_decompressor(gzip_decompressor&&) = delete;
    gzip_decompressor& operator=(const gzip_decompressor&) = delete;
    gzip_decompressor& operator=(gzip_decompressor&&) = delete;

    ~gzip_decompressor() {
        inflateEnd(&_zs);
    }

    seastar::future<> decompress(std::span<const std::byte> data) override {
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
                throw api_error::validation(fmt::format("gzip decompression error: {}", _zs.msg ? _zs.msg : "<no message>"));
            }

            auto produced = _buf_size - _zs.avail_out;
            if (produced > 0) {
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

            // The parser only yields once it has emitted a complete item, so a highly compressible
            // chunk that inflates into a long run without a newline in it would otherwise keep
            // inflating and appending - up to the parser's line size limit - in a single task.
            // The input comes from the bucket, so it is not ours to trust with the reactor.
            co_await coroutine::maybe_yield();
        } while (_zs.avail_in > 0 || _zs.avail_out == 0);
    }
    void cancel() noexcept override {
        inflateEnd(&_zs);
        memset(&_zs, 0, sizeof(_zs));
        _cancelled = true;
        _parser->cancel();
    }

    seastar::future<> close() override {
        // We consider stream of size 0 (_no_input_yet set to true) as valid, not truncated and empty stream
        // (this is done because AWS can produce .gz files of size 0 for empty partitions).
        if (_cancelled || _stream_ended || _no_input_yet) {
            co_return co_await _parser->close();
        }
        // Cancel the parser (to prevent any complaining from it) before throwing the error.
        _parser->cancel();
        throw api_error::validation("gzip decompression error: truncated gzip stream");
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
    shared_ptr<s3::client> _client;
    sstring _object_name;
    // Filled by the upload sink with the etag S3 assigned to the object it wrote - see
    // flush_and_close(). Shared with the sink, which outlives this object when the
    // destructor below has to leak it.
    lw_shared_ptr<sstring> _etag = make_lw_shared<sstring>();
    std::unique_ptr<output_stream<char>> _upload_stream;
    sink_result_builder _result;
    abort_source *_as;
    bool _closed = false;

public:
    s3_storage_sink(shared_ptr<s3::client> client, sstring object_name, abort_source *as)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _upload_stream(std::make_unique<output_stream<char>>(_client->make_upload_jumbo_sink(_object_name, s3::object_metadata{}, std::nullopt, as, _etag)))
        , _as(as)
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
        utils::get_local_injector().inject("alternator_export_to_s3_in_s3_storage_sink_write",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_s3_storage_sink_write"); });
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

        // We need to call `_upload_stream->close()` before error injection, otherwise it will not be closed
        // and we will die on assert in it's destructor.
        utils::get_local_injector().inject("alternator_export_to_s3_in_s3_storage_sink_flush_and_close",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_s3_storage_sink_flush_and_close"); });

        // The sink uploads the object in multiple parts, so its S3 etag is not the md5 of the
        // content that sink_result_builder computed - it is the md5 of the concatenated part
        // md5s with the part count appended. Only S3 knows how the data was split into parts,
        // so ask it for the etag instead of trying to reproduce it here.
        auto info = co_await _client->get_object_info(_object_name, _as);
        
        co_return _result.finalize(sstring(strip_etag_quotes(info.etag)));
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

static std::unique_ptr<decompression_interface> create_decompression_pipeline(std::function<seastar::future<>(rjson::value)> on_item, compression_type compression) {
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
        utils::get_local_injector().inject("alternator_export_to_s3_in_s3_create_sink_pipeline_before_sink_creation",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_s3_create_sink_pipeline_before_sink_creation"); });
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
        utils::get_local_injector().inject("alternator_export_to_s3_in_s3_create_sink_pipeline_before_compressor_creation",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_s3_create_sink_pipeline_before_compressor_creation"); });
        compressor = make_compressor(compression, std::move(sink));
        utils::get_local_injector().inject("alternator_export_to_s3_in_s3_create_sink_pipeline_after_compressor_creation",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_in_s3_create_sink_pipeline_after_compressor_creation"); });
        co_return std::make_unique<json_formatter>(std::move(compressor));
    } catch(...) {
        exception = std::current_exception();
    }

    // Closing the pipeline on this path finalizes the target object, so a failed export can leave an
    // empty (with gzip: header-and-trailer only) object behind. That is still better than leaking the
    // multipart upload, and the caller learns about the failure from the exception rethrown below.
    // Note that such an object reads back as a valid, empty export rather than as a failure: a
    // header-and-trailer-only member ends with Z_STREAM_END like any other, and an object with no bytes
    // at all is accepted as an empty stream by gzip_decompressor::close(). The caller must therefore not
    // take the presence of the object for success. See the contract on create_sink_pipeline() in export.hh.
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

// --- Export orchestration helpers ---

// This uniquely identifies a node incarnation and changes on reboot.
live_node_identifier executor::get_self_node_id() {
    auto host_id = _gossiper.my_host_id();
    auto ep_state = _gossiper.get_this_endpoint_state_ptr();
    auto generation = ep_state->get_heart_beat_state().get_generation();
    return live_node_identifier{ .host_id = fmt::to_string(host_id), .gossip_generation = std::uint32_t(generation.value()) };
}

future<std::unordered_set<live_node_identifier>> executor::get_live_nodes() {
    auto live_members = _gossiper.get_live_members();
    std::unordered_set<live_node_identifier> result;
    result.reserve(live_members.size());
    for (const auto& host_id : live_members) {
        auto ep_state = _gossiper.get_endpoint_state_ptr(host_id);
        if (!ep_state) {
            continue;
        }
        auto generation = ep_state->get_heart_beat_state().get_generation();
        result.insert(live_node_identifier{ .host_id = fmt::to_string(host_id), .gossip_generation = std::uint32_t(generation.value()) });
    }
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
// Format: arn:scylla:alternator:{keyspace_name}:scylla:table/{table_name}/export/{epoch_millis_zero_padded}-{16_alphanumeric_chars}
static sstring generate_export_arn(std::string_view keyspace_name, std::string_view table_name) {
    auto epoch_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto random_suffix = generate_random_alphanumeric(16);
    return fmt::format("arn:scylla:alternator:{}:scylla:table/{}/export/{:019d}-{}",
        keyspace_name, table_name, epoch_millis, random_suffix);
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

// Build an ExportDescription JSON response from a CQL result row.
static rjson::value build_export_description_response(const export_row& row) {
    rjson::value export_desc = rjson::empty_object();

    auto export_arn = row.export_arn;
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));
    auto table_arn = get_table_arn_from_export_arn(export_arn);
    rjson::add(export_desc, "TableArn", rjson::from_string(table_arn));

    rjson::add(export_desc, "ExportStatus", rjson::from_string(row.export_status));

    if (!row.export_manifest.empty()) {
        rjson::add(export_desc, "ExportManifest", rjson::from_string(row.export_manifest));
    }

    if (row.accepted_at != db_clock::time_point{}) {
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
            row.accepted_at.time_since_epoch()).count();
        rjson::add(export_desc, "StartTime", rjson::value(seconds));
        rjson::add(export_desc, "ExportTime", rjson::value(seconds));
    }
    if (row.completed_at != db_clock::time_point{}) {
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
            row.completed_at.time_since_epoch()).count();
        rjson::add(export_desc, "EndTime", rjson::value(seconds));
        rjson::add(export_desc, "ItemCount", rjson::value(row.item_count));
        // BilledSizeBytes - we always report 0 (not applicable for Scylla)
        rjson::add(export_desc, "BilledSizeBytes", rjson::value(int64_t(0)));
    }
    if (!row.failure_code.empty()) {
        rjson::add(export_desc, "FailureCode", rjson::from_string(row.failure_code));
    }
    if (!row.failure_message.empty()) {
        rjson::add(export_desc, "FailureMessage", rjson::from_string(row.failure_message));
    }
    if (!row.client_token.empty() && row.client_token.starts_with("u:")) {
        rjson::add(export_desc, "ClientToken", rjson::from_string(row.client_token.substr(2)));
    }

    // Parse the stored request to extract S3 bucket/prefix/format info
    if (!row.request.empty()) {
        try {
            auto req_json = rjson::parse(row.request);
            if (auto v = rjson::find(req_json, "ExportFormat")) {
                rjson::add(export_desc, "ExportFormat", rjson::from_string(rjson::to_string_view(*v)));
            }
            else {
                rjson::add(export_desc, "ExportFormat", rjson::from_string("DYNAMODB_JSON"));
            }
            if (auto v = rjson::find(req_json, "ExportType")) {
                rjson::add(export_desc, "ExportType", rjson::from_string(rjson::to_string_view(*v)));
            }
            if (auto v = rjson::find(req_json, "S3Bucket")) {
                rjson::add(export_desc, "S3Bucket", rjson::from_string(rjson::to_string_view(*v)));
            }
            if (auto v = rjson::find(req_json, "S3Prefix")) {
                auto prefix = rjson::to_string_view(*v);
                if (!prefix.empty()) {
                    rjson::add(export_desc, "S3Prefix", rjson::from_string(prefix));
                }
            }
        } catch (...) {
            // If we can't parse the stored request, just skip these fields
        }
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    return response;
}

future<> executor::garbage_collect_s3_exports() {
    auto client_tokens = co_await get_all_client_tokens(_qp);
    auto exports = co_await get_all_exports(_qp);
    auto live_nodes = co_await get_live_nodes();
    auto self_node_id = get_self_node_id();

    xlogger.debug("Garbage collection: {} client tokens, {} exports, {} live nodes, self node_id={}",
        client_tokens.size(), exports.size(), live_nodes.size(), self_node_id);
        
    auto is_node_alive = [&](const live_node_identifier& node_id) {
        return live_nodes.find(node_id) != live_nodes.end();
    };

    std::unordered_map<sstring, export_row&> export_arn_to_export;
    for (auto& export_row : exports) {
        // We ignore rows, which are under control of node, which is alive, those are fine.
        if (!is_node_alive(export_row.node_id)) {
            xlogger.debug("Export row {} is under control of dead node {}", export_row.export_arn, export_row.node_id);
            export_arn_to_export.insert({ export_row.export_arn, export_row });
        }
    }
    for(auto &ct : client_tokens) {
        // We ignore rows, which are under control of node, which is alive, those are fine.
        if (is_node_alive(ct.node_id)) continue;

        // Handle special case, where node died before writing export row (so it won't show up in export_rows)
        xlogger.debug("Client token row {} is under control of dead node {}", ct.client_token, ct.node_id);
        auto it = export_arn_to_export.find(ct.export_arn);
        if (it == export_arn_to_export.end()) {
            // Node died before writing the export row, let's fill in the missing row with `FAILED` status.
            // The export itself never happened, so there's nothing to clean up here on S3.

            xlogger.debug("Client token row {} has no corresponding export row, creating a FAILED export row", ct.client_token);
            // Parse user request from client token row.
            rjson::value request;
            try {
                request = rjson::parse(ct.request);
            } catch (...) {
                xlogger.debug("Failed to parse request from client token row {}: {}", ct.client_token, ct.request);
                // This can't happen in normal operation.
                // If we can't parse the request, we will invent data on the fly.
                request = rjson::empty_object();
            }

            xlogger.debug("Creating FAILED export row for client token {}: export_arn={}", ct.client_token, ct.export_arn);
            co_await insert_export(_qp, {
                .export_arn = ct.export_arn,
                .client_token = ct.client_token,
                .request = ct.request,
                .export_status = "FAILED",
                .failure_code = "NodeFailure",
                .failure_message = "The node that initiated the export has failed before the export could start.",
                .export_id_token = "",
                .accepted_at = db_clock::now(),
                .completed_at = db_clock::time_point{},
                .node_id = self_node_id,
            });
        }
    }

    for(auto &export_row : exports) {
        if (is_node_alive(export_row.node_id)) {
            // Node is alive, nothing to collect here.
            continue;
        }

        if (export_row.export_status == "COMPLETED" || export_row.export_status == "FAILED") {
            // The export completed - nothing to collect.
            xlogger.debug("Export row {} is already in terminal state {}, skipping", export_row.export_arn, export_row.export_status);
            continue;
        }

        // Either the export is in progress or is in progress of cleaning up, in both cases the node handling it is dead.
        // We take over by first updating `node_id` and `export_status` to show our ownership and a new state.

        auto new_export_row = export_row;
        new_export_row.export_status = "FAILING";
        new_export_row.node_id = self_node_id;
        xlogger.debug("Marking export row {} as FAILING", export_row.export_arn);

        if (!co_await update_export(_qp, new_export_row, export_row.export_status, export_row.node_id)) {
            // Other node is already handling it and is quicker, so we just skip this.
            xlogger.debug("Failed to update export row {} to FAILING, it may have been updated by another node", export_row.export_arn);
            continue;
        }

        // TODO: update TTLs
        // Note: we purposely don't clean up S3 data - the user needs to do it on it's own (maybe we should?).

        xlogger.debug("Marking export row {} as FAILED", export_row.export_arn);
        auto failing_export_status = std::exchange(new_export_row.export_status, "FAILED");
        if (!co_await update_export(_qp, new_export_row, failing_export_status, new_export_row.node_id)) {
            // This should never happen:
            // - either there was some write issue and node_id / export_status were not correctly updated, or
            // - another node took over and updated the row to a different status.
            // In both cases we can't do anything about it, so we just skip - either the other node will handle it (case 2) or
            // we will try again on next time of garbage collection.
            xlogger.debug("Failed to update export row {} to FAILED, it may have been updated by another node", export_row.export_arn);
            continue;
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

future<> executor::wait_for_next_s3_exports_gc_round() {
    auto& injector = utils::get_local_injector();
    auto deadline = lowres_clock::now() + s3_exports_gc_period;
    // With error injection compiled in we sleep in short slices, so a garbage collector,
    // which is already sleeping, notices `s3_exports_gc_wakeup_injection` being enabled.
    // Without error injection we sleep for the whole period at once.
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
    constexpr lowres_clock::duration max_slice = std::chrono::milliseconds(10);
#else
    constexpr lowres_clock::duration max_slice = s3_exports_gc_period;
#endif
    for (;;) {
        if (injector.is_enabled(s3_exports_gc_wakeup_injection)) {
            // Messages are not shared, so every message triggers exactly one round
            // (and a message sent before we started waiting is not lost).
            co_await injector.inject(s3_exports_gc_wakeup_injection,
                    utils::wait_for_message(s3_exports_gc_period, &_export_abort_source), false);
            co_return;
        }
        auto now = lowres_clock::now();
        if (now >= deadline) {
            co_return;
        }
        co_await seastar::sleep_abortable(std::min<lowres_clock::duration>(deadline - now, max_slice), _export_abort_source);
    }
}

future<> executor::garbage_collector_for_s3_exports() {
    auto holder = _export_gate.try_hold();
    if (!holder) {
        // The executor is stopping.
        co_return;
    }
    while (!_export_abort_source.abort_requested()) {
        try {
            co_await garbage_collect_s3_exports();
            co_await wait_for_next_s3_exports_gc_round();
            utils::get_local_injector().enter(s3_exports_gc_finished_injection);
        } catch (const abort_requested_exception&) {
            break;
        } catch (...) {
            xlogger.warn("Garbage collection of S3 exports failed: {}", std::current_exception());
        }
    }
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

    // AWS checks client token duplication first, so we will validate those later
    auto export_format = get_non_empty_string_attribute(request, "ExportFormat", "DYNAMODB_JSON");
    auto export_type = get_non_empty_string_attribute(request, "ExportType", "");

    // ExportTime - only "now" (or close to now) is supported
    // If not specified, use current time. If specified, must be within 5 minutes of now.
    auto now = (double)std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    auto user_client_token = get_non_empty_string_attribute(request, "ClientToken", "");

    auto node_id = get_self_node_id();

    // Canonicalize the request for idempotency checking
    auto canonical_request = canonicalize_request(request);

    // Handle ClientToken: prefix with "u:" for user-supplied, "r:" for random
    sstring client_token;
    if (user_client_token.empty()) {
        client_token = fmt::format("r:{}", generate_random_alphanumeric(16));
    } else {
        client_token = fmt::format("u:{}", user_client_token);
    }

    // Generate export_id_token (random identifier used in S3 key paths)
    auto export_id_token = generate_random_alphanumeric(16);

    // Generate a unique export ARN
    auto export_arn = generate_export_arn(parts.keyspace_name, parts.table_name);

    // Build the S3 key prefix for this export
    auto s3_key_prefix = s3_prefix.empty()
        ? fmt::format("AWSDynamoDB/{}/", export_id_token)
        : fmt::format("{}/AWSDynamoDB/{}/", s3_prefix, export_id_token);

    auto accepted_at = db_clock::now();

    // Start a garbage collection thread on shard 0 if it's not already running.
    // We need to do this before first writing a client token row to make sure
    // if something goes wrong during export the written rows will be clean up eventually.
    co_await container().invoke_on(0, [](executor &ex) {
        if (ex._garbage_collection_thread_for_s3_export_running) return;
        ex._garbage_collection_thread_for_s3_export_running = true;
        (void)ex.garbage_collector_for_s3_exports();

    });

    // We have all we need, let's publish the export request to the database.
    // We need to do this in two steps - first insert a client token row, then insert the export metadata row.
    // The order matters, because it's possible that another node is running the export with the same client token,
    // in which case we need to handle one of such requests as duplicate (we can't start two exports with the same client token).
    // We check for client token in client token table first (and we insert the row first as well),
    // only one node can succeed in inserting the client token row (insert-if-not-exists mechanic) and that node will
    // continue to run the export.
    auto client_token_inserted = co_await insert_client_row(_qp, client_row{
        .client_token = client_token,
        .export_arn = export_arn,
        .request = canonical_request,
        .node_id = node_id,
    });
    auto build_export_row = [&]() {
        return export_row{
            .export_arn = export_arn,
            .client_token = client_token,
            .request = canonical_request,
            .export_status = "IN_PROGRESS",
            .export_id_token = export_id_token,
            .accepted_at = accepted_at,
            .completed_at = db_clock::time_point{},
            .node_id = node_id,
        };
    };

    utils::get_local_injector().inject("alternator_export_to_s3_after_client_token_insertion",
            [] { throw std::runtime_error("injected failure alternator_export_to_s3_after_client_token_insertion"); });

    if (!client_token_inserted) {
        // Export with this client token already exists. We need to check if request is the same or different.
        // If the same - return the export is in progress (or completed or failed - the state is in the row itself) as if DescribeExport was called.
        // If not the same - return export conflict error as Amazon requires us to do.

        xlogger.debug("ClientToken {} already exists", client_token);
        auto existing_client_row = co_await get_client_row(_qp, client_token);
        if (!existing_client_row) {
            // The row existed a moment ago, but is gone now (expired or garbage collected in the meantime).
            // Treat it as a new export and continue.
            xlogger.debug("ClientToken {} row disappeared, returning IN_PROGRESS status", client_token);
            auto response = build_export_description_response(build_export_row());
            co_return rjson::print(std::move(response));
        }
        auto& client_row = *existing_client_row;
        if (canonical_request != client_row.request) {
            // Easy - different request, so we fail with export conflict as Amazon requires.
            xlogger.debug("Export conflict: different parameters");
            co_return api_error::export_conflict("Duplicate request detected - an export with this ClientToken already exists with different parameters");
        }

        // We need an export row for this client token - the export might be in progress or completed or failed.
        auto export_row = co_await get_export(_qp, client_row.export_arn);
        xlogger.debug("Export row for ClientToken {}: {}", client_token, export_row ? "found" : "not found");
        if (!export_row) {
            // Export row doesn't exist. Two possibilities:
            // - the other node is doing the export in the same moment and hasn't inserted the export row yet (but managed
            //   to insert client token row first), or
            // - the other node doing the export died before inserting the export row, leaving a dangling client token row
            //   (it's possible the node is gone but we don't know it yet, so we can't be sure).
            // In both cases we will return IN_PROGRESS status - the dangling client token row will be taken care of by
            // garbage collection thread.
            xlogger.debug("Returning IN_PROGRESS status for ClientToken {}", client_token);
            export_row = build_export_row();
        }
        else {
            // Export row exists, other node is (or was) running the export. Return as if DescribeExport was called.
            xlogger.debug("Returning existing export description for ClientToken {}", client_token);
        }
        auto response = build_export_description_response(*export_row);
        co_return rjson::print(std::move(response));
    }

    if (export_format != "DYNAMODB_JSON") {
        co_return api_error::validation(
                fmt::format("ExportFormat attribute: must be DYNAMODB_JSON, not `{}`", export_format));
    }

    if (!export_type.empty() && export_type != "FULL_EXPORT") {
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

    auto erow = build_export_row();
    auto export_row_inserted = co_await insert_export(_qp, erow);

    try {
        utils::get_local_injector().inject("alternator_export_to_s3_after_export_row_insertion",
                [] { throw std::runtime_error("injected failure alternator_export_to_s3_after_export_row_insertion"); });

        if (!export_row_inserted) {
            xlogger.debug("Unexpectedly failed to insert export row for ClientToken {} and ExportArn {}", client_token, export_arn);
            on_internal_error(xlogger, fmt::format("Failed to insert export row for ClientToken {} and ExportArn {}", client_token, export_arn));
        }

        // Launch background export fiber (fire-and-forget, gate-guarded)
        xlogger.debug("Launching background export fiber for ClientToken {}", client_token);
        (void)run_export(_export_gate.hold(), schema, erow, table_arn, s3_bucket, s3_prefix, s3_key_prefix, export_id_token);

        // Build the ExportDescription response
        xlogger.debug("Returning IN_PROGRESS response for ClientToken {}", client_token);
        auto response = build_export_description_response(erow);
        co_return rjson::print(std::move(response));
    }
    catch(std::exception &e) {
        erow.failure_code = "Exception";
        erow.failure_message = e.what();
    } catch(...) {
        erow.failure_code = "Exception";
        erow.failure_message = "Unknown error";
    }
    erow.export_status = "FAILED";
    auto updated = co_await update_export(_qp, erow, "IN_PROGRESS", node_id);
    if (!updated) {
        xlogger.debug("Export {} failed to update status to FAILED after completion", erow.export_arn);
    }
    else {
        xlogger.debug("Export {} failed during starting up phase", erow.export_arn);
    }
    auto response = build_export_description_response(erow);
    co_return rjson::print(std::move(response));
}

// How long a single page read is allowed to take. We allow for some more time than
// `executor::default_timeout()` as we expect this bulk background job
// bypassing the cache will be more expensive than an interactive request.
static constexpr auto export_scan_table_page_timeout = std::chrono::minutes(1);

seastar::future<> export_scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    abort_source& as,
    service_permit permit,
    seastar::noncopyable_function<seastar::future<>(rjson::value)> cb)
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
    service::query_state query_state(client_state, trace_state, std::move(permit));

    // LOCAL_QUORUM, unless the local datacenter has a single node - then LOCAL_ONE.
    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;
    {
        auto tmptr = proxy.get_token_metadata_ptr();
        const auto& topo = tmptr->get_topology();
        const auto& dc_endpoints = topo.get_datacenter_endpoints();
        auto it = dc_endpoints.find(topo.get_datacenter());
        if (it != dc_endpoints.end() && it->second.size() == 1) {
            cl = db::consistency_level::LOCAL_ONE;
        }
    }

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
            query::tombstone_limit(proxy.get_tombstone_limit()));

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

    while (!pager->is_exhausted()) {
        as.check();
        std::unique_ptr<cql3::result_set> rs;

        // How long we are willing to keep retrying the same page, and how the wait
        // between the attempts grows. The budget is a wall-clock one on purpose: the
        // things we retry for - a replica restarting, a topology change moving the data
        // around - routinely take minutes, while an attempt takes an unknown time, so a
        // budget counted in attempts would say little about how long we really wait.
        static constexpr auto max_retry_time = std::chrono::minutes(10);
        static constexpr auto initial_retry_delay = std::chrono::seconds(1);
        static constexpr auto max_retry_delay = std::chrono::seconds(30);
        auto retry_delay = initial_retry_delay;
        auto give_up_at = seastar::lowres_clock::now() + max_retry_time;

        // A read failure gets a much shorter leash than the wall-clock budget above,
        // see where we give up on it below for why.
        static constexpr int max_read_failures = 3;
        int read_failures = 0;

        // We will try to read a page several times to avoid an accidental transient
        // failure aborting whole scan. A full table scan runs for minutes or hours, so a
        // replica restarting or briefly falling behind - reported as an unavailable or a
        // read failure error, not only as a timeout - is just as likely as a timeout and
        // just as transient. The same goes for an overloaded node: the scan bypasses the
        // cache and pages as fast as the callback allows, so it is a likely victim of
        // that. We abort on any other error.
        for (int attempts = 1; ; ++attempts) {
            std::exception_ptr failure;
            try {
                rs = co_await pager->fetch_page(export_scan_table_page_size, gc_clock::now(),
                        db::timeout_clock::now() + export_scan_table_page_timeout);
                break;
            } catch(exceptions::read_timeout_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read timed out: {}", failure);
            } catch(exceptions::read_failure_exception&) {
                failure = std::current_exception();
                ++read_failures;
                xlogger.warn("S3 export scanner read failed on a replica: {}", failure);
            } catch(exceptions::unavailable_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read found too few replicas available: {}", failure);
            } catch(exceptions::overloaded_exception&) {
                failure = std::current_exception();
                xlogger.warn("S3 export scanner read was rejected, the node is overloaded: {}", failure);
            }
            // A read already in flight cannot be cancelled, so an abort requested while we
            // were reading is only noticed now. Check it before anything else can turn the
            // read's failure into the scan's result: the caller asked us to stop, and that
            // is what the scan has to report, not the read error we happened to get or the
            // sleep_aborted from the backoff below.
            as.check();
            // The usual reasons for a replica reporting a failure for this very read - an
            // sstable which cannot be read, a read exceeding the replica's memory kill
            // limit - are a property of the data, so waiting does not change the answer.
            // Some of them are transient after all (a replica losing its connection
            // mid-read is reported the same way), so we do retry, but only a couple of
            // times instead of spending the whole wall-clock budget.
            if (read_failures >= max_read_failures) {
                xlogger.error("S3 export scanner giving up after {} attempts, {} of which were replica read failures - "
                        "such a failure is usually caused by the data being read and waiting does not fix it: {}",
                        attempts, read_failures, failure);
                std::rethrow_exception(std::move(failure));
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
            try {
                co_await seastar::sleep_abortable(retry_delay, as);
            } catch (const seastar::sleep_aborted&) {
                // An abort landing during the backoff is reported by sleep_abortable() as a
                // freshly made sleep_aborted whenever the abort carried no exception of its
                // own, i.e. for a plain request_abort(). Everywhere else the scan fails with
                // the very exception `as` holds, so report this abort the same way - a caller
                // comparing against as.abort_requested_exception_ptr() by identity, the way
                // abort_source itself suggests, should not care where the abort landed.
                as.check();
                throw;
            }
            retry_delay = std::min(retry_delay * 2, max_retry_delay);
            // The pager which failed cannot be reused, start a fresh one resumed
            // from the page we last read successfully.
            make_pager(last_page_state);
        }
        last_page_state = pager->state(std::nullopt);

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

// Manifest files report times as ISO 8601 UTC with millisecond precision,
// e.g. "2020-11-04T07:28:34.028Z".
static sstring format_manifest_time(db_clock::time_point tp) {
    auto millis = tp.time_since_epoch().count() % 1000;
    return fmt::format("{:%FT%T}.{:03}Z", fmt::gmtime(db_clock::to_time_t(tp)), millis);
}

future<> executor::run_export(
        seastar::gate::holder gate_holder,
        schema_ptr schema,
        export_row row,
        std::string table_arn,
        std::string s3_bucket,
        std::string s3_prefix,
        std::string s3_key_prefix,
        std::string export_id_token) {
    int64_t item_count = 0;
    auto node_id = get_self_node_id();
    
    auto [ error_msg, error_code ] = co_await ([&]() -> future<std::tuple<std::string, std::string>> {
        try {
            auto client = get_s3_client(s3_bucket);
            // s3::client addresses objects as "/bucket/key".
            auto s3_object_name = [&s3_bucket](std::string_view key) {
                return fmt::format("/{}/{}", s3_bucket, key);
            };
            // Each manifest file is accompanied by a ".md5" object holding the base64 encoded md5 of its content.
            auto put_object_with_md5 = [&client, &s3_object_name](std::string_view json_key, std::string_view md5_key, std::string content) -> future<> {
                md5_hasher hasher;
                hasher.update(content.data(), content.size());
                auto md5_content = base64_encode(hasher.finalize());
                co_await client->put_object(s3_object_name(json_key), temporary_buffer<char>(content.data(), content.size()));
                co_await client->put_object(s3_object_name(md5_key), temporary_buffer<char>(md5_content.data(), md5_content.size()));
            };

            // Create the data file object key
            auto data_object_key = fmt::format("{}data/{}.json.gz", s3_key_prefix, export_id_token);

            utils::get_local_injector().inject("alternator_export_to_s3_before_create__started_object",
                    [] { throw std::runtime_error("injected failure alternator_export_to_s3_before_create__started_object"); });

            try {
                // Create empty _started object
                co_await client->put_object(s3_object_name(fmt::format("{}_started", s3_key_prefix)), temporary_buffer<char>());
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

            utils::get_local_injector().inject("alternator_export_to_s3_before_create_sink_pipeline",
                    [] { throw std::runtime_error("injected failure alternator_export_to_s3_before_create_sink_pipeline"); });
            
            // Create S3 sink pipeline
            std::exception_ptr exception;
            export_pipeline_interface::result export_result;

            // pipeline handling code is fragile in respect to clean up - the `flush_and_close()` method MUST BE called in all cases and
            // CANNOT be called via RAII (the method is asynchronous).
            // To ensure this we create and use pipeline wrapped in a try-catch block and straight after it we call `flush_and_close()`.
            std::unique_ptr<export_pipeline_interface> pipeline;

            try {
                pipeline = co_await create_sink_pipeline(s3_target_config{ client, s3_object_name(data_object_key), &_export_abort_source }, gzip_compression{});

                utils::get_local_injector().inject("alternator_export_to_s3_after_create_sink_pipeline",
                        [] { throw std::runtime_error("injected failure alternator_export_to_s3_after_create_sink_pipeline"); });

                // Scan the table and feed items through the pipeline
                co_await export_scan_table(_proxy, schema, _export_abort_source, empty_service_permit(), [&pipeline, &item_count](rjson::value item) -> future<> {
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
            rjson::add(summary, "endTime", rjson::from_string(format_manifest_time(row.completed_at)));
            rjson::add(summary, "tableArn", rjson::from_string(table_arn));
            rjson::add(summary, "tableId", rjson::from_string(fmt::to_string(schema->id())));
            // We don't support point-in-time exports from the past - the table is scanned
            // as of the moment the export was accepted.
            rjson::add(summary, "exportTime", rjson::from_string(format_manifest_time(row.accepted_at)));
            rjson::add(summary, "s3Bucket", rjson::from_string(s3_bucket));
            rjson::add(summary, "s3Prefix", rjson::from_string(s3_prefix));
            rjson::add(summary, "s3SseAlgorithm", rjson::from_string(""));
            rjson::add(summary, "s3SseKmsKeyId", rjson::from_string(""));
            rjson::add(summary, "manifestFilesS3Key", rjson::from_string(manifest_files_key));
            rjson::add(summary, "billedSizeBytes", rjson::value(int64_t(0)));
            rjson::add(summary, "itemCount", rjson::value(int64_t(row.item_count)));
            xlogger.debug("Processed item count: {}", item_count);
            rjson::add(summary, "outputFormat", "DYNAMODB_JSON");
            auto manifest_summary_content = rjson::print(summary);
            co_await put_object_with_md5(manifest_summary_key, fmt::format("{}manifest-summary.md5", s3_key_prefix), std::move(manifest_summary_content));

            auto export_manifest = fmt::format("{}manifest-summary.json", s3_key_prefix);
            row.item_count = item_count;
            row.export_status = "COMPLETED";
            row.export_manifest = export_manifest;
            auto updated = co_await update_export(_qp, row, "IN_PROGRESS", node_id);
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
        bool updated;
        try {
            updated = co_await update_export(_qp, row, "IN_PROGRESS", node_id);
        }
        catch(std::exception &e) {
            xlogger.debug("Export {}: failed to update status to FAILED: {}", row.export_arn, e.what());
            co_return;
        }
        catch(...) {
            // TODO: pull the exception type into message at least
            xlogger.debug("Export {}: failed to update status to FAILED: unknown exception", row.export_arn);
            co_return;
        }
        if (!updated) {
            xlogger.debug("Export {} failed to update status to FAILED after completion", row.export_arn);
            co_return;
        }
        xlogger.debug("Export {} failed with {} items", row.export_arn, item_count);
    }
    else {
        xlogger.debug("Export {} completed successfully with {} items", row.export_arn, row.item_count);
    }
}

future<executor::request_return_type> executor::describe_export(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_export++;

    auto export_arn = get_non_empty_string_attribute(request, "ExportArn");

    // Validate that the ARN looks like an export ARN.
    // Expected format: arn:scylla:alternator:<keyspace>:scylla:table/<name>/export/<id>
    // or AWS format: arn:aws:dynamodb:<region>:<account>:table/<name>/export/<id>
    if (!export_arn.starts_with("arn:")) {
        co_return api_error::validation(
            fmt::format("Invalid Export ARN: {}", export_arn));
    }

    try {
        // Amazon returns access denied rather than validation exception, when ARN is malformed
        // so try to parse it here and return correct error if parsing failed
        parse_arn(export_arn, "ExportArn", "table", "/export/");
    }
    catch(std::exception &e) {
        co_return api_error::access_denied(fmt::format("Invalid Export ARN {}: {}", export_arn, e.what()));
    }
    
    xlogger.debug("Describing export {}", export_arn);
    auto result = co_await get_export(_qp, sstring(export_arn));
    if (!result) {
        co_return api_error::validation(
            fmt::format("Invalid Export ARN {}", export_arn));
    }

    auto response = build_export_description_response(*result);
    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::list_exports(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_exports++;

    // Optional filter by TableArn
    std::string table_arn_filter;
    const rjson::value* table_arn_v = rjson::find(request, "TableArn");
    if (table_arn_v && table_arn_v->IsString()) {
        table_arn_filter = std::string(rjson::to_string_view(*table_arn_v));
    }

    // MaxResults (default 25, max 25 per DynamoDB spec)
    int max_results = 25;
    const rjson::value* max_results_v = rjson::find(request, "MaxResults");
    if (max_results_v && max_results_v->IsNumber()) {
        max_results = std::min(25, static_cast<int>(max_results_v->GetInt()));
        if (max_results < 1) {
            max_results = 1;
        }
    }

    // NextToken for pagination (last-seen export_arn)
    std::string next_token;
    const rjson::value* next_token_v = rjson::find(request, "NextToken");
    if (next_token_v && next_token_v->IsString()) {
        next_token = std::string(rjson::to_string_view(*next_token_v));
        try {
            parse_arn(next_token, "NextToken", "table", "/export/");
        }
        catch(std::exception &e) {
            co_return api_error::validation(fmt::format("Invalid NextToken ARN {}: {}", next_token, e.what()));
        }
    }

    auto result = co_await get_all_exports(_qp);

    // Collect and filter results
    struct export_summary {
        sstring export_arn;
        sstring export_status;
        sstring table_arn;
    };
    std::vector<export_summary> summaries;
    for (const auto& row : result) {
        // Filter by table_arn if specified
        auto table_arn = get_table_arn_from_export_arn(row.export_arn);
        if (!table_arn_filter.empty() && table_arn != table_arn_filter) {
            continue;
        }
        arn_parts parts;
        try {
            parts = parse_arn(table_arn, "TableArn", "table", "");
        }
        catch(std::exception &e) {
            xlogger.error("Failed to parse ARN `{}` from all-exports: {}", table_arn, e.what());
            continue;
        }

        try {
            _proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
        } catch (const data_dictionary::no_such_column_family&) {
            continue;
        }
        
        summaries.push_back({row.export_arn, row.export_status, sstring{ table_arn }});
    }

    // Sort by export_arn descending (AWS requires us to return results descending for each table)
    std::sort(summaries.begin(), summaries.end(),
        [](const export_summary& a, const export_summary& b) {
            return a.export_arn > b.export_arn;
        });

    // Apply pagination: skip past next_token
    auto it = summaries.begin();
    if (!next_token.empty()) {
        it = std::find_if(summaries.begin(), summaries.end(),
            [&next_token](const export_summary& s) {
                return s.export_arn < next_token;
            });
    }

    // Build response
    rjson::value export_summaries = rjson::empty_array();
    int count = 0;
    sstring last_arn;
    while (it != summaries.end() && count < max_results) {
        rjson::value summary = rjson::empty_object();
        rjson::add(summary, "ExportArn", rjson::from_string(it->export_arn));
        rjson::add(summary, "ExportStatus", rjson::from_string(it->export_status));
        rjson::add(summary, "ExportType", rjson::from_string("FULL_EXPORT"));
        rjson::push_back(export_summaries, std::move(summary));
        last_arn = it->export_arn;
        ++it;
        ++count;
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportSummaries", std::move(export_summaries));

    // If there are more results, include NextToken
    if (it != summaries.end()) {
        rjson::add(response, "NextToken", rjson::from_string(last_arn));
    }

    co_return rjson::print(std::move(response));
}

} // namespace alternator
