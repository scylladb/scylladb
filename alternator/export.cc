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
#if 0
        // revert this once #31703 is merged
        , _upload_stream(std::make_unique<output_stream<char>>(_client->make_upload_jumbo_sink(_object_name, s3::object_metadata{}, std::nullopt, as, _etag)))
#else
        , _upload_stream(std::make_unique<output_stream<char>>(_client->make_upload_jumbo_sink(_object_name, s3::object_metadata{}, std::nullopt, as)))
#endif
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

} // namespace alternator
