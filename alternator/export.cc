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
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "db_clock.hh"
#include "db/config.hh"
#include "db/consistency_level.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/timeout_clock.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"
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
#include "utils/assert.hh"
#include "utils/base64.hh"
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
#include <chrono>
#include <cmath>
#include <exception>
#include <limits>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string_view>
#include <string>
#include <utility>
#include <vector>
#include <zlib.h>

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

    // Reading the table needs SELECT on it, writing it out to S3 needs MODIFY on ALL OBJECT STORAGE.
    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::SELECT, _stats);
    co_await verify_object_storage_permission(_enforce_authorization, _warn_authorization, client_state, auth::permission::MODIFY, _stats);

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
    auto now = to_epoch_seconds(db_clock::now());
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
        // DynamoDB rounds a requested ExportTime to the nearest millisecond.
        export_time = std::chrono::duration<double>(std::chrono::round<std::chrono::milliseconds>(
                std::chrono::duration<double>(export_time))).count();
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
    // DynamoDB echoes ExportType only when the request carried one; a request that relied on the
    // FULL_EXPORT default gets none back.
    if (rjson::find(request, "ExportType")) {
        rjson::add(export_desc, "ExportType", rjson::from_string(export_type));
    }
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
    // FIXME: DynamoDB also answers an id of any shape but its own with ValidationException. Check
    // that once SCYLLADB-1893 mints ids of that shape.
    return parts;
}

// The persisted state of a single Alternator "export to S3", as needed to answer DescribeExport.
// The columns above the optional ones must all be written by the statement that creates the export
// row, so that a row is either complete or not there at all. Everything that describes a finished
// export is optional, because it is only filled in as the export runs.
struct export_row {
    sstring client_token;
    // The original ExportTableToPointInTime request, as JSON. It is the source of truth for the
    // request-derived fields of ExportDescription (S3Bucket, ExportFormat, TableArn, ...).
    sstring request;
    sstring export_status;
    ::table_id table_id;  // The exported table, as resolved when the export was accepted
    db_clock::time_point export_time;  // The point in time the data was exported from
    db_clock::time_point accepted_at;
    std::optional<sstring> export_manifest;
    std::optional<sstring> failure_code;
    std::optional<sstring> failure_message;
    std::optional<int64_t> item_count;
    std::optional<int64_t> billed_size_bytes;
    std::optional<db_clock::time_point> completed_at;
};

// Reads at most `row_limit` rows of the given partitions of the exports table, whose schema is `schema`.
static future<foreign_ptr<lw_shared_ptr<query::result>>> read_exports(service::storage_proxy& proxy, schema_ptr schema, dht::partition_range_vector partition_ranges,
        const query::partition_slice& partition_slice, query::row_limit row_limit, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit) {
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice), query::tombstone_limit(proxy.get_tombstone_limit()), row_limit);
    // system_distributed has RF=3, and QUORUM of that cannot be reached with a single token owner.
    auto cl = proxy.get_token_metadata_ptr()->count_normal_token_owners() > 1 ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    service::storage_proxy::result<service::storage_proxy::coordinator_query_result> rqr =
            co_await proxy.query_result(schema, std::move(command), std::move(partition_ranges), cl,
                    service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state, std::move(trace_state)));
    if (!rqr) {
        co_await coroutine::return_exception(create_api_error_from_coordinators_exception(std::move(rqr).assume_error()));
    }
    co_return std::move(rqr.assume_value().query_result);
}

// Returns the export with the given ARN, or nullopt if there is no such export.
static future<std::optional<export_row>> get_export(service::storage_proxy& proxy, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, sstring export_arn) {
    using sdks = db::system_distributed_keyspace;
    schema_ptr schema = proxy.data_dictionary().find_schema(sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    partition_key pk = partition_key::from_single_value(*schema, utf8_type->decompose(export_arn));
    dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*schema, pk))};
    const query::partition_slice& partition_slice = schema->full_slice();
    auto result = co_await read_exports(proxy, schema, std::move(partition_ranges), partition_slice, query::row_limit::max, client_state, std::move(trace_state), std::move(permit));
    query::result_set rs = query::result_set::from_raw_result(schema, partition_slice, *result);
    if (rs.empty()) {
        co_return std::nullopt;
    }
    const query::result_set_row& row = rs.row(0);
    // The statement creating the export row must fill all of these in, so a row missing any of them
    // is corrupt.
    for (auto column : {"client_token", "request", "export_status", "table_id", "export_time", "accepted_at"}) {
        if (!row.get_data_value(column)) {
            throw std::runtime_error(fmt::format("Export '{}' has no {} in {}.{}", export_arn, column, sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS));
        }
    }
    co_return export_row {
        .client_token = row.get_nonnull<sstring>("client_token"),
        .request = row.get_nonnull<sstring>("request"),
        .export_status = row.get_nonnull<sstring>("export_status"),
        .table_id = ::table_id(row.get_nonnull<utils::UUID>("table_id")),
        .export_time = row.get_nonnull<db_clock::time_point>("export_time"),
        .accepted_at = row.get_nonnull<db_clock::time_point>("accepted_at"),
        .export_manifest = row.get<sstring>("export_manifest"),
        .failure_code = row.get<sstring>("failure_code"),
        .failure_message = row.get<sstring>("failure_message"),
        .item_count = row.get<int64_t>("item_count"),
        .billed_size_bytes = row.get<int64_t>("billed_size_bytes"),
        .completed_at = row.get<db_clock::time_point>("completed_at"),
    };
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

    // Unlike the export itself, needs no permission on the table, which may have been deleted since.
    co_await verify_object_storage_permission(_enforce_authorization, _warn_authorization, client_state, auth::permission::SELECT, _stats);

    auto exp = co_await get_export(_proxy, client_state, std::move(trace_state), std::move(permit), sstring(export_arn));
    // DynamoDB answers ExportNotFoundException also when the ARN names a table it does not have,
    // but ValidationException when no table can have the name the ARN gives.
    if (!exp) {
        if (parts.keyspace_name.empty()) {
            co_return api_error::validation(fmt::format("ExportArn: Invalid Export ARN `{}` - empty keyspace name", export_arn));
        }
        validate_table_name(parts.table_name, "ExportArn: Invalid Export ARN - table name");
        co_return api_error::export_not_found(fmt::format("ExportArn: Export `{}` not found", export_arn));
    }

    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));
    // DynamoDB reports no ExportStatus but these three, so any other stored status is reported as
    // IN_PROGRESS.
    rjson::add(export_desc, "ExportStatus", rjson::from_string(exp->export_status == "COMPLETED" || exp->export_status == "FAILED" ? exp->export_status : "IN_PROGRESS"));
    rjson::add(export_desc, "StartTime", rjson::value(to_epoch_seconds(exp->accepted_at)));

    // The fields describing the request are echoed back from the request that started the export,
    // with the ExportFormat ExportTableToPointInTime defaults to. That request was validated when it
    // was accepted, so one that cannot be read back is a corrupt row, like a row missing a column,
    // and not the caller's error.
    try {
        auto exported_request = rjson::parse(exp->request);
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
        throw std::runtime_error(fmt::format("Export '{}' has a malformed request: {}", export_arn, e.what()));
    }

    // TableId identifies the table the export was taken from, not whatever table now answers to
    // the export's name - names can be reused, ids cannot.
    rjson::add(export_desc, "TableId", rjson::from_string(exp->table_id.to_sstring()));
    rjson::add(export_desc, "ExportTime", rjson::value(to_epoch_seconds(exp->export_time)));
    rjson::add(export_desc, "ClientToken", rjson::from_string(exp->client_token));

    // Everything below describes a finished export, so it is only there once one has run.
    if (exp->export_manifest) {
        rjson::add(export_desc, "ExportManifest", rjson::from_string(*exp->export_manifest));
    }
    if (exp->failure_code) {
        rjson::add(export_desc, "FailureCode", rjson::from_string(*exp->failure_code));
    }
    if (exp->failure_message) {
        rjson::add(export_desc, "FailureMessage", rjson::from_string(*exp->failure_message));
    }
    if (exp->item_count) {
        rjson::add(export_desc, "ItemCount", rjson::value(*exp->item_count));
    }
    if (exp->billed_size_bytes) {
        rjson::add(export_desc, "BilledSizeBytes", rjson::value(*exp->billed_size_bytes));
    }
    if (exp->completed_at) {
        rjson::add(export_desc, "EndTime", rjson::value(to_epoch_seconds(*exp->completed_at)));
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    co_return rjson::print(std::move(response));
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

// An export reduced to what ListExports reports about it. The optional members are unset in a row
// which lacks the column.
struct export_summary_row {
    sstring export_arn;
    // The export's status, as stored. ListExports reports it as ExportStatus.
    std::optional<sstring> status;
    // The original ExportTableToPointInTime request, as JSON. ExportType is read back from it.
    std::optional<sstring> request;
};

// Returns every export there is, in no particular order. The table is read a page at a time, as an
// unpaged read fails once its result outgrows max_memory_for_unlimited_query_hard_limit.
static future<std::vector<export_summary_row>> get_all_exports(service::storage_proxy& proxy, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit) {
    using sdks = db::system_distributed_keyspace;
    static constexpr uint64_t page_size = 1000;
    schema_ptr schema = proxy.data_dictionary().find_schema(sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    auto partition_slice = partition_slice_builder(*schema)
            .with_regular_column(to_bytes("export_status"))
            .with_regular_column(to_bytes("request"))
            .with_option<query::partition_slice::option::allow_short_read>()
            .build();
    dht::partition_range_vector partition_ranges{dht::partition_range::make_open_ended_both_sides()};
    std::vector<export_summary_row> exports;
    while (true) {
        auto result = co_await read_exports(proxy, schema, std::move(partition_ranges), partition_slice, query::row_limit(page_size), client_state, trace_state, permit);
        query::result_set rs = query::result_set::from_raw_result(schema, partition_slice, *result);
        for (const query::result_set_row& row : rs.rows()) {
            exports.push_back(export_summary_row {
                .export_arn = row.get_nonnull<sstring>("export_arn"),
                .status = row.get<sstring>("export_status"),
                .request = row.get<sstring>("request"),
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

    // Unlike the export itself, needs no permission on the exported tables, which may have been deleted since.
    co_await verify_object_storage_permission(_enforce_authorization, _warn_authorization, client_state, auth::permission::SELECT, _stats);

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
    // FIXME: not until SCYLLADB-1893 mints export ids - ExportTableToPointInTime's is a placeholder.
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
} // namespace alternator
