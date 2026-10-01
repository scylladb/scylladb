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
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "db_clock.hh"
#include "db/config.hh"
#include "db/consistency_level.hh"
#include "db/object_storage_endpoint_param.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "locator/host_id.hh"
#include "query/query-request.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "service_permit.hh"
#include "service/client_state.hh"
#include "service/pager/paging_state.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/sstables_manager.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/base64.hh"
#include "utils/chunked_vector.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"
#include "utils/hashers.hh"
#include "utils/overloaded_functor.hh"
#include "utils/rjson.hh"
#include "utils/s3/client.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
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
#include <fmt/format.h>

namespace alternator {

// Helper functions for the export to S3 metadata in the system_distributed keyspace.
// There are two tables:
// 1) `alternator_export_to_s3_exports` - the key is the export ARN. Has the export metadata
//    (table id, client token, status, timestamps, etc.).
// 2) `alternator_export_to_s3_client_tokens` - maps client tokens to export ARNs, with the
//    original request, for idempotent retries.
// For the schemas, see db/system_distributed_keyspace.cc.
// All writes to these tables must use the helper functions, which apply the correct
// conditions. db::alternator_export_table_helper does the CQL. The functions below only
// convert the rows.

// Identifies one incarnation of a node: host_id and gossip generation. The value changes
// after a restart, and after a restart of gossip (`nodetool disablegossip` and `enablegossip`):
// both increase the gossip generation. The export and the garbage collector
// use this.
// The `node_id` columns store it as "{host_id}:{gossip_generation}".
// A default value (a null host_id) means "no node".
struct live_node_identifier {
    locator::host_id host_id;
    std::uint32_t gossip_generation = 0;

    sstring to_string() const {
        if (!host_id) {
            return sstring();
        }
        return fmt::format("{}:{}", host_id, gossip_generation);
    }

    auto operator<=>(const live_node_identifier&) const = default;
};

} // namespace alternator

template <> struct fmt::formatter<alternator::live_node_identifier> : fmt::formatter<string_view> {
    auto format(const alternator::live_node_identifier& id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.to_string());
    }
};

namespace alternator {

static logging::logger xlogger("alternator-export");

// TTL of export rows. Each write of a row sets it (see insert_export() below).
// DynamoDB keeps the export history (DescribeExport / ListExports) for 90 days.
static constexpr std::chrono::seconds export_row_ttl = std::chrono::days(90);

// Returns nullopt if it inserted the row, and the existing row if a row with the same ARN is
// already there.
static future<std::optional<export_row>> insert_export(db::system_distributed_keyspace& sdks, const export_row& row) {
    db::alternator_export_table_helper helper(sdks.qp());
    // Set the TTL on insert. update_export() cannot change the row marker, so a row inserted
    // without a TTL does not expire.
    co_return co_await helper.insert_export(row, export_row_ttl);
}

static future<bool> update_export(db::system_distributed_keyspace& sdks, const export_row& row, const sstring& old_export_status,
        const sstring& old_node_id, std::chrono::seconds ttl) {
    db::alternator_export_table_helper helper(sdks.qp());
    co_return co_await helper.update_export(row, old_export_status, old_node_id, ttl);
}

// Writes the terminal state (COMPLETED / FAILED) of the `IN_PROGRESS` export row of this node.
// Returns false if the row is not IN_PROGRESS under this node. The conditional update fails if
// there is no row (the export can fail before the row is written). Then we insert the row. If
// the insert fails, a row exists that we must not overwrite, unless it is our IN_PROGRESS row.
static future<bool> try_write_final_export_row(db::system_distributed_keyspace& sdks, const export_row& row) {
    if (co_await update_export(sdks, row, "IN_PROGRESS", row.node_id, export_row_ttl)) {
        co_return true;
    }
    auto existing_row = co_await insert_export(sdks, row);
    if (!existing_row) {
        co_return true;
    }
    // The update can miss our IN_PROGRESS row. If its insert timed out after only a minority of
    // the replicas accepted it, the Paxos round of the update may not see it, and the Paxos
    // round of the insert above then completes it. Without a second update, the row would stay
    // IN_PROGRESS, and the garbage collector does not take the rows of a running node.
    if (existing_row->export_status == "IN_PROGRESS" && existing_row->node_id == row.node_id
            && existing_row->client_token == row.client_token) {
        co_return co_await update_export(sdks, row, "IN_PROGRESS", row.node_id, export_row_ttl);
    }
    co_return false;
}

// `query_processor` throws the coordinator errors. Convert overload and rate limit errors to
// RequestLimitExceeded, so that the client retries (the same as
// create_api_error_from_coordinators_exception() does for data tables).
// The reads below convert errors for all callers.
template <typename T>
static future<T> translate_coordinator_errors(future<T> f) {
    try {
        co_return co_await std::move(f);
    } catch (const exceptions::rate_limit_exception& e) {
        throw api_error::request_limit_exceeded(e.what());
    } catch (const exceptions::overloaded_exception& e) {
        throw api_error::request_limit_exceeded(e.what());
    }
}

// `qs` is the query state of the client request. Background fibers give null, and the read
// uses the internal query state.
static future<std::optional<export_row>> get_export(db::system_distributed_keyspace& sdks, const sstring& export_arn,
        service::query_state* qs = nullptr) {
    db::alternator_export_table_helper helper(sdks.qp());
    co_return co_await translate_coordinator_errors(helper.get_export(export_arn, qs));
}

// The export columns that ListExports and the garbage collector use. ExportType comes from
// `request`.
using export_summary_row = db::alternator_export_table_helper::export_summary_row;

// Returns all exports, in no specified order. If set, `as` aborts the scan.
static future<utils::chunked_vector<export_summary_row>> get_all_exports(db::system_distributed_keyspace& sdks,
        const abort_source* as = nullptr, service::query_state* qs = nullptr) {
    db::alternator_export_table_helper helper(sdks.qp());
    co_return co_await translate_coordinator_errors(helper.get_all_exports(as, qs));
}

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

    // The maximum number of parts in one piece of the jumbo sink. The sink copies each piece into
    // the object with one UploadPartCopy, and S3 refuses to copy a source larger than
    // `s3::maximum_part_size` (5 GiB) that way. A part is a little larger than
    // `s3::minimum_part_size` (5 MiB), so the default of 10'000 parts allows pieces of ~50 GiB,
    // and the export of a table with a larger data file would fail. Half of the limit leaves
    // space for the part sizes, and still allows objects of ~24 TiB (10'000 pieces of ~2.5 GiB).
    static constexpr unsigned max_parts_per_piece = s3::maximum_part_size / s3::minimum_part_size / 2;

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
                          client->make_upload_jumbo_sink_and_get_etag(object_name, s3::object_metadata{}, max_parts_per_piece, as))
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
    abort_source *_as;
    temporary_buffer<char> _buffer;
    std::optional<input_stream<char>> _src;
public:
    s3_storage_source(shared_ptr<s3::client> client, sstring object_name, abort_source *as)
        : _client(std::move(client))
        , _object_name(std::move(object_name))
        , _as(as)
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
        // An abort cuts the request in flight, but the download source still hands out the data
        // that it holds and the data that the connection received before the cut - possibly the
        // rest of the object. Check here, so that an abort fails the next read.
        if (_as) {
            _as->check();
        }
        try {
            _buffer = co_await _src->read();
        } catch (...) {
            // The HTTP client already reports a request that an abort cut short with the abort's
            // exception. This check is a guard: any read that fails after an abort reports the abort.
            if (_as) {
                _as->check();
            }
            throw;
        }
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

// --- Export orchestration helpers ---

// The configured S3 endpoints. The configuration does not map buckets to endpoints. Thus an
// export to S3 needs exactly one S3 endpoint in object_storage_endpoints, and uses it for all
// buckets.
static std::vector<sstring> s3_endpoints(service::storage_proxy& proxy) {
    return proxy.local_db().get_user_sstables_manager().endpoints(db::object_storage_endpoint_param::s3_type);
}

// The S3 client of an export. `client` calls back into `endpoint_client` (for the clients of
// other shards), so the export keeps `endpoint_client` alive while it uses `client`, also if the
// endpoint is removed from the configuration meanwhile.
struct export_s3_client {
    seastar::shared_ptr<sstables::object_storage_client> endpoint_client;
    seastar::shared_ptr<s3::client> client;
};

// Gets the S3 client of the S3 endpoint `endpoint` of object_storage_endpoints.
static export_s3_client get_s3_client(service::storage_proxy& proxy, const sstring& endpoint) {
    auto endpoint_client = proxy.local_db().get_user_sstables_manager().get_endpoint_client(endpoint);
    auto client = sstables::get_s3_client(endpoint_client);
    if (!client) {
        on_internal_error(xlogger, fmt::format("object storage endpoint {} is not an S3 endpoint", endpoint));
    }
    return export_s3_client{ .endpoint_client = std::move(endpoint_client), .client = std::move(client) };
}

// Returns the identifier of this node incarnation (host_id + gossip_generation).
// Returns nullopt if the gossiper has no state for this node, that is before gossip starts.
static std::optional<live_node_identifier> get_self_node_id(gms::gossiper& gossiper) {
    auto host_id = gossiper.my_host_id();
    auto ep_state = gossiper.get_this_endpoint_state_ptr();
    if (!ep_state) {
        return std::nullopt;
    }
    auto generation = ep_state->get_heart_beat_state().get_generation();
    return live_node_identifier{ .host_id = host_id, .gossip_generation = std::uint32_t(generation.value()) };
}

// Generates a unique export id in the DynamoDB format, see is_valid_export_id().
static sstring generate_export_id(db_clock::time_point accepted_at) {
    static thread_local std::mt19937 rng(std::random_device{}());
    auto epoch_millis = std::chrono::duration_cast<std::chrono::milliseconds>(accepted_at.time_since_epoch()).count();
    return fmt::format("{:014d}-{:08x}", epoch_millis, std::uint32_t(rng()));
}

// An export ARN is a table ARN, `/export/` and the export id. Expected format:
// arn:scylla:alternator:<keyspace>:scylla:table/<name>/export/<id>, or AWS format:
// arn:aws:dynamodb:<region>:<account>:table/<name>/export/<id>.
static constexpr std::string_view export_arn_infix = "/export/";

// A DynamoDB export id is the start time in milliseconds since the epoch (14 digits), a dash,
// and 8 hex digits (e.g. `01695353076000-06e2188f`). DynamoDB returns ValidationException for
// an id with a different format. generate_export_id() uses the same format.
static bool is_valid_export_id(std::string_view export_id) {
    static constexpr size_t digit_count = 14;
    static constexpr size_t hex_digit_count = 8;
    if (export_id.size() != digit_count + 1 + hex_digit_count || export_id[digit_count] != '-') {
        return false;
    }
    auto is_digit = [] (char c) { return c >= '0' && c <= '9'; };
    auto is_hex_digit = [&] (char c) { return is_digit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'); };
    return std::ranges::all_of(export_id.substr(0, digit_count), is_digit)
            && std::ranges::all_of(export_id.substr(digit_count + 1), is_hex_digit);
}

// Returns the time of the export data: the ExportTime of the request, or the accept time if
// the request has no ExportTime. We validated the request when we accepted it. If ExportTime
// is not a number (a corrupt row), we use the accept time.
static db_clock::time_point export_time_of_request(const rjson::value& request, db_clock::time_point accepted_at) {
    const rjson::value* export_time = rjson::find(request, "ExportTime");
    if (!export_time || !export_time->IsNumber()) {
        return accepted_at;
    }
    // DynamoDB rounds ExportTime to the nearest millisecond, which is the db_clock precision.
    return db_clock::time_point(std::chrono::round<db_clock::duration>(
            std::chrono::duration<double>(export_time->GetDouble())));
}

// Returns true if the status is final.
static bool is_terminal_status(std::string_view export_status) {
    return export_status == "COMPLETED" || export_status == "FAILED";
}

// DynamoDB has only IN_PROGRESS, COMPLETED and FAILED. We report any other stored status
// as IN_PROGRESS.
static rjson::value reported_status(std::string_view export_status) {
    return rjson::from_string(is_terminal_status(export_status) ? export_status : "IN_PROGRESS");
}

// Builds the ExportDescription of an export row for DescribeExport and ExportTableToPointInTime.
static rjson::value build_export_description_response(const export_row& exp) {
    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ExportArn", rjson::from_string(exp.export_arn));
    rjson::add(export_desc, "ExportStatus", reported_status(exp.export_status));
    rjson::add(export_desc, "StartTime", rjson::value(to_epoch_seconds(exp.accepted_at)));

    // Copy these fields from the original request (ExportFormat has a default). We validated the
    // request when we accepted it, so a parse error is a corrupt row, not a caller error.
    try {
        auto exported_request = rjson::parse(exp.request);
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
        throw std::runtime_error(fmt::format("Export '{}' has a malformed request: {}", exp.export_arn, e.what()));
    }

    // TableId identifies the table the export was taken from, not whatever table now answers to
    // the export's name - names can be reused, ids cannot.
    rjson::add(export_desc, "TableId", rjson::from_string(exp.table_id.to_sstring()));
    rjson::add(export_desc, "ExportTime", rjson::value(to_epoch_seconds(exp.export_time)));
    rjson::add(export_desc, "ClientToken", rjson::from_string(exp.client_token));

    // These fields are set only when the export finishes.
    if (exp.export_manifest) {
        rjson::add(export_desc, "ExportManifest", rjson::from_string(*exp.export_manifest));
    }
    if (exp.failure_code) {
        rjson::add(export_desc, "FailureCode", rjson::from_string(*exp.failure_code));
    }
    if (exp.failure_message) {
        rjson::add(export_desc, "FailureMessage", rjson::from_string(*exp.failure_message));
    }
    if (exp.item_count) {
        rjson::add(export_desc, "ItemCount", rjson::value(*exp.item_count));
    }
    if (exp.billed_size_bytes) {
        rjson::add(export_desc, "BilledSizeBytes", rjson::value(*exp.billed_size_bytes));
    }
    if (exp.completed_at) {
        rjson::add(export_desc, "EndTime", rjson::value(to_epoch_seconds(*exp.completed_at)));
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    return response;
}

future<> executor::s3_exports::stop() {
    _as.request_abort();
    co_await _gate.close();
}

static bool is_ascii_alnum(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

// The bucket and the prefix of a request become part of the path of the S3 requests of the export
// (see `s3_object_name()`). s3::client URI-encodes that path, but it keeps '/' and '.' as they are,
// so a '/' in the bucket would send the requests to a different bucket. For the bucket, we accept
// the characters and the length which DynamoDB accepts. DynamoDB also requires a letter or a digit
// as the first and the last character, so we require it too.
static bool is_valid_s3_bucket_name(std::string_view bucket) {
    return bucket.size() >= 3 && bucket.size() <= 255
            && is_ascii_alnum(bucket.front()) && is_ascii_alnum(bucket.back())
            && std::ranges::all_of(bucket, [] (char c) {
        return is_ascii_alnum(c) || c == '.' || c == '-' || c == '_';
    });
}

// DynamoDB accepts any S3 key as a prefix, but for now we accept only letters, digits and "/-_."
// (see docs/alternator/compatibility.md), which the URI encoding of the path leaves as they are.
// Since s3::client encodes the path, other characters could be accepted too, but no test sends
// them to S3 yet (SCYLLADB-6440).
// A "." or ".." segment between slashes is refused: the encoding keeps it, and an HTTP server or
// proxy can resolve such a segment, so the export would write elsewhere than DescribeExport
// reports, e.g. to another bucket with "../".
// The length limit is the one of DynamoDB, which is also the longest key S3 accepts (the export
// appends to the prefix, so a prefix close to the limit cannot work, but that is for S3 to say).
static bool is_valid_s3_prefix(std::string_view prefix) {
    static constexpr std::string_view other_safe_characters = "/-_.";
    if (prefix.size() > 1024 || !std::ranges::all_of(prefix, [] (char c) {
            return is_ascii_alnum(c) || other_safe_characters.find(c) != std::string_view::npos;
        })) {
        return false;
    }
    return std::ranges::none_of(prefix | std::views::split('/'), [] (auto segment) {
        std::string_view s(segment.begin(), segment.end());
        return s == "." || s == "..";
    });
}

// The failure code and the failure message of an export which a clean shutdown of its node
// stopped. Only s3_exports::stop() aborts the export fibers, thus an abort is a shutdown.
static constexpr std::string_view node_shutdown_failure_code = "NodeShutdown";
static constexpr std::string_view node_shutdown_failure_message =
        "The node that was performing the export was shut down before the export could finish.";

future<executor::request_return_type> executor::export_table_to_point_in_time(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.export_table_to_point_in_time++;

    // Required parameter
    auto table_arn = get_non_empty_string_attribute(request, "TableArn");
    // DynamoDB's limit. The table lookup ignores some parts of the ARN (e.g. the account), and
    // the metadata rows store the TableArn of the request, so it must be bounded.
    if (table_arn.size() > 1024) {
        co_return api_error::validation("TableArn attribute: must have at most 1024 characters");
    }

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
    auto export_type = get_non_empty_string_attribute(request, "ExportType", "");

    auto now = to_epoch_seconds(db_clock::now());
    // If there is no ExportTime, use the current time (see the validation below).
    auto export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    auto user_client_token = get_non_empty_string_attribute(request, "ClientToken", "");

    if (export_format != "DYNAMODB_JSON") {
        co_return api_error::validation(
                fmt::format("ExportFormat attribute: must be DYNAMODB_JSON, not `{}`", export_format));
    }

    if (!export_type.empty() && export_type != "FULL_EXPORT") {
        co_return api_error::validation(
                fmt::format("ExportType attribute: must be FULL_EXPORT, not `{}`", export_type));
    }

    // The pattern of ClientToken in DynamoDB is ^[^\$]+$. The token is also the partition key of
    // the client token table, so a long token makes the insert of the row throw. We keep the
    // length well below the limit of a key.
    constexpr size_t max_client_token_length = 1024;
    if (user_client_token.find('$') != std::string::npos || user_client_token.size() > max_client_token_length) {
        co_return api_error::validation(fmt::format(
                "ClientToken attribute: must not contain '$', and must have at most {} characters",
                max_client_token_length));
    }

    constexpr std::array unsupported_parameters{
            "FilterSpecification",
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

    if (!is_valid_s3_bucket_name(s3_bucket)) {
        co_return api_error::validation(fmt::format("S3Bucket attribute: `{}` is not a valid bucket name - "
                "it must have 3 to 255 characters, each of them a letter, a digit, '.', '-' or '_'", s3_bucket));
    }

    if (!is_valid_s3_prefix(s3_prefix)) {
        co_return api_error::validation(fmt::format("S3Prefix attribute: `{}` is not a valid key prefix - "
                "it must have at most 1024 characters, each of them a letter, a digit or one of \"/-_.\", "
                "and no \".\" or \"..\" segment between slashes", s3_prefix));
    }

    if (export_time_v) {
        if (!export_time_v->IsNumber()) {
            co_return api_error::validation("Expected a number attribute ExportTime");
        }
        export_time = export_time_v->GetDouble();
        // ExportTime becomes int64 milliseconds (db_clock). The cap, 9999-12-31T23:59:59Z, is far
        // below the limit of that type, so the conversions below cannot overflow.
        constexpr double max_export_time = 253402300799; // 9999-12-31T23:59:59Z
        if (std::isnan(export_time) || export_time < 0 || export_time > max_export_time) {
            co_return api_error::invalid_export_time("ExportTime number is out of range of valid values for this field");
        }
        // DynamoDB rounds a requested ExportTime to the nearest millisecond.
        export_time = std::chrono::duration<double>(std::chrono::round<std::chrono::milliseconds>(
                std::chrono::duration<double>(export_time))).count();
        // ExportTime must be in the last 5 minutes. DynamoDB refuses a time in the future.
        // `now` is cut down to the millisecond, while ExportTime is rounded to the nearest
        // millisecond. Thus a request for the current time can be one millisecond after `now`.
        auto to_millis = [] (double seconds) { return std::llround(seconds * 1000); };
        if (to_millis(export_time) > to_millis(now) + 1 || now - export_time > 300) {
            co_return api_error::invalid_export_time(fmt::format("ExportTime must be in the last 5 minutes. "
                                "ExportTime: {} s , current time: {} s", export_time, now));
        }
    }

    if (user_client_token.empty()) {
        co_return api_error::validation("ClientToken attribute is required");
    }

    auto node_id_opt = get_self_node_id(_gossiper);
    if (!node_id_opt) {
        // Without a node identifier, we cannot own the export.
        co_return api_error::internal("This node is not gossiping, so it can't start an export. Please retry the operation.");
    }
    auto node_id = std::move(*node_id_opt);

    // The request that we store and compare on a retry: only the members which we support (add a
    // member here when it becomes supported). We validated each of them above, so the stored
    // request is small, and a request cannot fill the metadata rows with members which we ignore.
    rjson::value request_to_compare = rjson::empty_object();
    for (std::string_view name : {"TableArn", "S3Bucket", "S3Prefix", "ExportFormat", "ExportType", "ExportTime"}) {
        if (const rjson::value* v = rjson::find(request, name)) {
            rjson::add_with_string_name(request_to_compare, name, rjson::copy(*v));
        }
    }
    sstring stored_request(rjson::print(request_to_compare));

    sstring client_token(user_client_token);

    auto accepted_at = db_clock::now();

    // As in DynamoDB, the export id is also the name of the S3 directory of the export. The ARN is
    // the table ARN and `/export/<export id>`.
    auto export_id_token = generate_export_id(accepted_at);
    auto export_arn = fmt::format("{}/export/{}",
            rjson::to_string_view(generate_arn_for_table(parts.keyspace_name, parts.table_name)), export_id_token);

    // The row of the new export. A failure below writes it as FAILED, so we create it first.
    export_row erow{
        .export_arn = export_arn,
        .client_token = client_token,
        .request = stored_request,
        .table_id = schema->id(),
        .export_status = "IN_PROGRESS",
        .export_id_token = export_id_token,
        .export_time = export_time_of_request(request_to_compare, accepted_at),
        .accepted_at = accepted_at,
        .node_id = node_id.to_string(),
    };

    // The S3 endpoint comes from the configuration, which must have exactly one. The export
    // uses the client of the endpoint which we check here: a live update of the configuration
    // after this check does not change the destination of the export. Do not wait between the
    // check and getting the client, or an update can come between them.
    auto endpoints = s3_endpoints(_proxy);
    if (endpoints.size() != 1) {
        co_return api_error::validation(fmt::format(
                "Export to S3 needs exactly one S3 endpoint in object_storage_endpoints, found {}", endpoints.size()));
    }
    auto s3_client = get_s3_client(_proxy, endpoints.front());

    std::exception_ptr failure;

    try {
        // The export row below is ours, so from here a failure writes FAILED. This includes a
        // failed insert, because a write that timed out can still apply.
        // The insert returns the existing export row if it did not apply.
        auto existing_export_row = co_await translate_coordinator_errors(insert_export(_sdks, erow));

        if (existing_export_row) {
            // Only the garbage collector can also write a row for this ARN (if this node is
            // unreachable for more than one hour). Or the row is ours, if a concurrent CAS
            // committed our value.
            xlogger.debug("Export row for ClientToken {} and ExportArn {} already exists",
                    client_token, export_arn);
            // An IN_PROGRESS row with our export id and our ClientToken is our row (the garbage
            // collector writes an empty id or FAILED). Run the export, or the row stays
            // IN_PROGRESS. Another export of this table can have the same id (the same millisecond
            // and random number). Then we must not run a second export into its S3 directory.
            if (existing_export_row->export_id_token != export_id_token
                    || existing_export_row->client_token != client_token
                    || existing_export_row->export_status != "IN_PROGRESS") {
                // Not our row. Report it as DescribeExport does.
                xlogger.debug("Export row for ExportArn {} is not this request's own, not starting the export",
                        export_arn);
                co_return rjson::print(build_export_description_response(*existing_export_row));
            }
            xlogger.debug("Export row for ExportArn {} is this request's own, starting the export", export_arn);
        }

        // Build the response before we start the fiber. After the fiber starts, a failure here
        // (e.g. no memory) must not write FAILED while the fiber continues the export.
        xlogger.debug("Returning IN_PROGRESS response for ClientToken {}", client_token);
        auto response = rjson::print(build_export_description_response(erow));

        xlogger.debug("Launching background export fiber for ClientToken {}", client_token);
        _s3_exports->start_export(std::move(s3_client), schema, erow, table_arn, s3_bucket, s3_prefix);
        co_return std::move(response);
    }
    catch (...) {
        // Keep the failure for the failure message of the export row below.
        failure = std::current_exception();
    }
    auto* e = try_catch<std::exception>(failure);
    if (_s3_exports->abort_requested()) {
        erow.failure_code = sstring(node_shutdown_failure_code);
        erow.failure_message = sstring(node_shutdown_failure_message);
    } else {
        erow.failure_code = "Exception";
        erow.failure_message = e ? e->what() : "Unknown error";
    }
    erow.export_status = "FAILED";
    erow.completed_at = db_clock::now();
    try {
        if (co_await try_write_final_export_row(_sdks, erow)) {
            xlogger.debug("Export {} failed during starting up phase", erow.export_arn);
        }
        else {
            // The write did not apply. The garbage collector took the export and marked it FAILED.
            xlogger.debug("Export {} failed during starting up phase, but the row is no longer ours",
                    erow.export_arn);
        }
    } catch (...) {
        xlogger.warn("Export {}: failed to write the FAILED state: {}", erow.export_arn, std::current_exception());
        throw;
    }
    // The export row is FAILED. Answer with it.
    co_return rjson::print(build_export_description_response(erow));
}

// An export ARN is a table ARN followed by `/export/<id>`.
static arn_parts parse_export_arn(std::string_view export_arn, std::string_view arn_field_name) {
    auto parts = parse_arn_reporting_validation(export_arn, arn_field_name, "Export", export_arn_infix);
    // parse_arn accepts all export ids, also an empty id.
    auto export_id = parts.postfix.substr(export_arn_infix.size());
    if (!is_valid_export_id(export_id)) {
        throw api_error::validation(fmt::format("{}: Invalid Export ARN `{}` - `{}` after `{}` is not an export id",
                arn_field_name, export_arn, export_id, export_arn_infix));
    }
    return parts;
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

    service::query_state qs(client_state, std::move(trace_state), std::move(permit));
    auto exp = co_await get_export(_sdks, sstring(export_arn), &qs);
    // DynamoDB answers ExportNotFoundException also when the ARN names a table it does not have,
    // but ValidationException when no table can have the name the ARN gives.
    if (!exp) {
        if (parts.keyspace_name.empty()) {
            co_return api_error::validation(fmt::format("ExportArn: Invalid Export ARN `{}` - empty keyspace name", export_arn));
        }
        validate_table_name(parts.table_name, "ExportArn: Invalid Export ARN - table name");
        co_return api_error::export_not_found(fmt::format("ExportArn: Export `{}` not found", export_arn));
    }
    // The insert of the export row writes these columns, so a row without one of them is corrupt.
    for (auto [column, missing] : {
            std::pair{"client_token", exp->client_token.empty()},
            std::pair{"request", exp->request.empty()},
            std::pair{"export_status", exp->export_status.empty()},
            std::pair{"table_id", !exp->table_id},
            std::pair{"export_time", exp->export_time == db_clock::time_point{}},
            std::pair{"accepted_at", exp->accepted_at == db_clock::time_point{}}}) {
        if (missing) {
            throw std::runtime_error(fmt::format("Export '{}' has no {} in {}.{}", export_arn, column,
                    db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS));
        }
    }

    co_return rjson::print(build_export_description_response(*exp));
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

// Throws std::runtime_error for an incomplete row, or for a row whose request is not valid JSON.
static rjson::value make_export_summary(const export_summary_row& exp) {
    if (!exp.status || !exp.request) {
        throw std::runtime_error(fmt::format("Export '{}' has no {}", exp.export_arn, exp.status ? "request" : "export_status"));
    }
    rjson::value summary = rjson::empty_object();
    rjson::add(summary, "ExportArn", rjson::from_string(exp.export_arn));
    rjson::add(summary, "ExportStatus", reported_status(*exp.status));
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

    service::query_state qs(client_state, std::move(trace_state), std::move(permit));
    auto exports = co_await get_all_exports(_sdks, nullptr, &qs);

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

// Manifest files report times as ISO 8601 UTC with millisecond precision,
// e.g. "2020-11-04T07:28:34.028Z".
static sstring format_manifest_time(db_clock::time_point tp) {
    auto millis = tp.time_since_epoch().count() % 1000;
    return fmt::format("{:%FT%T}.{:03}Z", fmt::gmtime(db_clock::to_time_t(tp)), millis);
}

// The fiber of one export. The state is the pair (export_status, node_id) of the export row.
// The worker changes the row from IN_PROGRESS to COMPLETED or FAILED. The garbage collector
// changes the row of a dead incarnation from IN_PROGRESS to FAILED. Each write is
// conditional on the pair, so a late worker cannot overwrite a takeover.
static future<> run_export(
        service::storage_proxy& proxy,
        db::system_distributed_keyspace& sdks,
        abort_source& as,
        schema_ptr schema,
        export_row row,
        export_s3_client s3_client,
        std::string table_arn,
        std::string s3_bucket,
        std::string s3_prefix) {
    // The full export is maintenance work, not only the scan. The fiber starts in the scheduling
    // group of the request, so change it.
    co_await coroutine::switch_to(proxy.local_db().get_export_import_scheduling_group());

    int64_t item_count = 0;

    // The block below throws to report a failure. The message comes from the exception. A step
    // that has a different failure code sets it before it throws.
    std::string failure_code = "Exception";
    std::string failure_message;
    try {
        // As in DynamoDB, all objects of the export are in a directory with the export id as name.
        // Do not add a second '/' if the prefix ends with '/'.
        auto s3_key_prefix = s3_prefix.empty() || s3_prefix.ends_with('/')
            ? fmt::format("{}AWSDynamoDB/{}/", s3_prefix, row.export_id_token)
            : fmt::format("{}/AWSDynamoDB/{}/", s3_prefix, row.export_id_token);
        auto& client = s3_client.client;
        // s3::client addresses objects as "/bucket/key".
        auto s3_object_name = [&s3_bucket](std::string_view key) {
            return fmt::format("/{}/{}", s3_bucket, key);
        };
        // Writes the manifest file "<name>.json" and the object "<name>.md5" with the base64 md5
        // of its content. Returns the key of the .json object.
        auto put_manifest = [&client, &s3_object_name, &s3_key_prefix, &as](std::string_view name, std::string content) -> future<sstring> {
            sstring json_key = fmt::format("{}{}.json", s3_key_prefix, name);
            auto md5_key = fmt::format("{}{}.md5", s3_key_prefix, name);
            md5_hasher hasher;
            hasher.update(content.data(), content.size());
            auto md5_content = base64_encode(hasher.finalize());
            co_await client->put_object(s3_object_name(json_key), temporary_buffer<char>(content.data(), content.size()), s3::object_metadata{}, &as);
            co_await client->put_object(s3_object_name(md5_key), temporary_buffer<char>(md5_content.data(), md5_content.size()), s3::object_metadata{}, &as);
            co_return json_key;
        };

        // Create the data file object key
        auto data_object_key = fmt::format("{}data/{}.json.gz", s3_key_prefix, row.export_id_token);

        try {
            // Create empty _started object
            co_await client->put_object(s3_object_name(fmt::format("{}_started", s3_key_prefix)), temporary_buffer<char>(), s3::object_metadata{}, &as);
        }
        catch (const storage_io_error& e) {
            // s3::client maps all "not found" errors (NoSuchBucket, HTTP 404) to ENOENT, so we
            // cannot be sure that the bucket is missing. Usually it is, so use the DynamoDB code,
            // but put the server error in the message.
            if (e.code().value() == ENOENT) {
                failure_code = "S3NoSuchBucket";
                throw std::runtime_error(fmt::format("The specified bucket does not exist or could not be reached: {}", e.what()));
            }
            // s3::client maps 401, 403 and AccessDenied to EACCES.
            if (e.code().value() == EACCES) {
                failure_code = "S3AccessDenied";
                throw std::runtime_error(fmt::format("Access to the bucket was denied: {}", e.what()));
            }
            throw;
        }

        // Create the S3 sink pipeline.
        // Always call `flush_and_close()`. It is asynchronous, so RAII is not possible. Thus we
        // keep the result of the scan in a future and rethrow it after the close.
        // After a failed scan, the close completes the upload, so the partial data file stays in
        // the bucket. Only an abort aborts the multipart upload.
        auto pipeline = co_await create_sink_pipeline(s3_target_config{ client, s3_object_name(data_object_key), &as }, gzip_compression{});

        // Scan the table and feed items through the pipeline
        auto scan = co_await coroutine::as_future(export_scan_table(proxy, schema, as, empty_service_permit(), [&pipeline, &item_count](rjson::value item) -> future<> {
            rjson::value wrapper = rjson::empty_object();
            rjson::add(wrapper, "Item", std::move(item));
            co_await pipeline->process(wrapper);
            ++item_count;
        }));

        auto flushed = co_await coroutine::as_future(pipeline->flush_and_close());
        pipeline.reset();
        if (scan.failed()) {
            // The scan failed first, so report its exception.
            flushed.ignore_ready_future();
            std::rethrow_exception(scan.get_exception());
        }
        auto export_result = flushed.get();

        row.completed_at = db_clock::now();
        row.item_count = item_count;
        // ScyllaDB does not bill exports.
        row.billed_size_bytes = 0;

        // Make and upload the manifest files.
        // manifest-files.json - JSON lines, one object for each data file.
        rjson::value data_file_entry = rjson::empty_object();
        rjson::add(data_file_entry, "itemCount", rjson::value(item_count));
        rjson::add(data_file_entry, "md5Checksum", rjson::from_string(export_result.md5));
        rjson::add(data_file_entry, "etag", rjson::from_string(export_result.etag));
        rjson::add(data_file_entry, "dataFileS3Key", rjson::from_string(data_object_key));
        auto manifest_files_content = rjson::print(data_file_entry) + "\n";
        auto manifest_files_key = co_await put_manifest("manifest-files", std::move(manifest_files_content));

        // manifest-summary.json
        rjson::value summary = rjson::empty_object();
        rjson::add(summary, "version", "2020-06-30");
        rjson::add(summary, "exportArn", rjson::from_string(row.export_arn));
        rjson::add(summary, "startTime", rjson::from_string(format_manifest_time(row.accepted_at)));
        rjson::add(summary, "endTime", rjson::from_string(format_manifest_time(*row.completed_at)));
        rjson::add(summary, "tableArn", rjson::from_string(table_arn));
        rjson::add(summary, "tableId", rjson::from_string(fmt::to_string(schema->id())));
        // We do not support point-in-time exports, and the scan is not a snapshot: each page has
        // a new timestamp, between `startTime` and `endTime`. We report `exportTime` for
        // compatibility. It is the requested time, in the 5 minutes before the accept time.
        rjson::add(summary, "exportTime", rjson::from_string(format_manifest_time(row.export_time)));
        rjson::add(summary, "s3Bucket", rjson::from_string(s3_bucket));
        rjson::add(summary, "s3Prefix", rjson::from_string(s3_prefix));
        // Alternator does not know the encryption of the bucket.
        rjson::add(summary, "s3SseAlgorithm", rjson::null_value());
        rjson::add(summary, "s3SseKmsKeyId", rjson::null_value());
        rjson::add(summary, "manifestFilesS3Key", rjson::from_string(manifest_files_key));
        rjson::add(summary, "billedSizeBytes", rjson::value(int64_t(0)));
        rjson::add(summary, "itemCount", rjson::value(item_count));
        rjson::add(summary, "outputFormat", "DYNAMODB_JSON");
        auto manifest_summary_content = rjson::print(summary);
        auto manifest_summary_key = co_await put_manifest("manifest-summary", std::move(manifest_summary_content));

        row.export_status = "COMPLETED";
        row.export_manifest = manifest_summary_key;
        if (!co_await try_write_final_export_row(sdks, row)) {
            // The garbage collector took the row. A write of FAILED does not apply either.
            xlogger.warn("Export {} completed, but the garbage collector took its row before", row.export_arn);
            co_return;
        }
        xlogger.debug("Export {} completed successfully with {} items", row.export_arn, item_count);
        co_return;
    } catch (const std::exception& e) {
        if (as.abort_requested()) {
            failure_code = node_shutdown_failure_code;
            failure_message = node_shutdown_failure_message;
        } else {
            failure_message = e.what();
        }
    } catch (...) {
        failure_message = "Unknown error";
    }

    // The export failed. Report it.
    row.completed_at = db_clock::now();
    xlogger.debug("Export {} failed with exception: {}", row.export_arn, failure_message);
    row.export_status = "FAILED";
    row.failure_code = std::move(failure_code);
    row.failure_message = std::move(failure_message);
    // The failure can occur after the success path set these fields (e.g. if the COMPLETED
    // write failed). update_export() writes all columns, so clear them.
    row.export_manifest = std::nullopt;
    row.item_count = std::nullopt;
    row.billed_size_bytes = std::nullopt;
    bool updated = co_await try_write_final_export_row(sdks, row);
    xlogger.debug("Export {} failed with {} items{}", row.export_arn, item_count,
            updated ? "" : ", but the row is not ours any more");
}

void executor::s3_exports::start_export(export_s3_client s3_client, schema_ptr schema, export_row row,
        std::string table_arn, std::string s3_bucket, std::string s3_prefix) {
    // Fire-and-forget, in the gate. `run_export()` writes failures to the export row, so an
    // exception from it is unexpected (e.g. bad_alloc), and it is only logged. If with_gate()
    // throws because the gate is closed (it throws at once, it does not return a failed future),
    // the fiber does not start.
    auto export_arn = row.export_arn;
    (void)with_gate(_gate, [&] {
        return futurize_invoke(run_export, _e._proxy, _e._sdks, _as, std::move(schema), std::move(row), std::move(s3_client),
                std::move(table_arn), std::move(s3_bucket), std::move(s3_prefix))
                .handle_exception([export_arn = std::move(export_arn)] (std::exception_ptr ep) {
            xlogger.error("Background export fiber for {} failed unexpectedly: {}", export_arn, ep);
        });
    });
}

} // namespace alternator
