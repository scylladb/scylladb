/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/export.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "alternator/error.hh"
#include "alternator/serialization.hh"
#include "alternator/system_distributed_helper.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "cql3/untyped_result_set.hh"
#include "db/system_distributed_keyspace.hh"
#include "gms/gossiper.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"
#include "service/client_state.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "utils/log.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "utils/rjson.hh"
#include "utils/s3/client.hh"
#include <algorithm>
#include <array>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <zlib.h>


namespace alternator {

logging::logger elogger("alternator-export");

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


future<> scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    noncopyable_function<future<>(rjson::value)> cb)
{
    // Build a wildcard selection (SELECT *) for all columns.
    auto selection = cql3::selection::selection::wildcard(schema);

    // Collect all regular column IDs to read.
    auto regular_columns =
        schema->regular_columns()
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
        std::move(ck_bounds), {}, std::move(regular_columns), opts);

    auto command = ::make_lw_shared<query::read_command>(
        schema->id(), schema->version(), partition_slice,
        proxy.get_max_result_size(partition_slice),
        query::tombstone_limit(proxy.get_tombstone_limit()));

    // Use an internal client state - no authorization checks needed for
    // this internal scan operation.
    auto& client_state = service::client_state::for_internal_calls();
    tracing::trace_state_ptr trace_state;
    auto query_state_ptr = std::make_unique<service::query_state>(
        client_state, trace_state, empty_service_permit());

    db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;
    auto query_options = std::make_unique<cql3::query_options>(
        cl, std::vector<cql3::raw_value>{});
    lw_shared_ptr<service::pager::paging_state> paging_state = nullptr;
    query_options = std::make_unique<cql3::query_options>(
        std::move(query_options), std::move(paging_state));

    // Scan the full token ring.
    dht::partition_range_vector partition_ranges{
        dht::partition_range::make_open_ended_both_sides()};

    auto p = service::pager::query_pagers::pager(
        proxy, schema, selection, *query_state_ptr, *query_options,
        command, std::move(partition_ranges), nullptr);

    while (!p->is_exhausted()) {
        // Fetch one page at a time. The page size in bytes is bounded by
        // the read command's max_result_size; we don't limit by row count.
        uint32_t limit = std::numeric_limits<uint32_t>::max();
        std::unique_ptr<cql3::result_set> rs = co_await p->fetch_page(
            limit, gc_clock::now(), executor::default_timeout());

        for (const auto& row : rs->rows()) {
            rjson::value item = rjson::empty_object();
            // Convert each row to a DynamoDB-style JSON item using the
            // same logic as describe_single_item() from executor_util.
            describe_single_item(*selection, row, std::nullopt, item);
            co_await cb(std::move(item));
        }
    }
}

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

// --- Export orchestration helpers ---

// Build node_id string: "{host_id}:{gossip_generation}"
// This uniquely identifies a node incarnation and changes on reboot.
sstring executor::get_self_node_id() {
    auto host_id = _gossiper.my_host_id();
    auto ep_state = _gossiper.get_this_endpoint_state_ptr();
    auto generation = ep_state->get_heart_beat_state().get_generation();
    return fmt::format("{}:{}", host_id, generation.value());
}

static locator::host_id get_host_id_from_node_id(const sstring& node_id) {
    auto pos = node_id.find(':');
    if (pos == sstring::npos) {
        throw std::invalid_argument(fmt::format("Invalid node_id format: {}", node_id));
    }
    return locator::host_id{ utils::UUID{ node_id.substr(0, pos) } };
}
// Canonicalize a JSON request by sorting object members alphabetically.
// This produces a deterministic string representation for idempotency checks.
static sstring canonicalize_request(const rjson::value& request) {
    if (!request.IsObject()) {
        return rjson::print(request);
    }
    // Collect member names and sort them
    std::vector<std::string_view> names;
    names.reserve(request.MemberCount());
    for (auto it = request.MemberBegin(); it != request.MemberEnd(); ++it) {
        names.push_back(std::string_view(it->name.GetString(), it->name.GetStringLength()));
    }
    std::sort(names.begin(), names.end());

    // Build sorted JSON object
    rjson::value sorted = rjson::empty_object();
    for (auto& name : names) {
        const auto* val = rjson::find(request, name);
        if (val) {
            rjson::add_with_string_name(sorted, name, rjson::copy(*val));
        }
    }
    return rjson::print(sorted);
}

// Generate random hex string of specified length (in hex chars).
static sstring generate_random_hex(size_t hex_chars) {
    static thread_local std::mt19937 rng(std::random_device{}());
    static constexpr char hex_digits[] = "0123456789abcdef";
    sstring result;
    result.resize(hex_chars);
    for (size_t i = 0; i < hex_chars; ++i) {
        result[i] = hex_digits[rng() % 16];
    }
    return result;
}

// Generate a unique export ARN.
// Format: arn:scylla:alternator:::table/{table_name}/export/{epoch_millis_zero_padded}-{16_hex_chars}
static sstring generate_export_arn(std::string_view keyspace_name, std::string_view table_name) {
    auto epoch_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto random_suffix = generate_random_hex(16);
    return fmt::format("arn:scylla:alternator:::table/{}/export/{:019d}-{}",
        table_name, epoch_millis, random_suffix);
}

// Build an ExportDescription JSON response from a CQL result row.
static rjson::value build_export_description_response(const export_row& row) {
    rjson::value export_desc = rjson::empty_object();

    auto export_arn = row.export_arn;
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));

    rjson::add(export_desc, "ExportStatus", rjson::from_string(row.export_status));

    if (!row.export_manifest.empty()) {
        rjson::add(export_desc, "ExportManifest", rjson::from_string(row.export_manifest));
    }
    rjson::add(export_desc, "ItemCount", rjson::value(row.item_count));

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

    // BilledSizeBytes - we always report 0 (not applicable for Scylla)
    if (row.item_count > 0) {
        rjson::add(export_desc, "BilledSizeBytes", rjson::value(int64_t(0)));
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

    elogger.debug("Garbage collection: {} client tokens, {} exports, {} live nodes, self node_id={}",
        client_tokens.size(), exports.size(), live_nodes.size(), self_node_id);

    auto is_node_alive = [&](const sstring& node_id) {
        auto host_id = get_host_id_from_node_id(node_id);
        return std::find(live_nodes.begin(), live_nodes.end(), host_id) != live_nodes.end();
    };

    std::unordered_map<sstring, export_row&> export_arn_to_export;
    for (auto& export_row : exports) {
        // We ignore rows, which are under control of node, which is alive, those are fine.
        if (!is_node_alive(export_row.node_id)) {
            elogger.debug("Export row {} is under control of dead node {}", export_row.export_arn, export_row.node_id);
            export_arn_to_export.insert({ export_row.export_arn, export_row });
        }
    }
    for(auto &ct : client_tokens) {
        // We ignore rows, which are under control of node, which is alive, those are fine.
        if (is_node_alive(ct.node_id)) continue;

        elogger.debug("Client token row {} is under control of dead node {}", ct.client_token, ct.node_id);
        auto it = export_arn_to_export.find(ct.export_arn);
        if (it == export_arn_to_export.end()) {
            // Node died before writing the export row, let's fill in the missing row with `FAILED` status.
            // The export itself never happened, so there's nothing to clean up here on S3.

            elogger.debug("Client token row {} has no corresponding export row, creating a FAILED export row", ct.client_token);
            // Parse user request from client token row.
            rjson::value request;
            try {
                request = rjson::parse(ct.request);
            } catch (...) {
                elogger.debug("Failed to parse request from client token row {}: {}", ct.client_token, ct.request);
                // This can't happen in normal operation.
                // If we can't parse the request, we will invent data on the fly.
                request = rjson::empty_object();
            }

            elogger.debug("Creating FAILED export row for client token {}: export_arn={}", ct.client_token, ct.export_arn);
            co_await insert_export(_qp, {
                .export_arn = ct.export_arn,
                .client_token = ct.client_token,
                .request = ct.request,
                .export_status = "FAILED",
                .failure_code = "NodeFailure",
                .failure_message = "The node that initiated the export has failed before the export could start.",
                .export_id_token = "",
                .accepted_at = db_clock::now(),
                .completed_at = db_clock::time_point::min(),
                .node_id = self_node_id,
            });
            continue;
        }

        elogger.debug("Client token row {} has corresponding export row {}", ct.client_token, it->second.export_arn);
        auto &export_row = it->second;
        if (export_row.export_status == "COMPLETED" || export_row.export_status == "FAILED") {
            // The export completed successfully.
            elogger.debug("Export row {} is already in terminal state {}, skipping", export_row.export_arn, export_row.export_status);
            continue;
        }

        // Either the export is in progress or is in progress of cleaning up, in both cases the node handling it is dead.
        // We take over by first updating `node_id` and `export_status` to show our ownership.

        auto new_export_row = export_row;
        new_export_row.export_status = "FAILING";
        new_export_row.node_id = self_node_id;
        elogger.debug("Marking export row {} as FAILING", export_row.export_arn);

        if (!co_await update_export(_qp, new_export_row, export_row.export_status, export_row.node_id)) {
            // Other node is already handling it and is quicker, so we just skip this.
            elogger.debug("Failed to update export row {} to FAILING, it may have been updated by another node", export_row.export_arn);
            continue;
        }

        // TODO: update TTLs

        elogger.debug("Marking export row {} as FAILED", export_row.export_arn);
        new_export_row.export_status = "FAILED";
        if (!co_await update_export(_qp, new_export_row, export_row.export_status, export_row.node_id)) {
            // This should never happen:
            // - either there was some write issue and node_id / export_status were not correctly updated, or
            // - another node took over and updated the row to a different status.
            // In both cases we can't do anything about it, so we just skip - the other node will handle it.
            elogger.debug("Failed to update export row {} to FAILED, it may have been updated by another node", export_row.export_arn);
            continue;
        }
    }
    elogger.debug("Garbage collection: finished");
}

future<> executor::garbage_collector_for_s3_exports() {
    while(true) {
        co_await seastar::sleep(std::chrono::hours(4));
        co_await garbage_collect_s3_exports();
    }
}

future<executor::request_return_type> executor::export_table_to_point_in_time(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.export_table_to_point_in_time++;

    elogger.debug("Request: {}", rjson::print(request));

    // Required parameters
    auto table_arn = get_non_empty_string_attribute(request, "TableArn");
    auto s3_bucket = get_non_empty_string_attribute(request, "S3Bucket");

    // Validate that the table exists
    auto parts = parse_arn(table_arn, "TableArn", "table", "");
    schema_ptr schema;
    try {
        schema = _proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
    } catch (const data_dictionary::no_such_column_family&) {
        elogger.debug("Table not found: {}", table_arn);
        co_return api_error::table_not_found(
                fmt::format("Table not found: {}", table_arn));
    }

    // Optional parameters
    auto s3_prefix = get_non_empty_string_attribute(request, "S3Prefix", "");
    auto export_format = get_non_empty_string_attribute(request, "ExportFormat", "DYNAMODB_JSON");
    if (export_format != "DYNAMODB_JSON") {
        elogger.debug("Invalid ExportFormat: {}", export_format);
        co_return api_error::validation(
                fmt::format("ExportFormat attribute: must be DYNAMODB_JSON, not `{}`", export_format));
    }

    auto export_type = get_non_empty_string_attribute(request, "ExportType", "FULL_EXPORT");
    if (export_type != "FULL_EXPORT") {
        elogger.debug("Invalid ExportType: {}", export_type);
        co_return api_error::validation(
                fmt::format("ExportType attribute: must be FULL_EXPORT, not `{}`", export_type));
    }

    // IncrementalExportSpecification - not supported
    const rjson::value* incremental_v = rjson::find(request, "IncrementalExportSpecification");
    if (incremental_v) {
        elogger.debug("IncrementalExportSpecification attribute is not supported");
        co_return api_error::validation("IncrementalExportSpecification attribute is not supported");
    }

    // ExportTime - only "now" (or close to now) is supported
    // If not specified, use current time. If specified, must be within 5 minutes of now.
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    int64_t export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    if (export_time_v) {
        if (!export_time_v->IsNumber()) {
            elogger.debug("Invalid ExportTime: not a number");
            co_return api_error::validation(fmt::format("Expected number attribute ExportTime, got {}.", export_time_v->GetType()));
        }
        export_time = static_cast<int64_t>(export_time_v->GetDouble());
        auto diff = export_time > now ? export_time - now : now - export_time;
        if (diff > 300) {
            elogger.debug("Invalid ExportTime: {} is more than 5 minutes away from current time {}", export_time, now);
            co_return api_error::invalid_export_time(fmt::format("ExportTime must be within 5 minutes of current time. "
                                "ExportTime: {}, current time: {}", export_time, now));
        }
    }

    // ClientToken - for idempotency
    auto user_client_token = get_non_empty_string_attribute(request, "ClientToken", "");

    // S3BucketOwner - accepted but not used
    auto s3_bucket_owner = get_non_empty_string_attribute(request, "S3BucketOwner", "");

    // S3SseAlgorithm and S3SseKmsKeyId - accepted but not used
    auto s3_sse_algorithm = get_non_empty_string_attribute(request, "S3SseAlgorithm", "");
    if (!s3_sse_algorithm.empty() && s3_sse_algorithm != "AES256" && s3_sse_algorithm != "KMS") {
        elogger.debug("Invalid S3SseAlgorithm: {}", s3_sse_algorithm);
        co_return api_error::validation(
                fmt::format("S3SseAlgorithm parameter must be either AES256 or KMS, not `{}`.", s3_sse_algorithm));
    }

    auto s3_sse_kms_key_id = get_non_empty_string_attribute(request, "S3SseKmsKeyId", "");

    auto node_id = get_self_node_id();

    // Canonicalize the request for idempotency checking
    auto canonical_request = canonicalize_request(request);

    // Handle ClientToken: prefix with "u:" for user-supplied, "r:" for random
    sstring client_token;
    if (user_client_token.empty()) {
        client_token = fmt::format("r:{}", generate_random_hex(16));
    } else {
        client_token = fmt::format("u:{}", user_client_token);
    }

    // Generate export_id_token (random identifier used in S3 key paths)
    auto export_id_token = generate_random_hex(8);

    // Generate a unique export ARN
    auto export_arn = generate_export_arn(sstring(parts.table_name));

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
    // The order matters, because it's possible that another node is running the export with the same client token -
    // export metadata table is keyed by export_arn, which contains a random suffix and we can't use it to detect this situation. 
    auto [ client_row, client_token_inserted ] = co_await get_or_insert_client_row(_qp, client_token);

    // We define it early because of `goto` later on.
    bool export_row_inserted = false;
    export_row erow;

    if (!client_token_inserted) {
        elogger.debug("ClientToken {} already exists", client_token);
        // Export with this client token already exists. Check if the request matches.
        if (canonical_request != client_row.request) {
            // Easy - different request, so we fail with export conflict as Amazon requires us.
            elogger.debug("Export conflict: different parameters");
            co_return api_error::export_conflict(
                "An export with this ClientToken already exists with different parameters");
        }

        // We need an export row for this client token - the export might be in progress or completed or failed.
        auto export_row = co_await get_export(_qp, client_row.export_arn);
        elogger.debug("Export row for ClientToken {}: {}", client_token, export_row ? "found" : "not found");
        if (!export_row) {
            // Export row doesn't exist. Two possibilities:
            // - the other node is doing the export in the same moment and hasn't inserted the export row yet (but managed
            //   to insert client token row first), or
            // - the other node doing the export died before inserting the export row, leaving a dangling client token row
            // In both cases we will return IN_PROGRESS status - the dangling client token row will be taken care of by
            // garbage collection thread.
            elogger.debug("Returning IN_PROGRESS status for ClientToken {}", client_token);
            goto build_in_progress_response;
        }

        // Export row exists, other node is (or was) running the export. Return as if DescribeExport was called.
        elogger.debug("Returning existing export description for ClientToken {}", client_token);
        auto response = build_export_description_response(*export_row);
        co_return rjson::print(std::move(response));
    }

    erow = {
        .export_arn = export_arn,
        .client_token = client_token,
        .request = canonical_request,
        .export_status = "IN_PROGRESS",
        .export_id_token = export_id_token,
        .accepted_at = accepted_at,
        .completed_at = db_clock::time_point::min(),
        .node_id = node_id,
    };
    export_row_inserted = co_await insert_export(_qp, erow);

    if (!export_row_inserted) {
        // Another node inserted the export row concurrently.
        // This should never happen, but since we got here we will return as if DescribeExport was called.
        auto export_row = co_await get_export(_qp, client_row.export_arn);
        if (!export_row) {
            on_internal_error(elogger, fmt::format("Failed to insert export row for ClientToken {} and failed to read it back", client_token));
        }
        auto response = build_export_description_response(*export_row);
        elogger.debug("(?) Export row already exists - returning existing export description for ClientToken {}", client_token);
        co_return rjson::print(std::move(response));
    }

    // Launch background export fiber (fire-and-forget, gate-guarded)
    elogger.debug("Launching background export fiber for ClientToken {}", client_token);
    (void)run_export(_export_gate.hold(), schema, std::move(erow), table_arn,s3_bucket, s3_key_prefix, export_id_token);

    // Build the ExportDescription response
build_in_progress_response:
    elogger.debug("Returning IN_PROGRESS response for ClientToken {}", client_token);
    rjson::value export_desc = rjson::empty_object();
    if (!user_client_token.empty()) {
        rjson::add(export_desc, "ClientToken", rjson::from_string(user_client_token));
    }
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(export_format));
    rjson::add(export_desc, "ExportStatus", "IN_PROGRESS");
    rjson::add(export_desc, "ExportTime", rjson::value(export_time));
    rjson::add(export_desc, "ExportType", rjson::from_string(export_type));
    rjson::add(export_desc, "S3Bucket", rjson::from_string(s3_bucket));
    if (!s3_prefix.empty()) {
        rjson::add(export_desc, "S3Prefix", rjson::from_string(s3_prefix));
    }
    rjson::add(export_desc, "TableArn", rjson::from_string(table_arn));
    auto start_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        accepted_at.time_since_epoch()).count();
    rjson::add(export_desc, "StartTime", rjson::value(start_time_seconds));

    if (!s3_bucket_owner.empty()) {
        rjson::add(export_desc, "S3BucketOwner", rjson::from_string(s3_bucket_owner));
    }
    if (!s3_sse_algorithm.empty()) {
        rjson::add(export_desc, "S3SseAlgorithm", rjson::from_string(s3_sse_algorithm));
    }
    if (!s3_sse_kms_key_id.empty()) {
        rjson::add(export_desc, "S3SseKmsKeyId", rjson::from_string(s3_sse_kms_key_id));
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    co_return rjson::print(std::move(response));
}

future<> executor::run_export(
        seastar::gate::holder gate_holder,
        schema_ptr schema,
        export_row row,
        std::string table_arn,
        std::string s3_bucket,
        std::string s3_key_prefix,
        std::string export_id_token) {
    int64_t item_count = 0;
    auto node_id = get_self_node_id();
    std::string error_msg;

    try {
        auto client = get_s3_client(s3_bucket);

        // Create the data file object key
        auto data_object_key = fmt::format("{}data/{}.json.gz", s3_key_prefix, export_id_token);

        // Create S3 sink pipeline with gzip compression
        auto pipeline = create_s3_sink_pipeline(client, data_object_key, gzip_compression{});

        // Scan the table and feed items through the pipeline
        co_await scan_table(_proxy, schema, [&pipeline, &item_count](rjson::value item) -> future<> {
            rjson::value wrapper = rjson::empty_object();
            rjson::add(wrapper, "Item", std::move(item));
            co_await pipeline->process(wrapper);
            ++item_count;
        });
        row.completed_at = db_clock::now();

        co_await pipeline->flush_and_close();

        // Generate and upload manifest files
        auto data_file_entry = fmt::format(
            R"({{"dataFileS3Key":"{}","itemCount":{}}})", data_object_key, item_count);

        // manifest-files.json
        auto manifest_files_key = fmt::format("{}manifest-files.json", s3_key_prefix);
        auto manifest_files_content = fmt::format("[{}]", data_file_entry);
        co_await client->put_object(manifest_files_key,
            temporary_buffer<char>(manifest_files_content.data(), manifest_files_content.size()));

        // manifest-summary.json
        auto manifest_summary_key = fmt::format("{}manifest-summary.json", s3_key_prefix);
        auto manifest_summary_content = fmt::format(
            R"({{"version":"2020-06-30","exportArn":"{}","startTime":"{}","endTime":"{}",)"
            R"("tableArn":"{}","exportFormat":"DYNAMODB_JSON","billedSizeBytes":0,)"
            R"("itemCount":{},"outputFormat":"DYNAMODB_JSON","dataFileCount":1,)"
            R"("dataFileS3Key":"{}"}})",
            row.export_arn,
            std::chrono::duration_cast<std::chrono::seconds>(row.accepted_at.time_since_epoch()).count(),
            std::chrono::duration_cast<std::chrono::seconds>(row.completed_at.time_since_epoch()).count(),
            table_arn, item_count, data_object_key);
        co_await client->put_object(manifest_summary_key,
            temporary_buffer<char>(manifest_summary_content.data(), manifest_summary_content.size()));

        auto export_manifest = fmt::format("{}manifest-summary.json", s3_key_prefix);
        row.export_status = "COMPLETED";
        row.export_manifest = export_manifest;
        auto updated = co_await update_export(_qp, row, "IN_PROGRESS", node_id);
        if (!updated) {
            elogger.debug("Export {} failed to update status to IN_PROGRESS after completion", row.export_arn);
            co_return;
        }
        elogger.debug("Export {} completed successfully with {} items", row.export_arn, item_count);
        co_return;
    } catch (std::exception &e) {
        error_msg = e.what();
    } catch(...) {
        error_msg = "Unknown error";
    }
    // We have failed with an exception, let's report it.
    row.completed_at = db_clock::now();
    elogger.debug("Export {} failed with exception: {}", row.export_arn, error_msg);
    row.export_status = "FAILED";
    row.failure_code = "Exception";
    row.failure_message = std::string(error_msg);
    auto updated = co_await update_export(_qp, row, "IN_PROGRESS", node_id);
    if (!updated) {
        elogger.debug("Export {} failed to update status to FAILED after completion", row.export_arn);
        co_return;
    }
    elogger.debug("Export {} failed with {} items", row.export_arn, item_count);
}

// // Helper to update export status to FAILED, used after catching exceptions
// // in run_export (where co_await is not allowed inside catch blocks).
// future<> executor::update_export_status_to_failed(
//         const sstring& export_arn, const sstring& node_id,
//         int64_t item_count, const sstring& failure_message) {
//     try {
//         co_await _sdks.update_export_status(
//             export_arn, "FAILED",
//             item_count, db_clock::now(), std::nullopt,
//             sstring("INTERNAL_ERROR"), sstring(failure_message));
//     } catch (...) {
//         elogger.error("Export {} failed to update status after failure: {}",
//             export_arn, std::current_exception());
//     }
// }

future<executor::request_return_type> executor::describe_export(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_export++;

    auto export_arn = get_non_empty_string_attribute(request, "ExportArn");

    // Validate that the ARN looks like an export ARN.
    // Expected format: arn:scylla:alternator:::table/<name>/export/<id>
    // or AWS format: arn:aws:dynamodb:<region>:<account>:table/<name>/export/<id>
    if (!export_arn.starts_with("arn:")) {
        co_return api_error::validation(
            fmt::format("Invalid Export ARN: {}", export_arn));
    }

    auto result = co_await get_export(_qp, sstring(export_arn));
    if (!result) {
        co_return api_error::export_not_found(
            fmt::format("Export not found: {}", export_arn));
    }

    auto response = build_export_description_response(*result);
    co_return rjson::print(std::move(response));
}

static std::string get_table_arn_from_export_arn(std::string_view export_arn) {
    // Expected format: arn:scylla:alternator:::table/<name>/export/<id>
    // or AWS format: arn:aws:dynamodb:<region>:<account>:table/<name>/export/<id>
    auto pos = export_arn.find("/export/");
    if (pos == std::string::npos) {
        throw std::invalid_argument(fmt::format("Invalid Export ARN format: {}", export_arn));
    }
    return export_arn.substr(0, pos);
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
        if (!table_arn_filter.empty() && row.table_arn != table_arn_filter) {
            continue;
        }
        summaries.push_back({row.export_arn, row.export_status, row.table_arn});
    }

    // Sort by export_arn ascending
    std::sort(summaries.begin(), summaries.end(),
        [](const export_summary& a, const export_summary& b) {
            return a.export_arn < b.export_arn;
        });

    // Apply pagination: skip past next_token
    auto it = summaries.begin();
    if (!next_token.empty()) {
        it = std::find_if(summaries.begin(), summaries.end(),
            [&next_token](const export_summary& s) {
                return s.export_arn > next_token;
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
        rjson::add(summary, "ExportType", "FULL_EXPORT");
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
