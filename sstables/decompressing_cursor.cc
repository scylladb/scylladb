/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/decompressing_cursor.hh"

#include <algorithm>
#include <optional>

#include <fmt/std.h>

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "tracing/traced_file.hh"

namespace sstables {

namespace {

static constexpr uint64_t disk_block_size = 512;

// The end of the part of Data.db that a read of `end` needs: the end of the
// chunk that contains `end`. If `end` is at the start of a chunk, no byte of
// that chunk is needed. Otherwise, the extent hint gives an upper bound of
// the chunk end. It can point past the end of the file, so clamp it. Without a
// hint, read to the end of the file.
uint64_t source_end(sstable_position end, uint64_t file_size) {
    const auto& ph = end.as_physical();
    if (ph.offset_within_chunk == 0) {
        return std::min<uint64_t>(ph.chunk_position, file_size);
    }
    if (ph.extent_hint) {
        return std::min<uint64_t>(ph.chunk_position + ph.extent_hint, file_size);
    }
    return file_size;
}

// Reads chunks forward from an input_stream. It can't read backwards.
class sequential_chunk_source final : public framed_chunk_source {
    decompressing_cursor_source_opener _open_source;
    framed_chunk_codec _codec;
    uint64_t _compressed_file_length;
    std::optional<input_stream<char>> _input;
    // The file position of the next byte that _input returns.
    uint64_t _input_pos;

    future<temporary_buffer<char>> read_exactly(uint64_t n) {
        auto buf = co_await _input->read_exactly(n);
        if (buf.size() < n) {
            throw_malformed_sstable_exception(format("Unexpected eof at {}, wanted {}, got {}.",
                    _input_pos + buf.size(), n, buf.size()));
        }
        _input_pos += n;
        co_return buf;
    }
public:
    sequential_chunk_source(decompressing_cursor_source_opener open_source, framed_chunk_codec codec,
            uint64_t compressed_file_length, uint64_t source_start)
        : _open_source(std::move(open_source))
        , _codec(codec)
        , _compressed_file_length(compressed_file_length)
        , _input_pos(source_start)
    {}

    // The header gives the exact extent, so the hint is not needed. The caller
    // already bounded the source by the end of the range.
    future<raw_chunk> read_chunk(uint64_t chunk_start, uint64_t) override {
        if (chunk_start < _input_pos) {
            on_internal_error(sstlog, format(
                    "sequential_chunk_source can't read backwards: chunk_start={}, input_pos={}", chunk_start, _input_pos));
        }
        if (!_input) {
            _input = input_stream<char>(co_await _open_source());
        }
        if (_input_pos != chunk_start) {
            co_await _input->skip(chunk_start - _input_pos);
            _input_pos = chunk_start;
        }
        auto header = co_await read_exactly(_codec.frame_len());
        const auto extent = _codec.extent_from_frame(header.get());
        // A corrupt header can contain a length that extent_from_frame()
        // accepts, but that puts the end of the chunk past the end of the file.
        // Report this as a corrupt header, not as an unexpected end of file.
        if (chunk_start + extent > _compressed_file_length) {
            throw_malformed_sstable_exception(format(
                    "compressed chunk at file offset {} has a corrupt length header: chunk of size {} would end at {}, past the compressed file length {}",
                    chunk_start, extent, chunk_start + extent, _compressed_file_length));
        }
        auto rest = co_await read_exactly(extent - _codec.frame_len());
        co_return raw_chunk{std::move(header), std::move(rest)};
    }

    future<> close() override {
        if (_input) {
            co_await _input->close();
        }
    }
};

} // anonymous namespace

// The offset of a position comes from the index, which has no checksum, so
// check it against the decompressed size of the chunk. Positions are
// normalised: the end of a chunk is written as the start of the next chunk. So
// an offset equal to the chunk size is invalid too.
static void check_offset_within_chunk(uint64_t chunk_start, int64_t offset_within_chunk, size_t chunk_size) {
    if (offset_within_chunk < 0 || uint64_t(offset_within_chunk) >= chunk_size) {
        throw_malformed_sstable_exception(format(
                "In compressed chunk at position {}, ended up with in-chunk offset {} not below chunk size {}",
                chunk_start, offset_within_chunk, chunk_size));
    }
}

decompressing_cursor::decompressing_cursor(std::unique_ptr<framed_chunk_source> source, framed_chunk_codec codec,
        const compressor& compressor, reader_permit permit, uint64_t compressed_file_length,
        disk_read_range range, std::optional<uint32_t> digest)
    : continuous_data_consumer_input_stream(_stream_position)
    , _source(std::move(source))
    , _codec(codec)
    , _compressor(compressor)
    , _permit(std::move(permit))
    , _compressed_file_length(compressed_file_length)
    , _stream_position(range.start)
    , _end(range.end)
    , _expected_digest(digest)
    , _actual_digest(crc32_utils::init_checksum())
{
    SCYLLA_ASSERT(_codec.framed());
    SCYLLA_ASSERT(range.start.is_physical());
    SCYLLA_ASSERT(range.end.is_physical());
}

bool decompressing_cursor::has_current_chunk() const {
    return _chunk && _chunk->position == uint64_t(_stream_position.position.as_physical().chunk_position);
}

future<> decompressing_cursor::load_chunk() {
    auto& pos = _stream_position.position.as_physical();
    const uint64_t chunk_start = pos.chunk_position;
    _chunk.reset();
    auto raw = co_await _source->read_chunk(chunk_start, pos.extent_hint);
    auto parsed = _codec.parse(std::string_view(raw.header.get(), raw.header.size()),
            std::string_view(raw.rest.get(), raw.rest.size()));
    const auto checksum = parsed.verify_checksum<crc32_utils>(chunk_start);
    add_to_digest(chunk_start, parsed, checksum);
    auto units = co_await _permit.request_memory(_codec.uncompressed_chunk_length());
    auto data = make_tracked_temporary_buffer(_codec.decompress(_compressor, parsed), std::move(units));
    check_offset_within_chunk(chunk_start, pos.offset_within_chunk, data.size());
    _chunk = chunk{chunk_start, parsed.extent(), std::move(data)};
    pos.extent_hint = parsed.extent();
}

void decompressing_cursor::add_to_digest(uint64_t chunk_start, const framed_chunk& chunk, uint32_t checksum) {
    if (!_expected_digest || chunk_start != _next_digest_chunk_position) {
        return;
    }
    // `mu` uses crc32 checksums, and its digest covers the per-chunk checksums.
    _actual_digest = chunk.fold_into_digest<crc32_utils>(_actual_digest, checksum, true);
    _next_digest_chunk_position = chunk_start + chunk.extent();
    if (_next_digest_chunk_position == _compressed_file_length) {
        if (_actual_digest != *_expected_digest) {
            throw_malformed_sstable_exception(format(
                    "Digest mismatch: expected={}, actual={}", *_expected_digest, _actual_digest));
        }
        _expected_digest.reset();
    }
}

future<> decompressing_cursor::ensure_chunk_if_not_eof() {
    if (has_current_chunk()) {
        return make_ready_future<>();
    }
    // A chunk that starts at or after _end is past the range, and the source
    // may not contain it. Report the end of the stream instead, as the
    // logical stream does at the end of its range. For example, the parser
    // can skip a row with skip() that stops exactly at a chunk-aligned _end,
    // and then call consume_one(). Positions are ordered without the
    // extent hint.
    auto chunk_start = sstable_position::from_physical(_stream_position.position.as_physical().chunk_position, 0, 0);
    if (chunk_start >= _end) {
        return make_ready_future<>();
    }
    return load_chunk();
}

void decompressing_cursor::advance(uint64_t n) {
    auto& pos = _stream_position.position.as_physical();
    pos.offset_within_chunk += n;
    _stream_position.bytes_consumed += n;
    if (uint64_t(pos.offset_within_chunk) == _chunk->data.size()) {
        pos.chunk_position += _chunk->extent;
        pos.offset_within_chunk = 0;
        // The next chunk's extent is unknown until it is loaded.
        pos.extent_hint = 0;
    }
}

void decompressing_cursor::seek(sstable_position pos) {
    SCYLLA_ASSERT(pos.is_physical());
    _stream_position.position = pos;
    if (has_current_chunk()) {
        auto& ph = _stream_position.position.as_physical();
        check_offset_within_chunk(_chunk->position, ph.offset_within_chunk, _chunk->data.size());
        ph.extent_hint = _chunk->extent;
    }
}

void decompressing_cursor::init_stream_position(sstable_position pos) {
    seek(pos);
    _stream_position.bytes_consumed = 0;
}

future<> decompressing_cursor::skip_to(sstable_position target) {
    if (target < _stream_position.position) {
        throw_malformed_sstable_exception(format("decompressing_cursor cannot seek backwards: current={}, target={}",
                _stream_position.position, target));
    }
    seek(target);
    return make_ready_future<>();
}

future<> decompressing_cursor::skip(uint64_t n) {
    while (n != 0) {
        co_await ensure_chunk_if_not_eof();
        if (!has_current_chunk()) {
            throw_malformed_sstable_exception(format("decompressing_cursor hit end-of-file while skipping {} bytes", n));
        }
        const uint64_t step = std::min<uint64_t>(n, _chunk->data.size() - _stream_position.position.as_physical().offset_within_chunk);
        advance(step);
        n -= step;
    }
    if (!has_current_chunk()) {
        _chunk.reset();
    }
}

future<consumption_result<char>> decompressing_cursor::consume_one(std::optional<sstable_position> end_position, consumer_one_fn consumer) {
    sstlog.trace("[decompressing_cursor@{}] consume_one: enter pos={} end={}",
            fmt::ptr(this), _stream_position.position, end_position);
    co_await ensure_chunk_if_not_eof();
    if (!has_current_chunk()) {
        co_return consumer(temporary_buffer<char>());
    }

    auto& pos = _stream_position.position.as_physical();
    const size_t in_chunk = pos.offset_within_chunk;
    size_t size = _chunk->data.size() - in_chunk;
    if (end_position) {
        // Clip the buffer so that the consumer never sees past `end_position`.
        SCYLLA_ASSERT(end_position->is_physical());
        if (*end_position <= _stream_position.position) {
            size = 0;
        } else if (const auto& end_ph = end_position->as_physical(); end_ph.chunk_position == pos.chunk_position) {
            size = std::min<size_t>(size, end_ph.offset_within_chunk - in_chunk);
        }
    }

    auto data = _chunk->data.share(in_chunk, size);
    advance(size);

    auto result = consumer(std::move(data));
    if (auto* stop = std::get_if<stop_consuming<char>>(&result.get())) {
        const size_t remainder = stop->get_buffer().size();
        SCYLLA_ASSERT(remainder <= size);
        if (remainder != 0) {
            // advance() can have moved the position to the next chunk. Move it
            // back into _chunk.
            pos.chunk_position = _chunk->position;
            pos.extent_hint = _chunk->extent;
            pos.offset_within_chunk = in_chunk + size - remainder;
            _stream_position.bytes_consumed -= remainder;
        }
    }
    if (!has_current_chunk()) {
        _chunk.reset();
    }
    co_return result;
}

sstable_position decompressing_cursor::compute_relative_position(int64_t offset) {
    if (offset == 0) {
        return _stream_position.position;
    }
    // Only called from a consume_one() consumer, to locate a point in the
    // buffer that it got. That buffer came from _chunk, so the target is in
    // _chunk, before the position. The position is in _chunk, or at its end.
    SCYLLA_ASSERT(offset < 0);
    SCYLLA_ASSERT(_chunk.has_value());
    const uint64_t end_in_chunk = has_current_chunk()
            ? uint64_t(_stream_position.position.as_physical().offset_within_chunk)
            : _chunk->data.size();
    SCYLLA_ASSERT(uint64_t(-offset) <= end_in_chunk);
    return sstable_position::from_physical(_chunk->position, _chunk->extent, end_in_chunk + offset);
}

future<> decompressing_cursor::close() {
    _chunk.reset();
    return _source->close();
}

std::unique_ptr<decompressing_cursor>
make_decompressing_cursor(decompressing_cursor_source_opener open_source,
        disk_read_range range, const compressor& compressor,
        const framed_chunk_codec& codec, uint64_t compressed_file_length,
        reader_permit permit, std::optional<uint32_t> digest) {
    sstlog.trace("make_decompressing_cursor(source_opener): start={} end={} uncompressed_chunk_length={} compressed_file_length={}",
            range.start, range.end, codec.uncompressed_chunk_length(), compressed_file_length);
    const uint64_t source_start = align_down<uint64_t>(range.start.as_physical().chunk_position, disk_block_size);
    return std::make_unique<decompressing_cursor>(
            std::make_unique<sequential_chunk_source>(std::move(open_source), codec, compressed_file_length, source_start),
            codec, compressor, std::move(permit), compressed_file_length, range, digest);
}

std::unique_ptr<decompressing_cursor>
make_decompressing_cursor(shared_sstable sst, disk_read_range range,
        reader_permit permit, tracing::trace_state_ptr trace_state,
        file_input_stream_options options, std::optional<uint32_t> digest) {
    SCYLLA_ASSERT(range.start.is_physical());
    const uint64_t file_size = sst->ondisk_data_size();
    const uint64_t source_start = align_down<uint64_t>(range.start.as_physical().chunk_position, disk_block_size);
    // Bounding the source keeps read-ahead inside the range. For a
    // single-partition read, it also makes the first read cover the whole
    // partition when it fits in the buffer.
    const uint64_t source_len = std::max(source_end(range.end, file_size), source_start) - source_start;
    sstlog.trace("make_decompressing_cursor(sstable): start={} end={} source_start={} source_len={}",
            range.start, range.end, source_start, source_len);
    auto opener = [sst, permit, trace_state, source_start, source_len, options = std::move(options)] () mutable -> future<data_source> {
        file f = make_tracked_file(sst->get_data_file(), permit);
        if (trace_state) {
            f = tracing::make_traced_file(std::move(f), trace_state, seastar::format("{}:", sst->get_filename()));
        }
        return sst->get_storage().make_data_or_index_source(
                *sst, component_type::Data, std::move(f), source_start, source_len, std::move(options));
    };
    return make_decompressing_cursor(decompressing_cursor_source_opener(std::move(opener)), range,
            sst->get_compression().get_compressor(), sst->get_chunk_codec(), file_size,
            std::move(permit), digest);
}

} // namespace sstables
