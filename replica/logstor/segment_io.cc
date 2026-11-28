/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/segment_io.hh"

#include <exception>

#include <seastar/core/align.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/util/log.hh>

#include "serializer_impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

namespace {

// Returns nullopt if the stream ends before the header, if no chunk starts at the position, if the
// chunk is from an earlier use of the segment (its segment_seq is not `expected_seq`), or if the
// header is invalid.
//
// Throws std::runtime_error at a chunk with a format version that this code does not read or with
// an unknown flag. Such a chunk is from another release and does not mark the end of the segment,
// so recovery must not handle the segment as empty.
//
// The function checks segment_seq before the version and the flags, so that the chunks of an
// earlier use of the segment end the scan and do not fail it.
//
// The function checks the version before the CRC, because a chunk of another format version can
// have another header layout and another CRC, so this code cannot check the CRC of such a chunk. As
// a result, a bit flip in the version byte makes this function throw, and does not end the scan as
// other corruption does.
//
// The function checks the flags after the CRC, which covers them, so that a flag that a bit flip
// set ends the scan as other corruption does.
future<std::optional<ondisk::chunk_header>> read_chunk_header(segment_reader& reader, log_segment_id segment_id,
        std::optional<segment_sequence> expected_seq) {
    const auto position = reader.position();
    auto common = co_await reader.read<ondisk::chunk_header_common>();
    if (!common || common->magic != ondisk::chunk_header_magic) {
        co_return std::nullopt;
    }
    if (expected_seq && common->segment_seq != *expected_seq) {
        co_return std::nullopt;
    }
    if (!ondisk::can_read_version(common->version)) {
        throw std::runtime_error(fmt::format("Chunk at offset {} of {} has format version {}, and this release reads versions {} to {}",
                position, segment_id, common->version, ondisk::min_read_version, ondisk::current_version));
    }
    // The kind gives the size of the rest of the header, so it must be valid before the read.
    if (!ondisk::validate_chunk_header_fields(*common)) {
        co_return std::nullopt;
    }

    auto rest = co_await reader.read_exactly(ondisk::chunk_header_size(common->kind) - ondisk::chunk_header_common_size);
    auto ch = ondisk::parse_chunk_header(*common, std::span(rest.get(), rest.size()));
    if (!ch) {
        co_return std::nullopt;
    }
    if (const auto unknown = ondisk::unknown_chunk_flags(*ch)) {
        throw std::runtime_error(fmt::format("Chunk at offset {} of {} has flags {:#x} that this release does not define",
                position, segment_id, unknown));
    }
    co_return ch;
}

// The body of a chunk, and whether it matches the body CRC of the chunk header.
struct chunk_body {
    temporary_buffer<char> buf;
    bool valid;
};

// Reads the body of the chunk whose header the reader read last, and checks it against the body
// CRC of the header. Returns nullopt if the stream ends first.
future<std::optional<chunk_body>> read_chunk_body(segment_reader& reader, const ondisk::chunk_header& ch) {
    auto body = co_await reader.read_exactly(ch.records_size);
    if (body.size() < ch.records_size) {
        co_return std::nullopt;
    }
    const bool valid = ondisk::validate_chunk_body(ch, std::span(body.get(), body.size()));
    co_return chunk_body{std::move(body), valid};
}

} // anonymous namespace

chunk_records::iterator::iterator(const scanned_chunk& chunk)
    : _chunk(&chunk)
    , _records(chunk.record_bytes().data(), chunk.record_bytes().size())
{
    read_next();
}

void chunk_records::iterator::read_next() {
    _current.reset();
    if (_records.size() == 0) {
        return;
    }
    const size_t records_size = _chunk->record_bytes().size();
    const size_t record_offset = _chunk->_body_position + (records_size - _records.size());
    const auto segment_id = _chunk->_segment_id;
    std::optional<ondisk::parsed_record_frame> record;
    try {
        record = ondisk::try_read_record_frame(_records);
    } catch (const std::runtime_error&) {
        std::throw_with_nested(std::runtime_error(fmt::format("Cannot read the record at offset {} of {}", record_offset, segment_id)));
    }
    if (!record) {
        logstor_logger.warn("Skipped the rest of a chunk of {}: the record frame at offset {} does not fit in the chunk body, which matches its CRC. "
                "The segment is corrupt; the records from this offset to the end of the chunk are lost.", segment_id, record_offset);
        return;
    }

    logstor_logger.trace("Found record of size {} bytes in {}",
                        record->bytes.header.size() + record->bytes.value.size(), segment_id);

    const auto frame_size = ondisk::record_frame_size(record->bytes.header.size(), record->bytes.value.size());
    _current = chunk_record{
        std::move(*record),
        record_location{
            .segment = segment_id,
            .offset = static_cast<uint32_t>(record_offset),
            .size = static_cast<uint32_t>(frame_size),
        },
    };

    // The next record frame starts at a multiple of record_alignment.
    const size_t consumed = records_size - _records.size();
    _records.skip(std::min(align_up(consumed, ondisk::record_alignment) - consumed, _records.size()));
}

segment_scanner::segment_scanner(seastar::input_stream<char>& in, log_segment_id segment_id, size_t segment_size)
    : _reader(in)
    , _segment_id(segment_id)
    , _segment_size(segment_size)
{
    logstor_logger.trace("Reading records from {}", segment_id);
}

future<const scanned_chunk*> segment_scanner::next_chunk() {
    _chunk.reset();
    if (_state == scan_state::more) {
        _chunk = co_await read_chunk();
    }
    if (_chunk) {
        co_return &*_chunk;
    }
    if (_state != scan_state::done) {
        _state = scan_state::done;
        report_failed_chunks();
    }
    co_return nullptr;
}

future<std::optional<scanned_chunk>> segment_scanner::read_chunk() {
    // The next chunk starts at the first block boundary after the previous one.
    co_await _reader.skip_to(align_up(_reader.position(), ondisk::block_alignment));
    if (_reader.position() >= _segment_size) {
        co_return std::nullopt;
    }

    // A chunk with an invalid header, or a chunk of an earlier use of the segment, ends the scan.
    // Chunks are written in sequence, so no chunk of this use of the segment follows it.
    auto header = co_await read_chunk_header(_reader, _segment_id, _segment_seq);
    if (!header) {
        co_return std::nullopt;
    }
    const auto& ch = *header;
    _segment_seq = ch.segment.segment_seq;

    // A chunk whose body is cut short still gives its segment info. Recovery takes the highest
    // segment_seq from it, and a segment_seq that is on disk must not go to a new segment again,
    // even when the body of its chunk is torn.
    auto seg_info = ch.segment;
    switch (seg_info.kind()) {
    case segment_kind::mixed:
        break;
    case segment_kind::full:
        // A full segment has only one chunk.
        _state = scan_state::last;
        break;
    }

    const size_t body_position = _reader.position();
    if (body_position + ch.records_size > _segment_size) {
        // The writer never writes a body past the end of the segment, and a torn write keeps the size
        // in a valid header. So the chunk is corrupt, not torn.
        logstor_logger.warn("Skipped the last chunk of {}: its header gives a body of {} bytes at offset {}, past the end of the segment. "
                "The segment is corrupt; the records of the chunk are lost.", _segment_id, ch.records_size, body_position);
        _state = scan_state::last;
        _last_chunk_failed = false;
        co_return scanned_chunk(_segment_id, std::move(seg_info), {}, body_position, false);
    }
    auto body = co_await read_chunk_body(_reader, ch);
    if (!body) {
        _state = scan_state::last;
        _last_chunk_failed = true;
        ++_failed_chunks;
        co_return scanned_chunk(_segment_id, std::move(seg_info), {}, body_position, false);
    }

    if (body->valid) {
        _last_chunk_failed = false;
    } else {
        // The valid header gives the size of the chunk, so the scan continues at the next chunk.
        // After a torn chunk, what follows is not a valid chunk or is a chunk of an earlier use of
        // the segment, which ends the scan.
        _last_chunk_failed = true;
        ++_failed_chunks;
    }
    co_return scanned_chunk(_segment_id, std::move(seg_info), std::move(body->buf), body_position, body->valid);
}

void segment_scanner::report_failed_chunks() const {
    // A segment has one chunk write in flight at a time, and a write is durable when it completes.
    // Thus a crash can tear only the last chunk of a segment. A failed chunk before it is corrupt.
    if (_failed_chunks == 1 && _last_chunk_failed) {
        logstor_logger.warn("Skipped the last chunk of {}: its body does not match its CRC or is cut short, probably a torn write from a crash.",
                _segment_id);
    } else if (_failed_chunks > 0) {
        logstor_logger.warn("Skipped {} chunks of {} whose body does not match its CRC or is cut short, including a chunk before the last. "
                "The segment is corrupt; their records are lost.", _failed_chunks, _segment_id);
    }
}

future<std::optional<segment_info>> read_chunk_info(seastar::input_stream<char>& in, log_segment_id segment_id) {
    segment_reader reader(in);
    auto header = co_await read_chunk_header(reader, segment_id, std::nullopt);
    co_return header.transform(&ondisk::chunk_header::segment);
}

streamed_segment_rewriter::streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer)
    : _target_segment(target_segment)
    , _target_seq(target_seq)
    , _on_buffer(std::move(on_buffer)) {
}

ondisk::chunk_header_common streamed_segment_rewriter::read_chunk_header_common() const {
    simple_memory_input_stream in(_pending_data.data(), ondisk::chunk_header_common_size);
    return ser::deserialize(in, std::type_identity<ondisk::chunk_header_common>{});
}

void streamed_segment_rewriter::maybe_parse_initial_header() {
    if (_initial_header_size || _pending_data.size() < ondisk::chunk_header_common_size) {
        return;
    }

    auto common = read_chunk_header_common();
    if (common.magic == ondisk::chunk_header_magic && !ondisk::can_read_version(common.version)) {
        throw std::runtime_error(fmt::format("Streamed logstor chunk has format version {}, and this release reads versions {} to {}",
                common.version, ondisk::min_read_version, ondisk::current_version));
    }
    if (!ondisk::validate_chunk_header_fields(common)) {
        throw std::runtime_error("Invalid streamed logstor chunk header");
    }
    _initial_header_size = ondisk::chunk_header_size(common.kind);
}

void streamed_segment_rewriter::rewrite_chunk_header() {
    auto common = read_chunk_header_common();
    const auto rest = std::span<const char>(_pending_data.data() + ondisk::chunk_header_common_size, *_initial_header_size - ondisk::chunk_header_common_size);
    auto ch = ondisk::parse_chunk_header(common, rest);
    if (!ch) {
        throw std::runtime_error("Invalid streamed logstor chunk header");
    }
    if (const auto unknown = ondisk::unknown_chunk_flags(*ch)) {
        throw std::runtime_error(fmt::format("Streamed logstor chunk has flags {:#x} that this release does not define", unknown));
    }

    logstor_logger.trace("Rewriting chunk header for segment {} seq {} with seq {}", _target_segment, common.segment_seq, _target_seq);

    // The rewriter changes only segment_seq and the CRC, so that the other bytes of the header keep
    // the values that the sender wrote, also the reserved bytes.
    common.segment_seq = _target_seq;
    common.crc = ondisk::calculate_chunk_header_crc(common, rest);

    simple_memory_output_stream out(_pending_data.data(), ondisk::chunk_header_common_size);
    ser::serialize<ondisk::chunk_header_common>(out, common);
}

future<> streamed_segment_rewriter::flush_pending_data() {
    if (_pending_data.empty()) {
        co_return;
    }
    co_await _on_buffer(bytes_view(reinterpret_cast<const int8_t*>(_pending_data.data()), _pending_data.size()));
    _pending_data.clear();
}

future<> streamed_segment_rewriter::put(std::span<temporary_buffer<char>> data) {
    for (auto& buf : data) {
        if (buf.empty()) {
            continue;
        }

        if (_header_rewritten) {
            co_await _on_buffer(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), buf.size()));
            continue;
        }

        _pending_data.insert(_pending_data.end(), buf.get(), buf.get() + buf.size());
        maybe_parse_initial_header();
        if (_initial_header_size && _pending_data.size() >= *_initial_header_size) {
            rewrite_chunk_header();
            _header_rewritten = true;
            co_await flush_pending_data();
        }
    }
}

future<> streamed_segment_rewriter::close() {
    if (!_header_rewritten && !_pending_data.empty()) {
        throw std::runtime_error("Truncated streamed logstor segment header");
    }
    co_return;
}

size_t streamed_segment_rewriter::buffer_size() const noexcept {
    return 128 * 1024;
}

}
