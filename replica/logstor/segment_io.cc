/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/segment_io.hh"

#include <seastar/core/align.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/util/log.hh>

#include "serializer_impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

namespace {

// The headers at the start of a chunk: the chunk header, and the segment header in a full segment.
struct chunk_headers {
    ondisk::chunk_header chunk;
    std::optional<ondisk::segment_header> segment;
};

// Returns nullopt if the stream ends first or the headers are invalid.
future<std::optional<chunk_headers>> read_chunk_headers(segment_reader& reader) {
    auto ch = co_await reader.read<ondisk::chunk_header>();
    if (!ch || !ondisk::validate_chunk_header_fields(*ch)) {
        co_return std::nullopt;
    }

    std::optional<ondisk::segment_header> sh;
    if (ch->kind == segment_kind::full) {
        sh = co_await reader.read<ondisk::segment_header>();
        if (!sh) {
            co_return std::nullopt;
        }
    }

    if (!ondisk::validate_chunk_headers(*ch, sh)) {
        co_return std::nullopt;
    }
    co_return chunk_headers{*ch, sh};
}

segment_info make_segment_info(const chunk_headers& headers) {
    const auto& ch = headers.chunk;
    const auto& sh = headers.segment;
    segment_info seg_info {
        .kind = ch.kind,
        .segment_seq = ch.segment_seq,
    };

    switch (ch.kind) {
    case segment_kind::full:
        // read_chunk_headers() reads the segment header of every full segment.
        if (!sh) {
            on_internal_error(logstor_logger, "Full segment chunk header without segment header");
        }
        seg_info.v = segment_info::full {
            .table = sh->table,
            .first_token = sh->first_token,
            .last_token = sh->last_token,
        };
        break;
    case segment_kind::mixed:
        seg_info.v = segment_info::mixed{};
        break;
    }
    return seg_info;
}

// The body of a chunk, and whether it matches the body CRC of the chunk header.
struct chunk_body {
    temporary_buffer<char> buf;
    bool valid;
};

// Reads the body of the chunk whose headers the reader read last, and checks it against the body
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
    auto record = ondisk::try_read_record_frame(_records);
    if (!record) {
        return;
    }

    const auto segment_id = _chunk->_segment_id;
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

    // A chunk with invalid headers, or a chunk of an earlier use of the segment, ends the scan.
    // Chunks are written in sequence, so no chunk of this use of the segment follows it.
    auto headers = co_await read_chunk_headers(_reader);
    if (!headers) {
        co_return std::nullopt;
    }
    const auto& ch = headers->chunk;

    if (!_segment_seq) {
        _segment_seq = ch.segment_seq;
    } else if (ch.segment_seq != *_segment_seq) {
        co_return std::nullopt;
    }

    // A chunk whose body is cut short still gives its segment info. Recovery takes the highest
    // segment_seq from it, and a segment_seq that is on disk must not go to a new segment again,
    // even when the body of its chunk is torn.
    auto seg_info = make_segment_info(*headers);
    // A full segment has only one chunk.
    if (seg_info.kind == segment_kind::full) {
        _state = scan_state::last;
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

future<std::optional<segment_info>> read_chunk_info(seastar::input_stream<char>& in) {
    segment_reader reader(in);
    auto headers = co_await read_chunk_headers(reader);
    co_return headers.transform(make_segment_info);
}

streamed_segment_rewriter::streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer)
    : _target_segment(target_segment)
    , _target_seq(target_seq)
    , _on_buffer(std::move(on_buffer)) {
}

ondisk::chunk_header streamed_segment_rewriter::read_chunk_header() const {
    simple_memory_input_stream ch_stream(_pending_data.data(), ondisk::chunk_header_size);
    return ser::deserialize(ch_stream, std::type_identity<ondisk::chunk_header>{});
}

void streamed_segment_rewriter::maybe_parse_initial_header() {
    if (_initial_header_size || _pending_data.size() < ondisk::chunk_header_size) {
        return;
    }

    auto ch = read_chunk_header();
    if (!ondisk::validate_chunk_header_fields(ch)) {
        throw std::runtime_error("Invalid streamed logstor chunk header");
    }

    size_t header_size = ondisk::chunk_header_size;
    if (ch.kind == segment_kind::full) {
        header_size += ondisk::segment_header_size;
    }
    _initial_header_size = header_size;
}

void streamed_segment_rewriter::rewrite_chunk_header() {
    simple_memory_input_stream in(_pending_data.data(), *_initial_header_size);
    auto ch = ser::deserialize(in, std::type_identity<ondisk::chunk_header>{});
    std::optional<ondisk::segment_header> sh;
    if (ch.kind == segment_kind::full) {
        sh = ser::deserialize(in, std::type_identity<ondisk::segment_header>{});
    }
    if (!ondisk::validate_chunk_headers(ch, sh)) {
        throw std::runtime_error("Invalid streamed logstor chunk headers");
    }

    logstor_logger.trace("Rewriting chunk header for segment {} seq {} with seq {}", _target_segment, ch.segment_seq, _target_seq);

    ch.segment_seq = _target_seq;
    ch.crc = ch.calculate_crc(sh);

    simple_memory_output_stream ch_stream(_pending_data.data(), ondisk::chunk_header_size);
    ser::serialize<ondisk::chunk_header>(ch_stream, ch);
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
