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

// The smallest record frame: a record_frame_header and a record_header with an empty key.
constexpr size_t min_frame_size = ondisk::record_frame_header_size + ondisk::record_header_fixed_size;

// Reads a segment through its input stream, and keeps the position in the segment of what it
// reads next.
class segment_reader {
    seastar::input_stream<char>& _in;
    size_t _position = 0;
public:
    explicit segment_reader(seastar::input_stream<char>& in) noexcept : _in(in) {}

    size_t position() const noexcept {
        return _position;
    }

    future<temporary_buffer<char>> read_exactly(size_t n) {
        _position += n;
        return _in.read_exactly(n);
    }

    // Reads an on-disk T, or returns nullopt if the stream ends first.
    template <typename T>
    future<std::optional<T>> read() {
        constexpr size_t size = ser::serializer<T>::serialized_size;
        return read_exactly(size).then([] (temporary_buffer<char> buf) -> std::optional<T> {
            if (buf.size() < size) {
                return std::nullopt;
            }
            return ser::deserialize_from_buffer(buf, std::type_identity<T>{});
        });
    }

    // Does nothing if the reader is at or past the position already.
    future<> skip_to(size_t position) {
        if (_position >= position) {
            return make_ready_future<>();
        }
        auto n = position - _position;
        _position = position;
        return _in.skip(n);
    }
};

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
// CRC of the header. Returns nullopt if the body goes past the end of the segment or the stream
// ends first.
future<std::optional<chunk_body>> read_chunk_body(segment_reader& reader,
        const ondisk::chunk_header& ch,
        size_t segment_size) {
    if (ch.records_size > segment_size - reader.position()) {
        co_return std::nullopt;
    }
    auto body = co_await reader.read_exactly(ch.records_size);
    if (body.size() < ch.records_size) {
        co_return std::nullopt;
    }
    const bool valid = ondisk::validate_chunk_body(ch, std::span(body.get(), body.size()));
    co_return chunk_body{std::move(body), valid};
}

struct parsed_record {
    record_header header;
    log_record_bytes_view bytes; // views into the chunk body
};

// Reads the record frame at the start of records, without its padding. Returns nullopt if the
// frame header is invalid or the record does not fit in what is left of records.
std::optional<parsed_record> read_record(simple_memory_input_stream& records) {
    // The sizes a record claims are checked against what is left of the chunk's records before
    // they are trusted, as the CRC only says the bytes are the ones written, not that the writer
    // was right.
    const size_t bytes_left = records.size();
    if (bytes_left < min_frame_size) {
        return std::nullopt;
    }
    auto frame_header = ser::deserialize(records, std::type_identity<ondisk::record_frame_header>{});
    if (!ondisk::validate_record_frame_header(frame_header) || size_t(frame_header.key_size) + frame_header.value_size > bytes_left - min_frame_size) {
        return std::nullopt;
    }
    const size_t header_size = ondisk::record_header_size(frame_header.key_size);

    auto header_stream = records.read_substream(header_size);
    auto value_stream = records.read_substream(frame_header.value_size);
    // Taken before read_record_header() consumes the stream.
    const auto header_bytes = bytes_view(reinterpret_cast<const int8_t*>(header_stream.begin()), header_size);
    return parsed_record{
        .header = ondisk::read_record_header(header_stream, frame_header.key_size),
        .bytes = {
            .header = header_bytes,
            .value = bytes_view(reinterpret_cast<const int8_t*>(value_stream.begin()), value_stream.size()),
        },
    };
}

// records_position is where the records start within the segment, which is what a record's
// location is relative to. A record that does not parse ends the scan of the chunk.
future<> scan_chunk_records(simple_memory_input_stream records,
        size_t records_position,
        log_segment_id segment_id,
        const record_header_consumer& on_record_header,
        const log_record_bytes_consumer& on_record) {
    const size_t records_size = records.size();
    while (records.size() > 0) {
        const size_t record_offset = records_position + (records_size - records.size());
        auto record = read_record(records);
        if (!record) {
            break;
        }

        logstor_logger.trace("Found record of size {} bytes in {}",
                            record->bytes.header.size() + record->bytes.value.size(), segment_id);

        record_location loc {
            .segment = segment_id,
            .offset = static_cast<uint32_t>(record_offset),
            .size = static_cast<uint32_t>(ondisk::record_frame_size(record->bytes.header.size(), record->bytes.value.size()))
        };

        if (on_record_header(loc, record->header) == want_data::yes) {
            co_await on_record(loc, record->header, record->bytes);
        }

        // The next record frame starts at a multiple of record_alignment.
        const size_t consumed = records_size - records.size();
        records.skip(std::min(align_up(consumed, ondisk::record_alignment) - consumed, records.size()));
    }
}

} // anonymous namespace

future<std::optional<segment_info>> read_segment_info(seastar::input_stream<char>& in) {
    segment_reader reader(in);
    auto headers = co_await read_chunk_headers(reader);
    if (!headers) {
        co_return std::nullopt;
    }

    co_return make_segment_info(*headers);
}

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        log_record_bytes_consumer on_record) {
    segment_reader reader(in);
    std::optional<segment_sequence> segment_seq;
    // The number of chunks whose body does not match its CRC, and whether the last chunk of the scan
    // is one of them.
    size_t failed_chunks = 0;
    bool last_chunk_failed = false;

    logstor_logger.trace("Reading records from {}", segment_id);

    while (reader.position() < segment_size) {
        // A chunk with invalid headers, or a chunk of an earlier generation of the segment, ends the
        // scan. Chunks are written in sequence, so no chunk of this generation of the segment
        // follows it.
        auto headers = co_await read_chunk_headers(reader);
        if (!headers) {
            break;
        }
        const auto& ch = headers->chunk;

        if (!segment_seq) {
            segment_seq = ch.segment_seq;
        } else if (ch.segment_seq != *segment_seq) {
            break;
        }

        // The segment info comes before the body check. Recovery takes the highest segment_seq from
        // it, and a segment_seq that is on disk must not go to a new segment again, even when the
        // body of its chunk is torn.
        auto seg_info = make_segment_info(*headers);
        co_await on_segment_info(seg_info);

        auto body = co_await read_chunk_body(reader, ch, segment_size);
        if (!body) {
            break;
        }

        // Deliver the records only from a valid body - a torn chunk can hold records of an
        // earlier use of the segment that parse cleanly.
        if (body->valid) {
            last_chunk_failed = false;
            auto records = simple_memory_input_stream(body->buf.get(), body->buf.size());
            co_await scan_chunk_records(records, reader.position() - records.size(), segment_id, on_record_header, on_record);
        } else {
            // The valid header gives the size of the chunk, so the scan continues at the next
            // chunk. After a torn chunk, what follows is not a valid chunk or is a chunk of an
            // earlier generation, which ends the scan.
            last_chunk_failed = true;
            ++failed_chunks;
        }

        if (seg_info.kind == segment_kind::full) {
            // A full segment has only one chunk.
            break;
        }

        // The next chunk starts at the first block boundary after this one.
        co_await reader.skip_to(align_up(reader.position(), ondisk::block_alignment));
    }

    // A segment has one chunk write in flight at a time, and a write is durable when it completes.
    // Thus a crash can tear only the last chunk of a segment. A failed chunk before it is corrupt.
    if (failed_chunks == 1 && last_chunk_failed) {
        logstor_logger.warn("Skipped the last chunk of {}: its body CRC does not match, probably a torn write from a crash.",
                segment_id);
    } else if (failed_chunks > 0) {
        logstor_logger.warn("Skipped {} chunks of {} whose body CRC does not match, including a chunk before the last. "
                "The segment is corrupt; their records are lost.", failed_chunks, segment_id);
    }
}

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        log_record_consumer on_record) {
    co_await scan_segment(in, segment_id, segment_size,
            std::move(on_segment_info), std::move(on_record_header),
            [on_record = std::move(on_record)] (record_location loc, const record_header& header, log_record_bytes_view record_view) {
                return on_record(loc, log_record{header, record_value(record_view.value)});
            });
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
