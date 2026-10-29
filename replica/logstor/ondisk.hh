/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <limits>
#include <optional>
#include <span>

#include "dht/token.hh"
#include "dht/decorated_key.hh"
#include "keys/keys.hh"
#include "replica/logstor/fixed_serializer.hh"
#include "replica/logstor/types.hh"
#include "serializer.hh"
#include "utils/managed_bytes.hh"

namespace replica::logstor::ondisk {

static constexpr size_t block_alignment = 4096;
static constexpr size_t record_alignment = 8;
static constexpr uint8_t current_version = 1;
static constexpr uint32_t chunk_header_magic = 0x4c475342;

// The on-disk header types below all have a fixed serialized size. fixed_serializer writes the
// fields of each type one after another in declaration order, so the order of the members is the
// on-disk layout, and the serialized size is the sum of the field sizes, known at compile time.
// field<> gives the encoding of each field type. test_logstor_ondisk_layout pins the bytes of each
// type, so that a changed member order or field type fails the test instead of silently changing
// the format. When adding a field or a type, update the expected bytes there.
//
// The record header is the exception: it carries the partition key, so its size varies per
// record. Its encoding is the pair of write_record_header()/read_record_header() at the
// bottom of this file, and record_frame_header carries the key size that the reader needs. The
// record value that follows the header is variable too, and record_frame_header carries its
// size; its encoding lives in replica/logstor/record_value.cc.

struct segment_header;

// A segment holds one or more chunks, each starting at a block_alignment boundary and padded to
// a multiple of it. A chunk is this header, the segment_header in a full segment, and the record
// frames, which are the chunk's body. Chunks are appended to a mixed segment one at a time; a full
// segment is a single chunk. A write buffer builds a chunk in memory and seals it - pads it and
// fills in its header - before it is written.
struct chunk_header : fixed_layout {
    uint32_t magic;
    segment_kind kind;
    uint8_t version;
    uint16_t reserved;
    segment_sequence segment_seq;
    uint32_t records_size; // size of the record frames following the header(s), padding included
    // CRC32 of the chunk's body: the record frames, records_size bytes after the headers. It is what
    // tells a chunk whose header is durable but whose body was only partly written - a crash mid-write
    // over a reused segment - from a whole one.
    uint32_t body_crc;
    uint32_t reserved2;
    uint32_t crc; // of the fields above, and of the segment header in a full segment

    uint32_t calculate_crc(const std::optional<segment_header>& sh) const;

    bool operator==(const chunk_header& other) const noexcept = default;
};

struct segment_header : fixed_layout {
    table_id table;
    dht::token first_token;
    dht::token last_token;

    bool operator==(const segment_header& other) const noexcept = default;
};

// A record is stored on disk as a record frame: this header, the record header it sizes,
// and the record value, followed by padding to record_alignment. The frame header is what a
// record_location points at, and it is the only fixed-size part of a frame.
struct record_frame_header : fixed_layout {
    uint32_t key_size;   // size of the partition key inside the record_header that follows
    uint32_t value_size; // size of the record value that follows the record_header

    bool operator==(const record_frame_header& other) const noexcept = default;
};

// The largest partition key a record can carry. CQL caps the serialized key at this size (see
// validation::max_key_size), so a larger key_size can only come from corruption.
static constexpr size_t max_key_size = std::numeric_limits<uint16_t>::max();

// Whether the header has the magic, a kind and the version this code writes. The kind tells the
// reader whether a segment header follows. The CRC is not checked here: validate_chunk_headers()
// checks it, as it needs the segment header too.
bool validate_chunk_header_fields(const chunk_header& ch);

// Whether the headers at the start of a chunk are the ones this code wrote: the chunk header fields
// are valid, a segment header is there exactly in a full segment, and the CRC matches.
bool validate_chunk_headers(const chunk_header& ch, const std::optional<segment_header>& sh);

// The CRC a chunk header carries in body_crc, over the chunk's body.
uint32_t calculate_body_crc(std::span<const char> body);

// Whether the body of the chunk - its record frames - is the one the header was written with.
inline bool validate_chunk_body(const chunk_header& ch, std::span<const char> body) {
    return calculate_body_crc(body) == ch.body_crc;
}

inline bool validate_record_frame_header(const record_frame_header& frame_header) noexcept {
    // A record always carries an encoded value, so a zero value_size cannot come from a record
    // this code wrote. It is what a scan sees in the zero-filled tail of a torn
    // buffer, and rejecting it stops the scan there instead of walking the tail as a run of
    // zero-length records. The key bound rejects a corrupt header before its key_size is
    // trusted to size a read or an allocation.
    return frame_header.value_size != 0 && frame_header.key_size <= max_key_size;
}

// Out of line so that the formatting and the throw stay out of the inlined read path.
[[noreturn]] void throw_invalid_record_frame(const record_frame_header& frame_header, size_t frame_size);

template <>
struct field<segment_kind> : enum_field<segment_kind, uint8_t> {};

template <>
struct field<segment_sequence> : projected_field<uint64_t, &segment_sequence::value> {};

// The raw value only, not the IDL form of dht::token, which also carries the token kind.
template <>
struct field<dht::token> : projected_field<int64_t, &dht::token::raw, &dht::token::from_int64> {};

template <>
struct field<utils::UUID> {
    static constexpr size_t size = 2 * field<int64_t>::size;

    template <typename Output>
    [[gnu::always_inline]]
    static void write(Output& out, const utils::UUID& v) {
        field<int64_t>::write(out, v.get_most_significant_bits());
        field<int64_t>::write(out, v.get_least_significant_bits());
    }

    template <typename Input>
    [[gnu::always_inline]]
    static utils::UUID read(Input& in) {
        auto msb = field<int64_t>::read(in);
        auto lsb = field<int64_t>::read(in);
        return utils::UUID(msb, lsb);
    }
};

template <>
struct field<table_id> : projected_field<utils::UUID, &table_id::uuid> {};

// The on-disk sizes, derived from the fields of each type.
static constexpr size_t chunk_header_size = fixed_size<chunk_header>;
static constexpr size_t segment_header_size = fixed_size<segment_header>;
static constexpr size_t record_frame_header_size = fixed_size<record_frame_header>;

static_assert(chunk_header_size % record_alignment == 0, "Chunk header size must be aligned by record_alignment");
static_assert(segment_header_size % record_alignment == 0, "Segment header size must be aligned by record_alignment");

// The record header is the token, the timestamp and the table id as four little-endian 64-bit
// words, followed by the partition key in its internal representation:
//
//   0         8             16                    32                  32 + key_size
//   +---------+-------------+---------------------+-------------------+
//   |  token  |  timestamp  |  table id msb, lsb  |   partition key   |
//   +---------+-------------+---------------------+-------------------+
//
// The fixed fields come first so that they are at constant offsets; the key is the only variable
// part, and its size is stored in the record_frame_header ahead of the record header.
static constexpr size_t record_header_fixed_size =
    sizeof(int64_t)                 // token
    + sizeof(api::timestamp_type)   // timestamp
    + 2 * sizeof(int64_t);          // table id

constexpr size_t record_header_size(size_t key_size) noexcept {
    return record_header_fixed_size + key_size;
}

inline size_t record_header_size(const record_header& h) noexcept {
    return record_header_size(h.key.key().representation().size());
}

template <typename Output>
void write_record_header(Output& out, const record_header& h) {
    ser::serializer<int64_t>::write(out, h.key.token().raw());
    ser::serializer<api::timestamp_type>::write(out, h.timestamp);
    ser::serializer<int64_t>::write(out, h.table.uuid().get_most_significant_bits());
    ser::serializer<int64_t>::write(out, h.table.uuid().get_least_significant_bits());
    for (bytes_view frag : fragment_range(managed_bytes_view(h.key.key().representation()))) {
        out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
}

// key_size comes from the record_frame_header that precedes the record header on disk.
template <typename Input>
record_header read_record_header(Input& in, uint32_t key_size) {
    auto token = dht::token::from_int64(ser::serializer<int64_t>::read(in));
    auto timestamp = ser::serializer<api::timestamp_type>::read(in);
    auto msb = ser::serializer<int64_t>::read(in);
    auto lsb = ser::serializer<int64_t>::read(in);
    managed_bytes key(managed_bytes::initialized_later(), key_size);
    for (bytes_mutable_view frag : fragment_range(managed_bytes_mutable_view(key))) {
        in.read(reinterpret_cast<char*>(frag.data()), frag.size());
    }
    return record_header{
        .key = dht::decorated_key(token, partition_key::from_bytes(std::move(key))),
        .timestamp = timestamp,
        .table = table_id(utils::UUID(msb, lsb)),
    };
}

// A record frame, as write_record_frame() writes it and read_record_frame() and
// try_read_record_frame() read it:
//
//   +-----------------------+-------------------+------------------+
//   |  record_frame_header  |   record_header   |   record value   |
//   |  key_size value_size  |                   |                  |
//   +-----------------------+-------------------+------------------+
//   |<------ 8 bytes ------>|<-- header_size -->|<-- value_size -->|
//   |<------------------------ frame_size ------------------------>|
//
// A record_location points at the start of the frame. The write buffer pads each frame to
// record_alignment after its frame_size bytes.

// The bytes a record frame takes, its padding excluded - what record_location::size holds.
constexpr size_t record_frame_size(size_t header_size, size_t value_size) noexcept {
    return record_frame_header_size + header_size + value_size;
}

inline size_t record_frame_size(const log_record& r) noexcept {
    return record_frame_size(record_header_size(r.header), r.value.size());
}

template <typename Output>
void write_record_frame(Output& out, const record_header& header, const record_value& value) {
    const size_t header_size = record_header_size(header);
    ser::serializer<record_frame_header>::write(out, record_frame_header {
        .key_size = static_cast<uint32_t>(header_size - record_header_fixed_size),
        .value_size = static_cast<uint32_t>(value.size()),
    });
    write_record_header(out, header);
    for (bytes_view frag : value.representation().fragments()) {
        out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
}

// The same frame for a record whose header and value are already serialized.
template <typename Output>
void write_record_frame(Output& out, log_record_bytes_view record_view) {
    ser::serializer<record_frame_header>::write(out, record_frame_header {
        .key_size = static_cast<uint32_t>(record_view.header.size() - record_header_fixed_size),
        .value_size = static_cast<uint32_t>(record_view.value.size()),
    });
    out.write(reinterpret_cast<const char*>(record_view.header.data()), record_view.header.size());
    out.write(reinterpret_cast<const char*>(record_view.value.data()), record_view.value.size());
}

template <typename Input>
log_record read_record_frame(Input& in) {
    auto frame_header = ser::serializer<record_frame_header>::read(in);
    const size_t header_size = record_header_size(frame_header.key_size);

    if (!validate_record_frame_header(frame_header) || header_size + frame_header.value_size != in.size()) [[unlikely]] {
        throw_invalid_record_frame(frame_header, record_frame_header_size + in.size());
    }

    auto header_stream = in.read_substream(header_size);
    auto value_stream = in.read_substream(frame_header.value_size);

    return log_record {
        .header = read_record_header(header_stream, frame_header.key_size),
        .value = record_value(bytes_view(reinterpret_cast<const int8_t*>(value_stream.begin()), value_stream.size())),
    };
}

// A record that a frame holds: the record header, and the views of the serialized header and value
// in the buffer of the frame.
struct parsed_record_frame {
    record_header header;
    log_record_bytes_view bytes;
};

// Reads the record frame at the start of `in`, without its padding. `in` can hold more after the
// frame, such as the next frames of a chunk body. Returns nullopt if the frame header is invalid or
// the frame does not fit in what is left of `in`.
template <typename Input>
std::optional<parsed_record_frame> try_read_record_frame(Input& in) {
    // The sizes a frame claims are checked against what is left of `in` before they are trusted,
    // as the body CRC only says the bytes are the ones written, not that the writer was right.
    if (in.size() < record_frame_header_size) {
        return std::nullopt;
    }
    auto frame_header = ser::serializer<record_frame_header>::read(in);
    const size_t header_size = record_header_size(frame_header.key_size);

    if (!validate_record_frame_header(frame_header) || header_size + frame_header.value_size > in.size()) {
        return std::nullopt;
    }

    auto header_stream = in.read_substream(header_size);
    auto value_stream = in.read_substream(frame_header.value_size);
    // Taken before read_record_header() consumes the stream.
    const auto header_bytes = bytes_view(reinterpret_cast<const int8_t*>(header_stream.begin()), header_size);

    return parsed_record_frame{
        .header = read_record_header(header_stream, frame_header.key_size),
        .bytes = {
            .header = header_bytes,
            .value = bytes_view(reinterpret_cast<const int8_t*>(value_stream.begin()), value_stream.size()),
        },
    };
}

} // namespace replica::logstor::ondisk
