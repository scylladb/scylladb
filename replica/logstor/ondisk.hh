/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <limits>
#include <optional>
#include <span>

#include <seastar/core/simple-stream.hh>

#include "dht/token.hh"
#include "dht/decorated_key.hh"
#include "keys/keys.hh"
#include "replica/logstor/fixed_serializer.hh"
#include "replica/logstor/types.hh"
#include "serializer.hh"
#include "utils/managed_bytes.hh"

namespace replica::logstor::ondisk {

// This file defines the logstor on-disk format. It has these parts, in this order:
//
//   - constants of the format;
//   - the field<> encodings of the field types that the format uses;
//   - the fixed-size types, each derived from fixed_layout, and their sizes;
//   - the checks of the fixed-size types;
//   - the variable-size parts: the record header and the record frame.
//
// fixed_serializer writes the fields of a fixed-size type one after another in declaration order,
// so the order of the members is the on-disk layout, and the serialized size is the sum of the
// field sizes, which the compiler calculates.
//
// test_logstor_ondisk_layout in test/boost/logstor_test.cc checks the exact bytes of each type, so
// that a changed member order or field type fails the test and does not silently change the
// format. When you add a field or a type, update the expected bytes there.
//
// The record header does not have a fixed size, because it holds the partition key.
// write_record_header() writes record_header_fixed and then the key, and read_record_header()
// reads them. record_frame_header holds the key size, so that the reader can find the end of the
// record header.
//
// The record value that follows the header also has a variable size, record_frame_header holds
// that size too, and replica/logstor/record_value.cc defines the encoding of the value.

static constexpr size_t block_alignment = 4096;
static constexpr size_t record_alignment = 8;
// current_version is the format version that this code writes.
static constexpr uint8_t current_version = 1;
// min_read_version is the oldest format version that this code reads, and this code reads every
// version from min_read_version to current_version. To remove the read path of an old version,
// increase min_read_version.
static constexpr uint8_t min_read_version = 1;
// known_chunk_flags holds the chunk header flags that this code defines.
static constexpr uint16_t known_chunk_flags = 0;
// known_record_flags holds the record frame header flags that this code defines.
static constexpr uint16_t known_record_flags = 0;
static constexpr uint32_t chunk_header_magic = 0x4c475342;

// max_key_size is the size of the largest partition key that a record can hold. CQL limits the
// serialized key to this size (see validation::max_key_size), and the record frame header holds the
// key size in 16 bits.
static constexpr size_t max_key_size = std::numeric_limits<uint16_t>::max();

// segment_kind is the kind of the segment that holds a chunk, as the chunk header records it.
enum class segment_kind : uint8_t {
    mixed = 1,
    full = 2,
};

template <>
struct field<segment_kind> : enum_field<segment_kind, uint8_t> {};

template <>
struct field<segment_sequence> : projected_field<uint64_t, &segment_sequence::value> {};

// A dht::token is written as its raw value only, and not in its IDL form, which also holds the
// token kind.
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

// api::timestamp_type is an alias, so it cannot have its own field<>, and fixed_serializer encodes
// it as int64_t.
static_assert(std::is_same_v<api::timestamp_type, int64_t>, "The timestamp must be of the on-disk type");

struct segment_header;

// A segment holds one or more chunks, each starting at a block_alignment boundary and padded to
// a multiple of it. A chunk is this header, the segment_header in a full segment, and the record
// frames, which are the chunk's body. Chunks are appended to a mixed segment one at a time; a full
// segment is a single chunk. A write buffer builds a chunk in memory and seals it - pads it and
// fills in its header - before it is written.
//
// The chunk header, 16 bytes a row, little-endian:
//
//       0                   4    5    6         8                                       16
//       +-------------------+----+----+---------+---------------------------------------+
//    0  |       magic       |ver |kind|  flags  |              segment_seq              |
//       +-------------------+----+----+---------+---------------------------------------+
//   16  |    records_size   |      body_crc     |                reserved               |
//       +-------------------+-------------------+---------------------------------------+
//   32  |                                    reserved                                   |
//       +-----------------------------------------------------------+-------------------+
//   48  |                          reserved                         |        crc        |
//       +-----------------------------------------------------------+-------------------+
//
// The first row holds magic, version, kind, flags and segment_seq. The first row keeps its position
// in every format version, so that a reader can find the version and the end of a segment in any
// layout of the other bytes. `crc` covers bytes 0 to 59, and the segment header in a full segment.
struct chunk_header : fixed_layout {
    uint32_t magic;
    uint8_t version;
    segment_kind kind;
    // The flags give properties that a chunk can have in a format version. No flag is defined yet.
    uint16_t flags;
    segment_sequence segment_seq;
    uint32_t records_size; // size of the record frames following the header(s), padding included
    // CRC32 of the chunk's body: the record frames, records_size bytes after the headers. It is what
    // tells a chunk whose header is durable but whose body was only partly written - a crash mid-write
    // over a reused segment - from a whole one.
    uint32_t body_crc;
    // The writer sets these bytes to zero. A new field uses part of these bytes, so that the header
    // keeps its size.
    std::array<char, 36> reserved;
    // crc is the CRC32 of the fields above it and, in a full segment, of the segment header. crc
    // must stay the last field, because calculate_crc() covers the serialized bytes before crc.
    uint32_t crc;

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
//
// The record frame header, little-endian:
//
//   0          2         4                     8
//   +----------+---------+---------------------+
//   | key_size |  flags  |     value_size      |
//   +----------+---------+---------------------+
struct record_frame_header : fixed_layout {
    // key_size is the size of the partition key in the record_header that follows. 16 bits are
    // enough for max_key_size.
    uint16_t key_size;
    // flags gives properties of the record. No flag is defined yet. When write_record_frame()
    // copies a record as bytes, it writes the frame header again from the sizes, so it drops the
    // flags. Before a flag is defined, the byte-copy path must also copy the flags.
    uint16_t flags;
    uint32_t value_size; // size of the record value that follows the record_header

    bool operator==(const record_frame_header& other) const noexcept = default;
};

static_assert(max_key_size == std::numeric_limits<decltype(record_frame_header::key_size)>::max(),
        "record_frame_header::key_size must hold every key size up to max_key_size");

// A record header starts with the token, the timestamp and the table id, as four little-endian
// 64-bit words. The partition key follows them, in key_size bytes of its internal representation:
//
//   0         8             16                    32                  32 + key_size
//   +---------+-------------+---------------------+-------------------+
//   |  token  |  timestamp  |      table id       |   partition key   |
//   +---------+-------------+---------------------+-------------------+
//
// The fixed fields come first so that they are at constant offsets. The key is the only variable
// part, and the record_frame_header before the record header holds the key size.
struct record_header_fixed : fixed_layout {
    dht::token token;
    api::timestamp_type timestamp;
    table_id table;
};

// These constants are the on-disk sizes, which fixed_size calculates from the fields of each type.
static constexpr size_t chunk_header_size = fixed_size<chunk_header>;
static constexpr size_t segment_header_size = fixed_size<segment_header>;
static constexpr size_t record_frame_header_size = fixed_size<record_frame_header>;
static constexpr size_t record_header_fixed_size = fixed_size<record_header_fixed>;
// min_record_frame_size is the size of the smallest record frame, which holds a
// record_frame_header and a record header with an empty key.
static constexpr size_t min_record_frame_size = record_frame_header_size + record_header_fixed_size;

static_assert(chunk_header_size % record_alignment == 0, "Chunk header size must be aligned by record_alignment");
static_assert(segment_header_size % record_alignment == 0, "Segment header size must be aligned by record_alignment");

// Returns true if this code reads chunks of format version `v`.
constexpr bool can_read_version(uint8_t v) noexcept {
    return v >= min_read_version && v <= current_version;
}

// Returns true if the header has the magic, a valid kind and a version that this code reads. The
// kind shows if a segment header follows. This function does not check the CRC, because the CRC
// also covers the segment header, and validate_chunk_headers() checks the CRC.
bool validate_chunk_header_fields(const chunk_header& ch);

// Whether the headers at the start of a chunk are the ones this code wrote: the chunk header fields
// are valid, a segment header is there exactly in a full segment, and the CRC matches.
bool validate_chunk_headers(const chunk_header& ch, const std::optional<segment_header>& sh);

// Returns the flags of `ch` that are not in known_chunk_flags. A chunk with such a flag is from a
// release that this code cannot read, and it does not mark the end of the segment, so recovery must
// fail and not handle the segment as empty.
inline uint16_t unknown_chunk_flags(const chunk_header& ch) noexcept {
    return ch.flags & ~known_chunk_flags;
}

// The CRC a chunk header carries in body_crc, over the chunk's body.
uint32_t calculate_body_crc(std::span<const char> body);

// Whether the body of the chunk - its record frames - is the one the header was written with.
inline bool validate_chunk_body(const chunk_header& ch, std::span<const char> body) {
    return calculate_body_crc(body) == ch.body_crc;
}

inline bool validate_record_frame_header(const record_frame_header& frame_header) noexcept {
    // A record always carries an encoded value, so a zero value_size cannot come from a record
    // this code wrote. A scan finds a zero value_size in the zero-filled end of a torn chunk, and
    // this check stops the scan there, so that the scan does not read the end as a run of
    // zero-length records.
    return frame_header.value_size != 0;
}

// Returns the flags of `frame_header` that are not in known_record_flags. A record with such a flag
// is from a release that this code cannot read, so a read or a scan must fail. A scan must not stop
// at that record as at the end of the chunk's frames, because the scan would then lose the records
// after that record.
inline uint16_t unknown_record_flags(const record_frame_header& frame_header) noexcept {
    return frame_header.flags & ~known_record_flags;
}

// These functions are out of line, so that the formatting and the throw are not in the inlined read
// path.
[[noreturn]] void throw_invalid_record_frame(std::span<const char> frame);
[[noreturn]] void throw_unknown_record_flags(uint16_t unknown);

constexpr size_t record_header_size(size_t key_size) noexcept {
    return record_header_fixed_size + key_size;
}

inline size_t record_header_size(const record_header& h) noexcept {
    return record_header_size(h.key.key().representation().size());
}

constexpr size_t record_header_size(const record_frame_header& frame_header) noexcept {
    return record_header_size(frame_header.key_size);
}

template <typename Output>
void write_record_header(Output& out, const record_header& h) {
    ser::serializer<record_header_fixed>::write(out, record_header_fixed {
        .token = h.key.token(),
        .timestamp = h.timestamp,
        .table = h.table,
    });
    for (bytes_view frag : fragment_range(managed_bytes_view(h.key.key().representation()))) {
        out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
}

// Reads the record header that fills all of `header`, which holds the fixed part and then the
// partition key. Throws std::out_of_range if `header` is shorter than the fixed part.
inline record_header read_record_header(bytes_view header) {
    seastar::simple_memory_input_stream in(reinterpret_cast<const char*>(header.data()), header.size());
    auto fixed = ser::serializer<record_header_fixed>::read(in);
    return record_header{
        .key = dht::decorated_key(fixed.token, partition_key::from_bytes(header.substr(record_header_fixed_size))),
        .timestamp = fixed.timestamp,
        .table = fixed.table,
    };
}

// A record frame, as write_record_frame() writes it and parse_record_frame() reads it:
//
//   0              8                    8 + header_size      frame_size
//   +--------------+--------------------+--------------------+.............+
//   | frame header |   record header    |    record value    |   padding   :
//   +--------------+--------------------+--------------------+.............+
//   |<- 8 bytes -->|<-- header_size --->|<--- value_size --->|
//   |<--------------------- frame_size --------------------->|
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
        .key_size = static_cast<uint16_t>(header_size - record_header_fixed_size),
        .flags = 0,
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
    static_assert(known_record_flags == 0, "A copied record must carry its flags before a record flag is defined");
    ser::serializer<record_frame_header>::write(out, record_frame_header {
        .key_size = static_cast<uint16_t>(record_view.header.size() - record_header_fixed_size),
        .flags = 0,
        .value_size = static_cast<uint32_t>(record_view.value.size()),
    });
    out.write(reinterpret_cast<const char*>(record_view.header.data()), record_view.header.size());
    out.write(reinterpret_cast<const char*>(record_view.value.data()), record_view.value.size());
}

// Reads the record frame at the start of `in`, without its padding, and returns the bytes of its
// record header and record value, which point into `in`. Returns nullopt, and leaves `in` as it
// was, if the frame header is invalid or the frame does not fit in `in`.
//
// This function checks the sizes in the frame header before it uses them, because the chunk CRC
// shows only that the bytes did not change after the write, and not that the writer wrote correct
// sizes.
//
// Throws std::runtime_error at a frame with an unknown flag (see unknown_record_flags()). The
// function checks the flags after the sizes, so that bytes that are not a frame do not cause the
// unknown-flag error.
inline std::optional<log_record_bytes_view> parse_record_frame(seastar::simple_memory_input_stream& in) {
    if (in.size() < min_record_frame_size) {
        return std::nullopt;
    }
    auto rest = in;
    auto frame_header = ser::serializer<record_frame_header>::read(rest);
    if (!validate_record_frame_header(frame_header)
            || size_t(frame_header.key_size) + frame_header.value_size > in.size() - min_record_frame_size) {
        return std::nullopt;
    }
    if (const auto unknown = unknown_record_flags(frame_header)) [[unlikely]] {
        throw_unknown_record_flags(unknown);
    }
    auto header = rest.read_substream(record_header_size(frame_header));
    auto value = rest.read_substream(frame_header.value_size);
    in = rest;
    return log_record_bytes_view{
        .header = bytes_view(reinterpret_cast<const int8_t*>(header.begin()), header.size()),
        .value = bytes_view(reinterpret_cast<const int8_t*>(value.begin()), value.size()),
    };
}

// Reads a record frame that fills all of `in`, as a record_location gives it. Throws, and leaves
// `in` as it was, if the frame is invalid or does not fill all of `in`.
inline log_record read_record_frame(seastar::simple_memory_input_stream& in) {
    auto rest = in;
    auto bytes = parse_record_frame(rest);
    if (!bytes || rest.size() != 0) [[unlikely]] {
        throw_invalid_record_frame(std::span(in.begin(), in.size()));
    }
    in = rest;
    return log_record {
        .header = read_record_header(bytes->header),
        .value = record_value(bytes->value),
    };
}

// A record that a frame holds: the record header, and the views of the serialized header and value
// in the buffer of the frame.
struct parsed_record_frame {
    record_header header;
    log_record_bytes_view bytes;
};

// Reads the record frame at the start of `in` as parse_record_frame() does, and also reads the
// record header in the frame. `in` can hold more bytes after the frame, such as the next frames of
// a chunk body.
inline std::optional<parsed_record_frame> try_read_record_frame(seastar::simple_memory_input_stream& in) {
    return parse_record_frame(in).transform([] (const log_record_bytes_view& bytes) {
        return parsed_record_frame{
            .header = read_record_header(bytes.header),
            .bytes = bytes,
        };
    });
}

} // namespace replica::logstor::ondisk

namespace replica::logstor {

// The code that picks a chunk layout uses the on-disk kind directly.
using ondisk::segment_kind;

}
