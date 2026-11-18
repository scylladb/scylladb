/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/ondisk.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <stdexcept>
#include <utility>
#include <fmt/format.h>
#include <seastar/core/byteorder.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>

#include "utils/crc.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

}

namespace replica::logstor::ondisk {

namespace {

// crc_output gives the first `limit` bytes that a serializer writes to a CRC. A serializer writes a
// field in one write() call, so a field of 1, 2, 4 or 8 bytes becomes one CRC instruction.
class crc_output {
    utils::crc32& _crc;
    size_t _left;
public:
    crc_output(utils::crc32& crc, size_t limit) noexcept
        : _crc(crc), _left(limit) {}

    void write(const char* p, size_t n) noexcept {
        n = std::min(n, _left);
        _left -= n;
        switch (n) {
        case 0: break;
        case 1: _crc.process_le(seastar::read_le<uint8_t>(p)); break;
        case 2: _crc.process_le(seastar::read_le<uint16_t>(p)); break;
        case 4: _crc.process_le(seastar::read_le<uint32_t>(p)); break;
        case 8: _crc.process_le(seastar::read_le<uint64_t>(p)); break;
        default: _crc.process(reinterpret_cast<const uint8_t*>(p), n);
        }
    }
};

} // anonymous namespace

bool validate_chunk_header_fields(const chunk_header_common& common) {
    if (common.magic != chunk_header_magic) {
        return false;
    }

    // The switch has no default, so that a new kind fails the build here until this code accepts
    // it. A value from disk that segment_kind does not define goes past the switch.
    switch (common.kind) {
    case segment_kind::mixed:
    case segment_kind::full:
        return can_read_version(common.version);
    }
    return false;
}

void on_unknown_segment_kind(segment_kind kind) {
    on_internal_error(logstor_logger, fmt::format("Unknown segment kind {}", std::to_underlying(kind)));
}

uint32_t calculate_chunk_header_crc(const chunk_header_common& common, std::span<const char> rest) {
    utils::crc32 c;
    static_assert(offsetof(chunk_header_common, crc) + sizeof(common.crc) == sizeof(chunk_header_common),
            "crc must be the last field of chunk_header_common, because the CRC covers the bytes before it");
    crc_output common_out(c, chunk_header_common_size - sizeof(common.crc));
    ser::serializer<chunk_header_common>::write(common_out, common);
    c.process(reinterpret_cast<const uint8_t*>(rest.data()), rest.size());
    return c.get();
}

std::optional<chunk_header> parse_chunk_header(const chunk_header_common& common, std::span<const char> rest) {
    if (!validate_chunk_header_fields(common) || rest.size() != chunk_header_size(common.kind) - chunk_header_common_size) {
        return std::nullopt;
    }
    if (calculate_chunk_header_crc(common, rest) != common.crc) {
        return std::nullopt;
    }

    chunk_header ch {
        .segment = { .segment_seq = common.segment_seq },
        .flags = common.flags,
        .records_size = common.records_size,
        .body_crc = common.body_crc,
    };
    switch (common.kind) {
    case segment_kind::mixed:
        break;
    case segment_kind::full: {
        seastar::simple_memory_input_stream in(rest.data(), rest.size());
        auto full = ser::serializer<chunk_header_full_part>::read(in);
        ch.segment.v = segment_info::full {
            .table = full.table,
            .first_token = full.first_token,
            .last_token = full.last_token,
        };
        break;
    }
    }
    return ch;
}

void write_chunk_header(seastar::simple_memory_output_stream& out, const chunk_header& ch) {
    // The full part goes to a local buffer first, because the CRC in the common part covers it.
    std::array<char, chunk_header_full_part_size> rest_buf;
    const auto rest = std::visit(overloaded_functor{
        [] (const segment_info::mixed&) {
            return std::span<const char>();
        },
        [&] (const segment_info::full& full) {
            seastar::simple_memory_output_stream rest_out(rest_buf.data(), rest_buf.size());
            ser::serializer<chunk_header_full_part>::write(rest_out, chunk_header_full_part {
                .table = full.table,
                .first_token = full.first_token,
                .last_token = full.last_token,
                .reserved = {},
            });
            return std::span<const char>(rest_buf);
        },
    }, ch.segment.v);

    chunk_header_common common {
        .magic = chunk_header_magic,
        .version = current_version,
        .kind = ch.kind(),
        .flags = ch.flags,
        .segment_seq = ch.segment.segment_seq,
        .records_size = ch.records_size,
        .body_crc = ch.body_crc,
        .reserved = {},
        .crc = 0,
    };
    common.crc = calculate_chunk_header_crc(common, rest);
    ser::serializer<chunk_header_common>::write(out, common);
    out.write(rest.data(), rest.size());
}

uint32_t calculate_body_crc(std::span<const char> body) {
    utils::crc32 c;
    c.process(reinterpret_cast<const uint8_t*>(body.data()), body.size());
    return c.get();
}

void throw_invalid_record_frame(std::span<const char> frame) {
    if (frame.size() < record_frame_header_size) {
        throw std::runtime_error(fmt::format("Invalid record frame: {} bytes, shorter than its header", frame.size()));
    }
    seastar::simple_memory_input_stream in(frame.data(), frame.size());
    auto frame_header = ser::serializer<record_frame_header>::read(in);
    throw std::runtime_error(fmt::format("Invalid record frame: key_size {} and value_size {} in a frame of {} bytes",
            frame_header.key_size, frame_header.value_size, frame.size()));
}

void throw_unknown_record_flags(uint16_t unknown) {
    throw std::runtime_error(fmt::format("Record frame has unknown flags {:#x}", unknown));
}

} // namespace replica::logstor::ondisk
