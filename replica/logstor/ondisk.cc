/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/ondisk.hh"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <fmt/format.h>
#include <seastar/core/byteorder.hh>

#include "utils/crc.hh"

namespace replica::logstor::ondisk {

namespace {

// crc_output gives the bytes that a serializer writes to a CRC, and if a limit is set, it gives
// only the first `limit` bytes. A serializer writes a field in one write() call, so a field of 1, 2,
// 4 or 8 bytes becomes one CRC instruction.
class crc_output {
    utils::crc32& _crc;
    size_t _left;
public:
    explicit crc_output(utils::crc32& crc, size_t limit = std::numeric_limits<size_t>::max()) noexcept
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

uint32_t chunk_header::calculate_crc(const std::optional<segment_header>& sh) const {
    utils::crc32 c;
    static_assert(offsetof(chunk_header, crc) + sizeof(crc) == sizeof(chunk_header),
            "crc must be the last field of chunk_header, because the CRC covers the bytes before it");
    crc_output ch_out(c, chunk_header_size - sizeof(crc));
    ser::serializer<chunk_header>::write(ch_out, *this);
    if (sh) {
        crc_output sh_out(c);
        ser::serializer<segment_header>::write(sh_out, *sh);
    }
    return c.get();
}

bool validate_chunk_header_fields(const chunk_header& ch) {
    if (ch.magic != chunk_header_magic) {
        return false;
    }

    switch (ch.kind) {
    case segment_kind::mixed:
    case segment_kind::full:
        break;
    default:
        return false;
    }

    return ch.version == current_version;
}

bool validate_chunk_headers(const chunk_header& ch, const std::optional<segment_header>& sh) {
    if (!validate_chunk_header_fields(ch) || sh.has_value() != (ch.kind == segment_kind::full)) {
        return false;
    }
    return ch.calculate_crc(sh) == ch.crc;
}

uint32_t calculate_body_crc(std::span<const char> body) {
    utils::crc32 c;
    c.process(reinterpret_cast<const uint8_t*>(body.data()), body.size());
    return c.get();
}

void throw_invalid_record_frame(const record_frame_header& frame_header, size_t frame_size) {
    throw std::runtime_error(fmt::format("Invalid record frame: key_size {} and value_size {} in a frame of {} bytes",
            frame_header.key_size, frame_header.value_size, frame_size));
}

} // namespace replica::logstor::ondisk
