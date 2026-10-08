/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/ondisk.hh"

#include <stdexcept>
#include <fmt/format.h>

#include "utils/crc.hh"

namespace replica::logstor::ondisk {

uint32_t chunk_header::calculate_crc() const {
    utils::crc32 c;
    c.process_le(magic);
    c.process_le(static_cast<uint8_t>(kind));
    c.process_le(version);
    c.process_le(reserved);
    c.process_le(segment_seq.value);
    c.process_le(records_size);
    return c.get();
}

bool validate_chunk_header(const chunk_header& ch) {
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

    if (ch.version != current_version) {
        return false;
    }

    return ch.calculate_crc() == ch.crc;
}

void throw_invalid_record_frame(const record_frame_header& frame_header, size_t frame_size) {
    throw std::runtime_error(fmt::format("Invalid record frame: key_size {} and value_size {} in a frame of {} bytes",
            frame_header.key_size, frame_header.value_size, frame_size));
}

} // namespace replica::logstor::ondisk
