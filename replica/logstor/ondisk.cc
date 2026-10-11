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

uint32_t chunk_header::calculate_crc(const std::optional<segment_header>& sh) const {
    utils::crc32 c;
    c.process_le(magic);
    c.process_le(static_cast<uint8_t>(kind));
    c.process_le(version);
    c.process_le(reserved);
    c.process_le(segment_seq.value);
    c.process_le(records_size);
    if (sh) {
        c.process_le(sh->table.uuid().get_most_significant_bits());
        c.process_le(sh->table.uuid().get_least_significant_bits());
        c.process_le(sh->first_token.raw());
        c.process_le(sh->last_token.raw());
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

void throw_invalid_record_frame(const record_frame_header& frame_header, size_t frame_size) {
    throw std::runtime_error(fmt::format("Invalid record frame: key_size {} and value_size {} in a frame of {} bytes",
            frame_header.key_size, frame_header.value_size, frame_size));
}

} // namespace replica::logstor::ondisk
