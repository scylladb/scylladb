/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/ondisk.hh"

#include "utils/crc.hh"

namespace replica::logstor::ondisk {

uint32_t buffer_header::calculate_crc() const {
    utils::crc32 c;
    c.process_le(magic);
    c.process_le(static_cast<uint8_t>(kind));
    c.process_le(version);
    c.process_le(reserved);
    c.process_le(segment_seq.value);
    c.process_le(records_size);
    return c.get();
}

bool validate_header(const buffer_header& bh) {
    if (bh.magic != buffer_header_magic) {
        return false;
    }

    switch (bh.kind) {
    case segment_kind::mixed:
    case segment_kind::full:
        break;
    default:
        return false;
    }

    if (bh.version != current_version) {
        return false;
    }

    return bh.calculate_crc() == bh.crc;
}

bool validate_record_frame_header(const record_frame_header& frame_header) {
    // A record always carries an encoded value, so a zero value_size cannot come from a record
    // this code wrote. It is what a scan sees in the zero-filled tail of a torn
    // buffer, and rejecting it stops the scan there instead of walking the tail as a run of
    // zero-length records. The key bound rejects a corrupt header before its key_size is
    // trusted to size a read or an allocation.
    return frame_header.value_size != 0 && frame_header.key_size <= max_key_size;
}

} // namespace replica::logstor::ondisk
