/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>

#include "commitlog_entry.hh"

// Pure helpers for the per-fragment payload math used by
// segment_manager::oversized_allocation. Extracted as free functions with
// no I/O or segment_manager dependencies. The arithmetic is then unit-
// testable in isolation, and the production loop calls a single helper per
// branch instead of inlining the formula.

namespace db::commitlog_oversized_alloc {

/**
 * Maximum number of payload bytes (user mutation bytes, excluding any
 * CRC, sector, entry, segment or descriptor framing) of an oversized
 * fragment that can be appended to the *already-allocated* buffer of a
 * segment.
 *
 * @param alignment              the segment's on-disk alignment in bytes.
 * @param buffer_data_remaining  bytes free in the current buffer's data
 *                               ostream (excludes the per-buffer header).
 * @param segment_position       the segment's current logical write
 *                               position (file_pos + buffer position).
 * @param max_segment_size       upper bound on segment_position the
 *                               segment is allowed to reach.
 * @param max_mutation_size      upper bound on a single fragment's payload.
 * @return  the recommended payload size in bytes, or 0 when the buffer
 *          cannot host another fragment.
 */
inline size_t in_buffer_fragment_payload(
        size_t alignment,
        size_t buffer_data_remaining,
        size_t segment_position,
        size_t max_segment_size,
        size_t max_mutation_size) {
    if (segment_position >= max_segment_size) {
        return 0;
    }
    const size_t sector_size = alignment - detail::sector_overhead_size;
    const size_t buf_rem = std::min(max_segment_size - segment_position, buffer_data_remaining);
    if (buf_rem <= alignment) {
        // caller falls through to post_cycle_fragment_payload().
        return 0;
    }
    const size_t rem2 = buf_rem - (1 + buf_rem / sector_size) * detail::sector_overhead_size;
    const size_t entry_ovh = detail::entry_overhead_size + detail::fragmented_entry_overhead_size;
    const size_t cap = std::min(rem2, max_mutation_size);
    const size_t result = cap > entry_ovh ? cap - entry_ovh : 0;
    assert(result < buf_rem);
    return result;
}

/**
 * Maximum payload bytes for a fragment that will land in the *next*
 * buffer of a segment (after a cycle, or in a freshly-opened segment).
 * Accounts for the per-buffer chunk header and, when file_pos is zero,
 * the per-file descriptor header.
 *
 * @param alignment          the segment's on-disk alignment in bytes.
 * @param file_pos           the segment's current on-disk file position.
 * @param max_segment_size   upper bound on file_pos the segment is
 *                           allowed to reach.
 * @param max_mutation_size  upper bound on a single fragment's payload.
 * @return  the recommended payload size in bytes, or 0 when the segment
 *          cannot host another fragment header.
 */
inline size_t post_cycle_fragment_payload(
        size_t alignment,
        size_t file_pos,
        size_t max_segment_size,
        size_t max_mutation_size) {
    if (file_pos >= max_segment_size) {
        return 0;
    }
    const size_t sector_size = alignment - detail::sector_overhead_size;
    const size_t file_rem = max_segment_size - file_pos;
    if (file_rem < alignment) {
        return 0;
    }
    const size_t rem2 = file_rem - (1 + file_rem / sector_size) * detail::sector_overhead_size;
    const size_t first_buffer_ovh = (file_pos == 0 ? detail::descriptor_header_size : 0) + detail::segment_overhead_size;
    const size_t entry_ovh = detail::entry_overhead_size + detail::fragmented_entry_overhead_size;
    const size_t fixed_ovh = first_buffer_ovh + entry_ovh;
    const size_t cap = std::min(rem2, max_mutation_size);
    const size_t result = cap > fixed_ovh ? cap - fixed_ovh : 0;
    assert(result < file_rem);
    return result;
}

/**
 * Inverse of post_cycle_fragment_payload: given a recommended payload,
 * returns the on-disk position the segment would advance to after the
 * write. Mirrors segment::next_position semantics for the case where
 * the buffer is empty / about to be cycled.
 *
 * Used by tests to assert that a recommended payload, padded with all
 * overheads, never crosses max_segment_size.
 */
inline size_t projected_post_cycle_position(
        size_t alignment,
        size_t file_pos,
        size_t payload_bytes) {
    const size_t fixed_ovh =
        (file_pos == 0 ? detail::descriptor_header_size : 0)
        + detail::segment_overhead_size
        + detail::entry_overhead_size
        + detail::fragmented_entry_overhead_size;
    const size_t used = payload_bytes + fixed_ovh;
    const size_t sector_size = alignment - detail::sector_overhead_size;
    const size_t sector_ovh = (used / sector_size) * detail::sector_overhead_size;
    return file_pos + used + sector_ovh;
}

} // namespace db::commitlog_oversized_alloc
