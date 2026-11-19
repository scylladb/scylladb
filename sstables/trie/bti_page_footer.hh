/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The page footer of BTI index files (Partitions.db and Rows.db) in `mu`.
//
// Every page of these files ends with a footer:
//
//     [ trie nodes or raw bytes ][ zero padding ][ table: slot k-1 ... slot 0 ][ count: uint16 BE ][ crc32: uint32 BE ]
//
// The table holds the Data.db chunks referenced by the payloads in the page.
// `count` is the number of slots. The CRC32 covers all bytes of the page
// before it.
//
// The usable area of a page ends where the table starts. No trie node and no
// raw byte lies in the table or in the footer.
//
// See docs/architecture/sstable/sstable3/sstable-mu-format.rst.

#include <cstdint>
#include <span>
#include <string_view>
#include <fmt/format.h>
#include <seastar/core/byteorder.hh>
#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/version.hh"
#include "common.hh"

namespace sstables::trie {

// The size of the fixed part of the footer: the slot count and the CRC32.
inline constexpr size_t bti_page_footer_size = 6;
// The size of a chunk table entry.
inline constexpr size_t bti_chunk_table_entry_size = 5;

// Whether the BTI index files of this format have page footers.
// This depends on the format version, not on the position encoding:
// an uncompressed `mu` sstable uses logical positions, but has footers.
inline bool bti_index_has_page_footers(sstable_version_types v) {
    return v >= sstable_version_types::mu;
}

// A view of one page of a BTI index file with page footers.
// The constructor checks that the slot count fits in the page, but not the CRC.
// Use verify_bti_page() for that.
class bti_page_view {
    const_bytes _page;
    uint16_t _count;
public:
    // `page` is the whole page, `page_pos` its position in the file (for error messages).
    bti_page_view(const_bytes page, uint64_t page_pos) : _page(page) {
        if (_page.size() < bti_page_footer_size) {
            throw_malformed_sstable_exception(fmt::format(
                "BTI index page at {} is too short: {} bytes", page_pos, _page.size()));
        }
        _count = seastar::read_be<uint16_t>(reinterpret_cast<const char*>(_page.data() + _page.size() - bti_page_footer_size));
        if (bti_page_footer_size + size_t(_count) * bti_chunk_table_entry_size > _page.size()) {
            throw_malformed_sstable_exception(fmt::format(
                "BTI index page at {} has too many chunk table slots: {}", page_pos, _count));
        }
    }
    // The number of slots in the chunk table.
    uint16_t table_size() const noexcept {
        return _count;
    }
    // The size of the usable area: the bytes before the chunk table.
    size_t usable_size() const noexcept {
        return _page.size() - bti_page_footer_size - size_t(_count) * bti_chunk_table_entry_size;
    }
    // The stored CRC32.
    uint32_t stored_crc() const noexcept {
        return seastar::read_be<uint32_t>(reinterpret_cast<const char*>(_page.data() + _page.size() - 4));
    }
};

// The CRC32 of a page, as stored in its footer.
inline uint32_t compute_bti_page_crc(const_bytes page) {
    return crc32_utils::checksum(crc32_utils::init_checksum(),
        reinterpret_cast<const char*>(page.data()), page.size() - 4);
}

// Checks the size, the slot count and the CRC32 of a page.
// Throws malformed_sstable_exception if the page is corrupt.
inline void verify_bti_page(const_bytes page, uint64_t page_pos, uint64_t page_size, std::string_view file_name) {
    if (page.size() != page_size) {
        throw_malformed_sstable_exception(fmt::format(
            "{}: BTI index page at {} has {} bytes, expected {}", file_name, page_pos, page.size(), page_size));
    }
    auto view = bti_page_view(page, page_pos);
    auto crc = compute_bti_page_crc(page);
    if (crc != view.stored_crc()) {
        throw_malformed_sstable_exception(fmt::format(
            "{}: BTI index page at {} has a bad checksum: stored {:#010x}, computed {:#010x}",
            file_name, page_pos, view.stored_crc(), crc));
    }
}

} // namespace sstables::trie
