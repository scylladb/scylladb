/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): how the cache splits an sstable component into
// pages. See sstables/object_storage_page_cache_geometry.hh.

#include <algorithm>
#include <ranges>
#include <stdexcept>
#include <fmt/format.h>
#include "sstables/object_storage_page_cache_geometry.hh"
#include "sstables/compress.hh"

namespace sstables {

static void check_position(uint64_t pos, uint64_t file_size) {
    if (pos >= file_size) {
        throw std::out_of_range(fmt::format("page_geometry: position {} is beyond the file size {}", pos, file_size));
    }
}

page_geometry::page_bounds page_geometry::pages_covering(uint64_t from, uint64_t to) const {
    if (from >= to) {
        throw std::out_of_range(fmt::format("page_geometry: empty range [{}, {})", from, to));
    }
    return {page_at(from).start, page_at(to - 1).end};
}

page_geometry::page_bounds fixed_size_page_geometry::page_at(uint64_t pos) const {
    check_position(pos, _file_size);
    uint64_t start = pos - pos % _page_size;
    return {start, std::min(start + _page_size, _file_size)};
}

uint64_t compressed_page_geometry::file_size() const {
    return _compression.compressed_file_length();
}

page_geometry::page_bounds compressed_page_geometry::page_at(uint64_t pos) const {
    check_position(pos, file_size());
    // Find the last chunk which starts at or before pos, by a binary search
    // over the chunks' start positions: chunk i starts at offsets.at(i), and
    // ends where the next chunk starts, or at the end of the file. The search
    // is over the chunks' indexes, because segmented_offsets' iterator can't
    // be used by the standard algorithms.
    auto offsets = _compression.offsets.get_accessor();
    size_t n = _compression.offsets.size();
    auto indexes = std::views::iota(size_t(0), n);
    auto it = std::ranges::upper_bound(indexes, pos, {}, [&] (size_t i) { return offsets.at(i); });
    // The first chunk starts at 0, so "it" is after it.
    size_t chunk = std::ranges::distance(indexes.begin(), it) - 1;
    uint64_t end = chunk + 1 < n ? offsets.at(chunk + 1) : file_size();
    return {offsets.at(chunk), end};
}

} // namespace sstables
