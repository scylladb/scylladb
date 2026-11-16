/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option). It defines how the cache splits an sstable
// component into pages.

#pragma once

#include <cstdint>

namespace sstables {

struct compression;

// How the object storage page cache (see object_storage_page_cache.hh) splits
// an sstable component into pages, the units in which the component's byte
// ranges are cached. Positions are in the component's file as it is stored on
// the object store - for a compressed Data component, in the compressed file.
//
// All the readers of a component must split it into the same pages, so that
// a page cached by one reader is found by another. For example, a scan must
// cache its pages at the same positions where a later point read will look
// for them: if a scan cached data at the positions of its read buffers (0,
// 128 KB, 256 KB, ...), while point reads looked up the positions of
// compressed chunks, the point reads would never find the scan's pages.
//
// There are two kinds of pages:
//
// * A compressed Data component is split into its compressed chunks
//   (compressed_page_geometry), at the positions listed in the compression
//   metadata (the CompressionInfo component), so its pages have different
//   sizes. This is natural: the compressed reader already reads and
//   decompresses whole chunks, and caching the chunks compressed keeps more
//   data in the cache.
//
// * All the other components - an uncompressed Data component, and the index
//   components (Partitions, Rows, and the older Index) - are split into
//   4 KiB pages (fixed_size_page_geometry), at multiples of 4 KiB, except
//   that the last page ends at the end of the file. The index readers already
//   read and cache the index in aligned 4 KiB pages (see utils::cached_file),
//   so each of their reads is one cached page.
class page_geometry {
public:
    // The byte range [start, end) of a page.
    struct page_bounds {
        uint64_t start;
        uint64_t end;
    };

    virtual ~page_geometry() = default;

    // The size of the component's file, where its last page ends.
    virtual uint64_t file_size() const = 0;

    // The page which contains the byte at the given position. Throws
    // std::out_of_range if the position is not smaller than file_size().
    virtual page_bounds page_at(uint64_t pos) const = 0;

    // The average size of the component's pages, at least 1. It estimates
    // how many pages hold a given number of bytes.
    virtual uint64_t average_page_size() const = 0;

    // The smallest range of whole pages which contains the byte range
    // [from, to). The range must not be empty, and must be inside the file.
    //
    // A read which misses the cache is extended to these pages: a GET request
    // to the object store costs the same regardless of its size, so reading a
    // few more bytes costs nothing, and allows caching whole pages.
    page_bounds pages_covering(uint64_t from, uint64_t to) const;
};

// Pages of a fixed size, at positions which are multiples of that size. The
// last page is shorter if the file size is not a multiple of the page size.
// This is how all the components are split, except a compressed Data
// component (see page_geometry above).
class fixed_size_page_geometry final : public page_geometry {
    uint64_t _file_size;
    uint64_t _page_size;
public:
    // The sstable index readers read and cache the index components
    // (Partitions, Rows, Index) in aligned 4 KiB pages (see
    // utils::cached_file), so caching them on disk in the same pages makes
    // every index page read either one hit or one miss.
    static constexpr uint64_t default_page_size = 4096;

    explicit fixed_size_page_geometry(uint64_t file_size, uint64_t page_size = default_page_size)
        : _file_size(file_size), _page_size(page_size) {}

    virtual uint64_t file_size() const override {
        return _file_size;
    }
    virtual page_bounds page_at(uint64_t pos) const override;
    virtual uint64_t average_page_size() const override {
        return _page_size;
    }
};

// The pages of a compressed Data component, which are its compressed chunks
// (see page_geometry above). The chunks' positions are those listed in the
// compression metadata (the CompressionInfo component), which must outlive
// this object.
class compressed_page_geometry final : public page_geometry {
    const compression& _compression;
public:
    explicit compressed_page_geometry(const compression& c) : _compression(c) {}

    virtual uint64_t file_size() const override;
    virtual page_bounds page_at(uint64_t pos) const override;
    virtual uint64_t average_page_size() const override;
};

} // namespace sstables
