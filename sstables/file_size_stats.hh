/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>

namespace sstables {

struct file_size_stats {
    // Actual size of files on disk.
    // In particular, for compressed sstables, this includes the post-compression size of data files.
    int64_t on_disk = 0;
    // The hypothetical size of files on disk if the data files were uncompressed.
    // (Not fully accurate. This is equivalent to `on_disk - physical_data_db_size + logical_data_db_size`.
    // For full accuracy, we would have to account for the replacement of CompressionInfo.db with CRC.db, etc.
    // We don't bother with that. This value is only used for estimation and metrics anyway).
    int64_t before_compression = 0;

    file_size_stats& operator+=(const file_size_stats& other) {
        on_disk += other.on_disk;
        before_compression += other.before_compression;
        return *this;
    }
    file_size_stats& operator-=(const file_size_stats& other) {
        on_disk -= other.on_disk;
        before_compression -= other.before_compression;
        return *this;
    }
    friend file_size_stats operator+(const file_size_stats& lhs, const file_size_stats& rhs) {
        file_size_stats result = lhs;
        result += rhs;
        return result;
    }
    bool operator==(const file_size_stats& other) const = default;
};

} // namespace sstables