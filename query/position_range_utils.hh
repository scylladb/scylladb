/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "mutation/position_in_partition.hh"
#include "query/query-request.hh"
#include "keys/clustering_interval_set.hh"

namespace query {

/// Helper utilities for working with position_range and migrating from clustering_range.
///
/// These utilities support the gradual migration from clustering_range (interval<clustering_key_prefix>)
/// to position_range. See docs/dev/clustering-range-to-position-range-migration.md for details.

/// Convert a vector of clustering_ranges to a vector of position_ranges
inline std::vector<position_range> clustering_row_ranges_to_position_ranges(const clustering_row_ranges& ranges) {
    std::vector<position_range> result;
    result.reserve(ranges.size());
    for (const auto& r : ranges) {
        result.emplace_back(position_range::from_range(r));
    }
    return result;
}

/// Convert a vector of position_ranges to a vector of clustering_ranges
/// Note: Empty position ranges (those that don't contain any keys) are skipped
inline clustering_row_ranges position_ranges_to_clustering_row_ranges(const std::vector<position_range>& ranges, const schema& s) {
    clustering_row_ranges result;
    result.reserve(ranges.size());
    for (const auto& r : ranges) {
        if (auto cr = position_range_to_clustering_range(r, s)) {
            result.emplace_back(std::move(*cr));
        }
    }
    return result;
}

/// Deoverlap clustering_row_ranges correctly using clustering_interval_set.
/// This avoids the known bugs with clustering_range::deoverlap (see scylladb#22817, #21604, #8157).
inline clustering_row_ranges deoverlap_clustering_row_ranges(const schema& s, const clustering_row_ranges& ranges) {
    clustering_interval_set interval_set(s, ranges);
    return interval_set.to_clustering_row_ranges();
}

/// Intersect two clustering_row_ranges correctly using clustering_interval_set.
/// This avoids the known bugs with clustering_range::intersection (see scylladb#22817, #21604, #8157).
inline clustering_row_ranges intersect_clustering_row_ranges(const schema& s, 
                                                              const clustering_row_ranges& ranges1,
                                                              const clustering_row_ranges& ranges2) {
    clustering_interval_set set1(s, ranges1);
    clustering_interval_set set2(s, ranges2);
    
    clustering_interval_set result;
    for (const auto& r : set1) {
        if (set2.overlaps(s, r)) {
            result.add(s, r);
        }
    }
    
    return result.to_clustering_row_ranges();
}

} // namespace query
