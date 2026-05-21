/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include <seastar/core/sstring.hh>

namespace cql3 {

/// Helpers for computing external (heap-allocated) memory usage of CQL3 types.

/// Per-allocation overhead imposed by the system allocator. Seastar's allocator
/// rounds small allocations up to size classes and stores per-object bookkeeping;
/// glibc malloc adds a chunk header plus alignment. We use a single conservative
/// estimate so that node-based containers are not systematically under-counted.
inline constexpr size_t heap_allocation_overhead = 16;

/// Heap memory used by the nodes of a std::map (libstdc++ _Rb_tree_node): each
/// element is stored in a node alongside a header of three pointers
/// (parent/left/right) and a color word. Does not include memory owned by the
/// stored values themselves; callers add that separately.
template <typename Map>
size_t map_node_external_memory_usage(const Map& m) noexcept {
    constexpr size_t node_header = 3 * sizeof(void*) + sizeof(size_t);
    return m.size() * (sizeof(typename Map::value_type) + node_header + heap_allocation_overhead);
}

/// Heap memory used by the nodes and bucket array of a std::unordered_map
/// (libstdc++ _Hash_node): each element is stored in a node with a next pointer
/// and a cached hash code, plus the container's bucket array. Uses the
/// container's actual bucket_count() for the bucket array rather than estimating
/// it. Does not include memory owned by the stored values themselves; callers
/// add that separately.
template <typename Map>
size_t unordered_map_node_external_memory_usage(const Map& m) noexcept {
    constexpr size_t node_header = sizeof(void*) + sizeof(size_t);
    return m.bucket_count() * sizeof(void*)
         + m.size() * (sizeof(typename Map::value_type) + node_header + heap_allocation_overhead);
}

/// Heap memory used by the nodes and bucket array of a std::unordered_set
/// (libstdc++ _Hash_node): each element is stored in a separately heap-allocated
/// node holding the value, a next pointer and a cached hash code, plus the
/// container's bucket array. Uses the container's actual bucket_count() for the
/// bucket array rather than estimating it. Does not include memory owned by the
/// stored values themselves; callers add that separately.
template <typename Set>
size_t unordered_set_node_external_memory_usage(const Set& set) noexcept {
    constexpr size_t node_header = sizeof(void*) + sizeof(size_t);
    return set.bucket_count() * sizeof(void*)
         + set.size() * (sizeof(typename Set::value_type) + node_header + heap_allocation_overhead);
}

template <typename CharT, typename SizeT, SizeT MaxSize, bool NulTerminate>
size_t basic_sstring_external_memory_usage(
        const seastar::basic_sstring<CharT, SizeT, MaxSize, NulTerminate>& s) noexcept {
    if (s.size() > MaxSize) {
        return s.size() + (NulTerminate ? 1 : 0);
    }
    return 0;
}

inline size_t sstring_external_memory_usage(const seastar::sstring& s) noexcept {
    return basic_sstring_external_memory_usage(s);
}

template <typename T>
size_t vector_external_memory_usage(const std::vector<T>& v) noexcept {
    return v.capacity() * sizeof(T);
}

template <typename T>
size_t optional_external_memory_usage(const std::optional<T>& opt) {
    if (opt) {
        return opt->external_memory_usage();
    }
    return 0;
}

} // namespace cql3
