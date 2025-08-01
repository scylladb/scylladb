/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/log.hh>

extern seastar::logger trie_logger;

namespace sstables::trie {

// Enables code which is useful for debugging during development,
// but too expensive to be compiled into release builds (even if dynamically disabled).
constexpr bool developer_build = false;

// Many asserts and logs are only useful during development,
// where the cost of logging doesn't matter at all.
// And during development it might be useful to have A LOT of them,
// and make them expensive.
//
// But in production, if the code is hot enough, we might care even about the small
// cost of dynamically checking whether a logger is enabled, which discourages adding more trace logs,
// which is very sad.
//
// Maybe the right thing is to do is to have logs that are compiled out in production builds
// and only enabled by developers who are actively working on the relevant feature.
// This way we free to add as many logs as we please, without worrying at all about the performance cost.
template <typename... Args>
void expensive_log(seastar::logger::format_info_t<Args...> fmt, Args&&... args) {
    if constexpr (developer_build) {
        trie_logger.trace(std::move(fmt), std::forward<Args>(args)...);
    }
}

inline void expensive_assert(bool expr, std::source_location srcloc = std::source_location::current()) {
    if constexpr (developer_build) {
        if (!expr) {
            __assert_fail("", srcloc.file_name(), srcloc.line(), srcloc.function_name());
        }
    }
}

// We aleady have bytes_view, so perhaps it should be used here.
// But std::span<const std::byte> is, in some sense, the standard type for this purpose.
// std::as_bytes() exists, after all.
using const_bytes = std::span<const std::byte>;

inline constexpr uint64_t round_down(uint64_t a, uint64_t factor) {
    return a - a % factor;
}

inline constexpr uint64_t round_up(uint64_t a, uint64_t factor) {
    return round_down(a + factor - 1, factor);
}

} // namespace sstables::trie
