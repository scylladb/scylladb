/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <stdexcept>
#include <type_traits>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>

namespace sstables {

enum class sstable_version_types { ka, la, mc, md, me };
enum class sstable_format_types { big };

constexpr std::array<sstable_version_types, 5> all_sstable_versions = {
    sstable_version_types::ka,
    sstable_version_types::la,
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
};

constexpr std::array<sstable_version_types, 3> writable_sstable_versions = {
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
};

constexpr sstable_version_types oldest_writable_sstable_format = sstable_version_types::mc;

template <size_t S1, size_t S2>
constexpr bool check_sstable_versions(const std::array<sstable_version_types, S1>& all_sstable_versions,
        const std::array<sstable_version_types, S2>& writable_sstable_versions, sstable_version_types oldest_writable_sstable_format) {
    for (auto v : writable_sstable_versions) {
        if (v < oldest_writable_sstable_format) {
            return false;
        }
    }
    size_t expected = 0;
    for (auto v : all_sstable_versions) {
        if (v >= oldest_writable_sstable_format) {
            ++expected;
        }
    }
    return expected == S2;
}

static_assert(check_sstable_versions(all_sstable_versions, writable_sstable_versions, oldest_writable_sstable_format));

inline auto get_highest_sstable_version() {
    return all_sstable_versions[all_sstable_versions.size() - 1];
}

inline sstable_version_types from_string(const seastar::sstring& format) {
    if (format == "ka") {
        return sstable_version_types::ka;
    }
    if (format == "la") {
        return sstable_version_types::la;
    }
    if (format == "mc") {
        return sstable_version_types::mc;
    }
    if (format == "md") {
        return sstable_version_types::md;
    }
    if (format == "me") {
        return sstable_version_types::me;
    }
    throw std::invalid_argument("Wrong sstable format name: " + format);
}

extern const std::unordered_map<sstable_version_types, seastar::sstring, seastar::enum_hash<sstable_version_types>> version_string;
extern const std::unordered_map<sstable_format_types, seastar::sstring, seastar::enum_hash<sstable_format_types>> format_string;


inline seastar::sstring to_string(sstable_version_types format) {
    switch (format) {
        case sstable_version_types::ka: return "ka";
        case sstable_version_types::la: return "la";
        case sstable_version_types::mc: return "mc";
        case sstable_version_types::md: return "md";
        case sstable_version_types::me: return "me";
    }
    throw std::runtime_error("Wrong sstable format");
}

inline int operator<=>(sstable_version_types a, sstable_version_types b) {
    auto to_int = [] (sstable_version_types x) {
        return static_cast<std::underlying_type_t<sstable_version_types>>(x);
    };
    return to_int(a) - to_int(b);
}

}
