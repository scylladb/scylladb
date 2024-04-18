/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

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

sstable_version_types version_from_string(std::string_view s);
sstable_format_types format_from_string(std::string_view s);

extern const std::unordered_map<sstable_version_types, seastar::sstring, seastar::enum_hash<sstable_version_types>> version_string;
extern const std::unordered_map<sstable_format_types, seastar::sstring, seastar::enum_hash<sstable_format_types>> format_string;


inline int operator<=>(sstable_version_types a, sstable_version_types b) {
    auto to_int = [] (sstable_version_types x) {
        return static_cast<std::underlying_type_t<sstable_version_types>>(x);
    };
    return to_int(a) - to_int(b);
}

}

template <>
struct fmt::formatter<sstables::sstable_version_types> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::sstable_version_types& version, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", sstables::version_string.at(version));
    }
};

template <>
struct fmt::formatter<sstables::sstable_format_types> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::sstable_format_types& format, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", sstables::format_string.at(format));
    }
};
