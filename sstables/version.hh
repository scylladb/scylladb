/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <initializer_list>
#include <type_traits>
#include <utility>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>

namespace sstables {

enum class sstable_version_types { ka, la, mc, md, me, ms, mt };
enum class sstable_format_types { big };

constexpr bool version_is_one_of(sstable_version_types v, std::initializer_list<sstable_version_types> versions) {
    return std::ranges::find(versions, v) != versions.end();
}

// The Data file has the layout of Cassandra 2.x (`ka`, `la`).
constexpr bool has_kl_data_file(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {ka, la});
}

// The Data file has the layout of Cassandra 3.x (`mc` to `mt`),
// and it is read by the `mx` reader.
constexpr bool has_mx_data_file(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// The current sstable writer can write this version.
constexpr bool is_writable(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// The sstable has Index.db and Summary.db. Otherwise it has the trie index
// (Partitions.db and Rows.db).
constexpr bool has_summary_and_index(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {ka, la, mc, md, me});
}

// The promoted index in Index.db has the `m` layout.
constexpr bool has_mx_promoted_index(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// Statistics.db has a serialization header, which records the column
// names and types that the sstable was written with.
constexpr bool has_serialization_header(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// Statistics.db lists the generations that the sstable was compacted from.
constexpr bool has_compaction_ancestors(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {ka, la});
}

// Statistics.db has a correct maximum local deletion time, whoever wrote it.
// For older versions, only a Scylla-written sstable can be trusted.
constexpr bool has_correct_max_deletion_time(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// Statistics.db has correct minimum and maximum clustering keys.
constexpr bool has_correct_min_max_column_names(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {md, me, ms, mt});
}

// Statistics.db records the host that wrote the sstable.
constexpr bool has_originating_host_id(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {me, ms, mt});
}

// Filter.db uses the `m` serialization of the bloom filter.
constexpr bool has_m_format_filter(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// The checksums of the Data file have the `m` layout. The chunk checksums
// (in CRC.db or in a compressed Data file) and the digest use CRC32, and the
// digest of a compressed Data file also covers the chunk checksums.
// Otherwise they have the `k`/`l` layout, which uses Adler-32.
constexpr bool has_m_format_checksums(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {mc, md, me, ms, mt});
}

// The format can store a table with compact storage.
constexpr bool supports_compact_storage(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {ka, la, mc, md, me, ms, mt});
}

// Decorated keys in sstables and memtables are ordered by a bytewise comparison
// of their "legacy form".
// Due to a design mistake, `ms` was the one format with index encoding designed
// for the other (column-wise, wrong) ordering.
// All `ms` files which were ever actually produced are compatible with the legacy
// ordering anyway, because some asserts in the writer would prevent the creation
// of an sstable which doesn't match that ordering.
// Nevertheless, index readers have to know when they are dealing with `ms`, because
// it uses a specific encoding of partition keys in the index.
constexpr bool uses_legacy_dk_order(sstable_version_types v) {
    using enum sstable_version_types;
    return version_is_one_of(v, {ka, la, mc, md, me, mt});
}

// Returns true if `a` is an older version than `b` of the same format.
// This is the only order between versions. `upgradesstables` uses it to
// find the sstables that a newer version of their format supersedes.
// All versions so far belong to the `big` format.
constexpr bool is_older_within_format(sstable_version_types a, sstable_version_types b) {
    return std::to_underlying(a) < std::to_underlying(b);
}

// A set of sstable versions.
class sstable_version_set {
    uint32_t _mask = 0;

    static constexpr uint32_t bit(sstable_version_types v) {
        return uint32_t(1) << std::to_underlying(v);
    }
public:
    constexpr sstable_version_set() = default;
    constexpr sstable_version_set(std::initializer_list<sstable_version_types> versions) {
        for (auto v : versions) {
            insert(v);
        }
    }
    constexpr void insert(sstable_version_types v) {
        _mask |= bit(v);
    }
    constexpr bool contains(sstable_version_types v) const {
        return _mask & bit(v);
    }
    constexpr sstable_version_set& operator|=(const sstable_version_set& o) {
        _mask |= o._mask;
        return *this;
    }
    friend constexpr sstable_version_set operator|(sstable_version_set a, const sstable_version_set& b) {
        return a |= b;
    }
    constexpr bool operator==(const sstable_version_set&) const = default;
};

constexpr std::array<sstable_version_types, 7> all_sstable_versions = {
    sstable_version_types::ka,
    sstable_version_types::la,
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
    sstable_version_types::ms,
    sstable_version_types::mt,
};

constexpr std::array<sstable_version_types, 5> writable_sstable_versions = {
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
    sstable_version_types::ms,
    sstable_version_types::mt,
};

constexpr sstable_version_types oldest_writable_sstable_format = sstable_version_types::mc;

inline auto get_highest_sstable_version() {
    return all_sstable_versions[all_sstable_versions.size() - 1];
}

sstable_version_types version_from_string(std::string_view s);
sstable_format_types format_from_string(std::string_view s);

extern const std::unordered_map<sstable_version_types, seastar::sstring, seastar::enum_hash<sstable_version_types>> version_string;
extern const std::unordered_map<sstable_format_types, seastar::sstring, seastar::enum_hash<sstable_format_types>> format_string;

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
