/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <type_traits>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>

namespace sstables {

// `pq` encodes the Data component as Parquet. Everything else about the sstable
// -- the component set, the statistics layout, the index -- follows the `m`
// family, because only the row encoding changes. See
// docs/dev/parquet-storage-format.md section 5.2.
enum class sstable_version_types { ka, la, mc, md, me, ms, mt, pq };
enum class sstable_format_types { big };

// `pq` is a member of both arrays as of 2026-08-17: it is a version the node can
// actually read and write.
//
// It round-trips the whole mutation model -- row markers, row and partition
// tombstones, static rows, range tombstones, non-frozen collections and counters --
// clears all 34 sub-tests of sstable_conforms_to_mutation_source_test, and leaves
// sstable_datafile_test at its pre-existing baseline for this environment. It also
// collects the same Statistics metadata mx does: a per-partition column_stats
// (timestamp, local-deletion-time and TTL trackers, the tombstone drop-time
// histogram, row and cell counts) plus update_min_max_components(). That metadata is
// what compaction and tombstone GC read, so it is correctness rather than reporting.
//
// Two things are deliberately *not* implied by membership:
//
//   - `pq` is never the default. get_highest_sstable_version() skips it, so a table is
//     still written as the newest native version unless its `storage_format` schema
//     property opts in. Enrolling pq without that change silently made Parquet the
//     default for every caller that asks for "the highest version".
//   - `pq` sorts last in the enum but is a different format, not a newer generation of
//     the same one -- see implies_mx_generation() below.
//
// Still missing, and none of it blocks membership: partition_size / start_offset stay
// 0 (byte offsets do not exist per partition, since the Parquet image is encoded once
// at end of stream), add_compression_ratio() is not called (pq carries the CRC
// component set, not CompressionInfo), and sstable::find_first_position_in_partition
// has no pq path. See docs/dev/parquet-storage-format.md section 11.
//
// pq cannot be added to one array alone: check_sstable_versions() requires anything at
// or after oldest_writable_sstable_format to be writable too. There is also a
// static_assert on writable_sstable_versions.size() in the conformance test whose whole
// job is to make someone notice.
constexpr std::array<sstable_version_types, 8> all_sstable_versions = {
    sstable_version_types::ka,
    sstable_version_types::la,
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
    sstable_version_types::ms,
    sstable_version_types::mt,
    sstable_version_types::pq,
};

constexpr std::array<sstable_version_types, 6> writable_sstable_versions = {
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
    sstable_version_types::ms,
    sstable_version_types::mt,
    sstable_version_types::pq,
};

constexpr sstable_version_types oldest_writable_sstable_format = sstable_version_types::mc;

// The newest version of the *native* format -- what a table is written as by
// default, and what "upgrade the sstables" upgrades to.
//
// This deliberately skips `pq`, even though `pq` sorts last in the enum and is a
// member of all_sstable_versions. `pq` is a different format, not a newer
// generation of the same one (see implies_mx_generation below), and Parquet is
// never the default: it is opt-in per table through the `storage_format` schema
// property. Returning the array's last element would make every caller that asks
// for "the highest version" -- 31 of them, including every test that creates an
// sstable without naming a version -- silently start writing Parquet.
inline auto get_highest_sstable_version() {
    auto v = all_sstable_versions[all_sstable_versions.size() - 1];
    return v == sstable_version_types::pq
         ? all_sstable_versions[all_sstable_versions.size() - 2]
         : v;
}

sstable_version_types version_from_string(std::string_view s);
sstable_format_types format_from_string(std::string_view s);

// `pq` sorts after every mx version, but it is a different format rather than a
// newer generation of the same one. Comparisons of the form "v >= mc" are fine
// where they ask a structural question pq also answers yes to (index shape,
// filter format, serialization header). They are NOT fine where the code infers
// "we already have an sstable at version X, so the cluster must support X" --
// pq's ordinal position implies nothing about mt or ms support. Use this for
// those.
constexpr bool implies_mx_generation(sstable_version_types v, sstable_version_types at_least) {
    return v != sstable_version_types::pq && v >= at_least;
}

bool has_summary_and_index(sstable_version_types v);
bool uses_legacy_dk_order(sstable_version_types v);

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
