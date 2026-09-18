/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <fmt/format.h>

#include "enum_set.hh"

namespace sstables {

enum class component_type {
    Index,
    CompressionInfo,
    Data,
    TOC,
    Summary,
    Digest,
    CRC,
    Filter,
    Statistics,
    TemporaryTOC,
    TemporaryStatistics,
    Scylla,
    Rows,
    Partitions,
    TemporaryHashes,
    Unknown,
};

constexpr size_t num_component_types = size_t(component_type::Unknown);

// Type-safe set of recognized component types. Excludes Unknown, which is
// not a real component (it marks entries kept in _unrecognized_components).
using component_super_enum = super_enum<component_type,
        component_type::Index,
        component_type::CompressionInfo,
        component_type::Data,
        component_type::TOC,
        component_type::Summary,
        component_type::Digest,
        component_type::CRC,
        component_type::Filter,
        component_type::Statistics,
        component_type::TemporaryTOC,
        component_type::TemporaryStatistics,
        component_type::Scylla,
        component_type::Rows,
        component_type::Partitions,
        component_type::TemporaryHashes>;
using component_set = enum_set<component_super_enum>;

// Ensure component_set covers exactly the component_type domain below Unknown.
// The highest enumerator in the set must be the one just before Unknown.
static_assert(component_super_enum::max_sequence == num_component_types - 1,
        "component_set must cover every component_type below Unknown; update the list when adding a new component_type");

struct sstable;
struct component_name {
    const sstable& sst;
    component_type component;
    seastar::sstring format() const;
};

}

using component_type = ::sstables::component_type;

template <>
struct fmt::formatter<sstables::component_type> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::component_type& comp_type, FormatContext& ctx) const {
        using enum sstables::component_type;
        switch (comp_type) {
        case Index:
            return formatter<string_view>::format("Index", ctx);
        case CompressionInfo:
            return formatter<string_view>::format("CompressionInfo", ctx);
        case Data:
            return formatter<string_view>::format("Data", ctx);
        case TOC:
            return formatter<string_view>::format("TOC", ctx);
        case Summary:
            return formatter<string_view>::format("Summary", ctx);
        case Digest:
            return formatter<string_view>::format("Digest", ctx);
        case CRC:
            return formatter<string_view>::format("CRC", ctx);
        case Filter:
            return formatter<string_view>::format("Filter", ctx);
        case Statistics:
            return formatter<string_view>::format("Statistics", ctx);
        case TemporaryTOC:
            return formatter<string_view>::format("TemporaryTOC", ctx);
        case TemporaryStatistics:
            return formatter<string_view>::format("TemporaryStatistics", ctx);
        case Scylla:
            return formatter<string_view>::format("Scylla", ctx);
        case Partitions:
            return formatter<string_view>::format("Partitions", ctx);
        case Rows:
            return formatter<string_view>::format("Rows", ctx);
        case TemporaryHashes:
            return formatter<string_view>::format("TemporaryHashes", ctx);
        case Unknown:
            return formatter<string_view>::format("Unknown", ctx);
        }
    }
};

template <>
struct fmt::formatter<sstables::component_name> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::component_name& name, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", name.format());
    }
};
