/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/format.h>

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
    Unknown,
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
        case Unknown:
            return formatter<string_view>::format("Unknown", ctx);
        }
    }
};
