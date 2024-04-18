/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <string_view>
#include <fmt/format.h>

namespace streaming {

enum class stream_reason : uint8_t {
    unspecified,
    bootstrap,
    decommission,
    removenode,
    rebuild,
    repair,
    replace,
    tablet_migration,
    tablet_rebuild,
};

}

template <>
struct fmt::formatter<streaming::stream_reason> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const streaming::stream_reason& r, FormatContext& ctx) const {
        using enum streaming::stream_reason;
        switch (r) {
        case unspecified:
            return formatter<string_view>::format("unspecified", ctx);
        case bootstrap:
            return formatter<string_view>::format("bootstrap", ctx);
        case decommission:
            return formatter<string_view>::format("decommission", ctx);
        case removenode:
            return formatter<string_view>::format("removenode", ctx);
        case rebuild:
            return formatter<string_view>::format("rebuild", ctx);
        case repair:
            return formatter<string_view>::format("repair", ctx);
        case replace:
            return formatter<string_view>::format("replace", ctx);
        case tablet_migration:
            return formatter<string_view>::format("tablet migration", ctx);
        case tablet_rebuild:
            return formatter<string_view>::format("tablet rebuild", ctx);
        }
        std::abort();
    }
};
