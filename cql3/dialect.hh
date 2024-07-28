// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <fmt/core.h>

namespace cql3 {

struct dialect {
    bool operator==(const dialect&) const = default;
};

inline
dialect
internal_dialect() {
    return dialect{
    };
}

}

template <>
struct fmt::formatter<cql3::dialect> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const cql3::dialect& d, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "cql3::dialect{{}}");
    }
};
