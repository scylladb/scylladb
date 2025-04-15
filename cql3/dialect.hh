// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <fmt/core.h>

namespace cql3 {

struct dialect {
    bool duplicate_bind_variable_names_refer_to_same_variable = true;  // if :a is found twice in a query, the two references are to the same variable (see #15559)
    bool operator==(const dialect&) const = default;
};

inline
dialect
internal_dialect() {
    return dialect{
        .duplicate_bind_variable_names_refer_to_same_variable = true,
    };
}

}

template <>
struct fmt::formatter<cql3::dialect> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const cql3::dialect& d, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "cql3::dialect{{duplicate_bind_variable_names_refer_to_same_variable={}}}",
                d.duplicate_bind_variable_names_refer_to_same_variable);
    }
};
