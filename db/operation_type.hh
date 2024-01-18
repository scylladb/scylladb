/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <fmt/core.h>

namespace db {

enum class operation_type : uint8_t {
    read = 0,
    write = 1
};

}

template <> struct fmt::formatter<db::operation_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(db::operation_type, fmt::format_context& ctx) const -> decltype(ctx.out());
};
