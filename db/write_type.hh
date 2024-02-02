/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <assert.h>
#include <cstdint>
#include <fmt/core.h>

namespace db {

enum class write_type : uint8_t {
    SIMPLE,
    BATCH,
    UNLOGGED_BATCH,
    COUNTER,
    BATCH_LOG,
    CAS,
    VIEW,
};

}

template <>
struct fmt::formatter<db::write_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(db::write_type, fmt::format_context& ctx) const -> decltype(ctx.out());
};
