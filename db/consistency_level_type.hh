/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <fmt/core.h>

namespace db {

/// CQL consistency levels.
///
/// Values are guaranteed to be dense and in the tight range [MIN_VALUE, MAX_VALUE].
enum class consistency_level {
    ANY, MIN_VALUE = ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    SERIAL,
    LOCAL_SERIAL,
    LOCAL_ONE, MAX_VALUE = LOCAL_ONE
};

}

template <> struct fmt::formatter<db::consistency_level> {
private:
    static std::string_view to_string(db::consistency_level);
public:
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(db::consistency_level cl, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", to_string(cl));
    }
};
