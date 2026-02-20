/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <fmt/core.h>
#include <unordered_map>
#include <seastar/core/sstring.hh>
#include "enum_set.hh"

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

/// Mapper for consistency_level enum, for use with enum_option<>.
struct consistency_level_option {
    static std::unordered_map<seastar::sstring, consistency_level> map();
};

/// Efficient bitmask-based set of consistency levels.
using consistency_level_set = enum_set<super_enum<consistency_level,
    consistency_level::ANY,
    consistency_level::ONE,
    consistency_level::TWO,
    consistency_level::THREE,
    consistency_level::QUORUM,
    consistency_level::ALL,
    consistency_level::LOCAL_QUORUM,
    consistency_level::EACH_QUORUM,
    consistency_level::SERIAL,
    consistency_level::LOCAL_SERIAL,
    consistency_level::LOCAL_ONE>>;

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
