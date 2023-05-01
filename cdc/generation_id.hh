/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <variant>

#include "db_clock.hh"
#include "utils/UUID.hh"

namespace cdc {

struct generation_id_v1 {
    db_clock::time_point ts;
    bool operator==(const generation_id_v1&) const = default;
};

struct generation_id_v2 {
    db_clock::time_point ts;
    utils::UUID id;
    bool operator==(const generation_id_v2&) const = default;
};

using generation_id = std::variant<generation_id_v1, generation_id_v2>;

db_clock::time_point get_ts(const generation_id&);

} // namespace cdc

template <>
struct fmt::formatter<cdc::generation_id_v1> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const cdc::generation_id_v1& gen_id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", gen_id.ts);
    }
};

template <>
struct fmt::formatter<cdc::generation_id_v2> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const cdc::generation_id_v2& gen_id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "({}, {})", gen_id.ts, gen_id.id);
    }
};

template <>
struct fmt::formatter<cdc::generation_id> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const cdc::generation_id& gen_id, FormatContext& ctx) const {
        return std::visit([&ctx] (auto& id) {
            return fmt::format_to(ctx.out(), "{}", id);
        }, gen_id);
    }
};
