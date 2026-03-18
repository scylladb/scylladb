/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <variant>

#include "db_clock.hh"
#include "utils/UUID.hh"

namespace cdc {


struct generation_id {
    db_clock::time_point ts;
    utils::UUID id;
    bool operator==(const generation_id&) const = default;
};

db_clock::time_point get_ts(const generation_id&);

} // namespace cdc

template <>
struct fmt::formatter<cdc::generation_id> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const cdc::generation_id& gen_id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "({}, {})", gen_id.ts, gen_id.id);
    }
};
