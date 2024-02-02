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

enum class read_repair_decision {
  NONE,
  GLOBAL,
  DC_LOCAL
};

}

template <> struct fmt::formatter<db::read_repair_decision> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(db::read_repair_decision d, fmt::format_context& ctx) const {
        std::string_view name;
        switch (d) {
        using enum db::read_repair_decision;
        case NONE:
            name = "NONE";
            break;
        case GLOBAL:
            name = "GLOBAL";
            break;
        case DC_LOCAL:
            name = "DC_LOCAL";
            break;
        default:
              name = "ERR";
              break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
