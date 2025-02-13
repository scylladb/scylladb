/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <fmt/core.h>
#include <string_view>
#include <seastar/core/sstring.hh>

namespace db {

namespace view {

enum class build_status {
    STARTED,
    SUCCESS,
};

inline build_status build_status_from_string(std::string_view str) {
    if (str == "STARTED") {
        return build_status::STARTED;
    }
    if (str == "SUCCESS") {
        return build_status::SUCCESS;
    }
    throw std::runtime_error(fmt::format("Unknown view build status: {}", str));
}

inline seastar::sstring build_status_to_sstring(build_status status) {
    switch (status) {
    case build_status::STARTED:
        return "STARTED";
    case build_status::SUCCESS:
        return "SUCCESS";
    }
}

}

}
template <> struct fmt::formatter<db::view::build_status> : fmt::formatter<string_view> {
    auto format(const db::view::build_status& status, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", db::view::build_status_to_sstring(status));
    }
};
