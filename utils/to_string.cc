/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/to_string.hh"

auto fmt::formatter<std::strong_ordering>::format(std::strong_ordering order, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    if (order > 0) {
        return fmt::format_to(ctx.out(), "gt");
    } else if (order < 0) {
        return fmt::format_to(ctx.out(), "lt");
    } else {
        return fmt::format_to(ctx.out(), "eq");
    }
}

auto fmt::formatter<std::weak_ordering>::format(std::weak_ordering order, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    if (order > 0) {
        return fmt::format_to(ctx.out(), "gt");
    } else if (order < 0) {
        return fmt::format_to(ctx.out(), "lt");
    } else {
        return fmt::format_to(ctx.out(), "eq");
    }
}

auto fmt::formatter<std::partial_ordering>::format(std::partial_ordering order, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    if (order == std::partial_ordering::unordered) {
        return fmt::format_to(ctx.out(), "unordered");
    } else if (order > 0) {
        return fmt::format_to(ctx.out(), "gt");
    } else if (order < 0) {
        return fmt::format_to(ctx.out(), "lt");
    } else {
        return fmt::format_to(ctx.out(), "eq");
    }
}
