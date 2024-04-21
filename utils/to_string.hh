/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>
#if FMT_VERSION >= 100000
#include <fmt/std.h>
#else
#include <optional>
#endif

#if FMT_VERSION < 100000

template <typename T>
struct fmt::formatter<std::optional<T>> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const std::optional<T>& opt, FormatContext& ctx) const {
        if (opt) {
            return fmt::format_to(ctx.out(), "{}", *opt);
        } else {
            return fmt::format_to(ctx.out(), "{{}}");
        }
    }
};

#endif

template <> struct fmt::formatter<std::strong_ordering> : fmt::formatter<string_view> {
    auto format(std::strong_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<std::weak_ordering> : fmt::formatter<string_view> {
    auto format(std::weak_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<std::partial_ordering> : fmt::formatter<string_view> {
    auto format(std::partial_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};
