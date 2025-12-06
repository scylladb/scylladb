/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <fmt/format.h>
#include <functional>

/// Type-erased formatter.
/// Allows passing formattable objects without exposing their types.
class abstract_formatter {
    std::function<void(fmt::format_context&)> _formatter;
public:
    abstract_formatter() = default;

    template<typename Func>
    requires std::is_invocable_v<Func, fmt::format_context&>
    explicit abstract_formatter(Func&& f) : _formatter(std::forward<Func>(f)) {}

    fmt::format_context::iterator format_to(fmt::format_context& ctx) const {
        if (_formatter) {
            _formatter(ctx);
        }
        return ctx.out();
    }

    explicit operator bool() const noexcept { return bool(_formatter); }
};

template <> struct fmt::formatter<abstract_formatter> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    auto format(const abstract_formatter& formatter, fmt::format_context& ctx) const {
        return formatter.format_to(ctx);
    }
};
