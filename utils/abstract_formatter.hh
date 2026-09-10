/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <fmt/format.h>
#include <functional>
#include <type_traits>

/// Makes a callable usable as a "{}" argument of fmt::format and friends.
/// The callable receives the enclosing format context and writes into it,
/// so nothing is rendered unless the message is actually formatted (e.g.
/// when a log line is disabled), and no intermediate string is built.
///
/// The callable is stored by value without type erasure, so wrapping a
/// lambda that captures a few references costs nothing on the hot path:
///
///     const auto state = lambda_formatter([&] (fmt::format_context& ctx) {
///         fmt::format_to(ctx.out(), "x={}", x);
///     });
///     logger.debug("state: {}", state);
///
/// If Func is contextually convertible to bool (e.g. std::function), an
/// empty callable formats as nothing instead of being invoked.
template <typename Func>
requires std::is_invocable_v<const Func&, fmt::format_context&>
class lambda_formatter {
    Func _func;
public:
    lambda_formatter() = default;
    explicit lambda_formatter(Func func) : _func(std::move(func)) {}

    fmt::format_context::iterator format_to(fmt::format_context& ctx) const {
        if constexpr (requires { static_cast<bool>(_func); }) {
            if (!_func) {
                return ctx.out();
            }
        }
        _func(ctx);
        return ctx.out();
    }
};

template <typename Func>
struct fmt::formatter<lambda_formatter<Func>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    auto format(const lambda_formatter<Func>& formatter, fmt::format_context& ctx) const {
        return formatter.format_to(ctx);
    }
};

/// Type-erased formatter.
/// Allows passing formattable objects without exposing their types.
/// Unlike a lambda_formatter over a concrete lambda, constructing it may
/// allocate, so prefer lambda_formatter where the type needn't be erased.
using abstract_formatter = lambda_formatter<std::function<void(fmt::format_context&)>>;
