#pragma once

/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <concepts>
#include <functional>

namespace utils {

template <typename Signature>
class wrapped_function;

/// type-safe wrapper around \c std::function that uses concepts to enforce
/// strict type checking on lambdas passed to it.
/// Works around https://github.com/llvm/llvm-project/issues/141791
template <typename Ret, typename... Args, bool Noexcept>
class wrapped_function<Ret (Args...) noexcept(Noexcept)> {
    std::function<Ret (Args...) noexcept(Noexcept)> _func;

public:
    wrapped_function() noexcept = default;
    wrapped_function(std::nullptr_t) noexcept {}

    template <std::invocable<Args...> Func>
    requires ((std::is_void_v<Ret> && std::is_void_v<std::invoke_result_t<Func, Args...>>)
        || (!std::is_void_v<Ret> && std::convertible_to<std::invoke_result_t<Func, Args...>, Ret>))
    wrapped_function(Func&& func) : _func(std::forward<Func>(func)) {}

    explicit operator bool() const noexcept { return static_cast<bool>(_func); }

    Ret operator()(Args...args) const noexcept(Noexcept) {
        return _func(std::forward<Args>(args)...);
    }
};

} // namespace utils
