#pragma once

/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/format.h>

#include <seastar/util/bool_class.hh>
#include <seastar/core/sstring.hh>

#include "enum_set.hh"

using namespace seastar;

namespace utils {

enum class optional_param_flag {
    user_provided,
    force
};

using optional_param_flags_set = enum_set<super_enum<optional_param_flag,
    optional_param_flag::user_provided,
    optional_param_flag::force
>>;

template <class T = sstring>
class optional_param_base {
public:
    using value_type = T;

    using flag = optional_param_flag;
    using flags_set = optional_param_flags_set;

    std::optional<T> _value;
    // or implicitly provided as default by scylla
    flags_set _flags;

public:
    optional_param_base() = default;

    explicit optional_param_base(sstring value, flags_set opts = {}) noexcept
        : _value(std::move(value))
        , _flags(opts)
    {}

    template <typename... Args>
    optional_param_base& emplace(Args... args) {
        _value.emplace(std::forward<Args>(args)...);
        return *this;
    }

    constexpr explicit operator bool() const noexcept {
        return has_value();
    }

    constexpr const T* operator->() const noexcept {
        return &_value;
    }

    constexpr const T& operator*() const noexcept {
        return *_value;
    }

    constexpr bool has_value() const noexcept {
        return _value.has_value();
    }

    constexpr const T& value() const noexcept {
        return _value.value();
    }

    constexpr T value_or(T&& default_value) const noexcept {
        return _value.value_or(std::forward<T>(default_value));
    }

    flags_set flags() const noexcept {
        return _flags;
    }

    constexpr bool user_provided() const noexcept {
        return _flags.contains(flag::user_provided);
    }

    optional_param_base& set_user_provided(bool value = true) noexcept {
        if (value) {
            _flags.set(flag::user_provided);
        } else {
            _flags.remove(flag::user_provided);
        }
        return *this;
    }

    constexpr bool force() const noexcept {
        return _flags.contains(flag::force);
    }

    optional_param_base& set_force(bool value = true) {
        if (value) {
            _flags.set(flag::force);
        } else {
            _flags.remove(flag::force);
        }
        return *this;
    }

    void reset() noexcept {
        _value.reset();
        _flags = {};
    }
};

using optional_param = optional_param_base<sstring>;

} //namespace utils

template<>
struct fmt::formatter<utils::optional_param_flags_set> : fmt::formatter<string_view> {
    auto format(const utils::optional_param_flags_set& flags, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template<class T>
struct fmt::formatter<utils::optional_param_base<T>> : fmt::formatter<string_view> {
    auto format(const utils::optional_param_base<T>& p, fmt::format_context& ctx) const -> decltype(ctx.out()) {
        if (p) {
            return fmt::format_to(ctx.out(), "{} ({})", p.value(), p.flags());
        } else {
            return fmt::format_to(ctx.out(), "(none)");
        }
    }
};
