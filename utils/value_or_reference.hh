/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>

template <typename T>
struct value_or_reference {
    std::optional<T> _opt;
    const T& _ref;

    explicit value_or_reference(T&& v) : _opt(std::move(v)), _ref(*_opt) {}
    explicit value_or_reference(const T& v) : _ref(v) {}

    value_or_reference(value_or_reference&& o) : _opt(std::move(o._opt)), _ref(_opt ? *_opt : o._ref) {}
    value_or_reference(const value_or_reference& o) : _opt(o._opt), _ref(_opt ? *_opt : o._ref) {}

    const T& get() const {
        return _ref;
    }
};
