/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <limits>
#include <optional>

namespace detail {

template<typename T, typename Comparator>
requires std::is_nothrow_copy_constructible_v<T> && std::is_nothrow_move_constructible_v<T>
class extremum_tracker {
    T _default_value;
    std::optional<T> _value;
public:
    explicit extremum_tracker(const T& default_value) noexcept
        : _default_value(default_value)
    {}

    void update(const T& value) noexcept {
        if (!_value || Comparator{}(value, *_value)) {
            _value = value;
        }
    }

    void update(const extremum_tracker& other) noexcept {
        if (other._value) {
            update(*other._value);
        }
    }

    const T& get() const noexcept {
        return _value ? *_value : _default_value;
    }
};

} // namespace detail

template <typename T>
using min_tracker = detail::extremum_tracker<T, std::less<T>>;

template <typename T>
using max_tracker = detail::extremum_tracker<T, std::greater<T>>;

template <typename T>
requires std::is_nothrow_copy_constructible_v<T> && std::is_nothrow_move_constructible_v<T>
class min_max_tracker {
    min_tracker<T> _min_tracker;
    max_tracker<T> _max_tracker;
public:
    min_max_tracker() noexcept
        : _min_tracker(std::numeric_limits<T>::min())
        , _max_tracker(std::numeric_limits<T>::max())
    {}

    min_max_tracker(const T& default_min, const T& default_max) noexcept
        : _min_tracker(default_min)
        , _max_tracker(default_max)
    {}

    void update(const T& value) noexcept {
        _min_tracker.update(value);
        _max_tracker.update(value);
    }

    void update(const min_max_tracker<T>& other) noexcept {
        _min_tracker.update(other._min_tracker);
        _max_tracker.update(other._max_tracker);
    }

    const T& min() const noexcept {
        return _min_tracker.get();
    }

    const T& max() const noexcept {
        return _max_tracker.get();
    }
};
