/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <functional>

namespace detail {

template<typename T, typename Comparator>
class extremum_tracker {
    T _default_value;
    bool _is_set = false;
    T _value;
public:
    extremum_tracker() {}

    explicit extremum_tracker(T default_value) {
        _default_value = default_value;
    }

    void update(T value) {
        if (!_is_set) {
            _value = value;
            _is_set = true;
        } else {
            if (Comparator{}(value,_value)) {
                _value = value;
            }
        }
    }

    T get() const {
        if (_is_set) {
            return _value;
        }
        return _default_value;
    }
};

} // namespace detail

template <typename T>
using min_tracker = detail::extremum_tracker<T, std::less<T>>;

template <typename T>
using max_tracker = detail::extremum_tracker<T, std::greater<T>>;
