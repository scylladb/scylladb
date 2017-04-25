/*
 * Copyright (C) 2017 ScyllaDB
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

#include <seastar/util/gcc6-concepts.hh>
#include <type_traits>

GCC6_CONCEPT(
template<typename T>
concept bool HasTriCompare =
    requires(const T& t) {
        { t.compare(t) } -> int;
    } && std::is_same<std::result_of_t<decltype(&T::compare)(T, T)>, int>::value; //FIXME: #1449
)

template<typename T>
class with_relational_operators {
private:
    template<typename U>
    GCC6_CONCEPT( requires HasTriCompare<U> )
    int do_compare(const U& t) const {
        return static_cast<const U*>(this)->compare(t);
    }
public:
    bool operator<(const T& t) const {
        return do_compare(t) < 0;
    }

    bool operator<=(const T& t) const {
        return do_compare(t) <= 0;
    }

    bool operator>(const T& t) const {
        return do_compare(t) > 0;
    }

    bool operator>=(const T& t) const {
        return do_compare(t) >= 0;
    }

    bool operator==(const T& t) const {
        return do_compare(t) == 0;
    }

    bool operator!=(const T& t) const {
        return do_compare(t) != 0;
    }
};
