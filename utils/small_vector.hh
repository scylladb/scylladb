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

#if __has_include(<boost/container/small_vector.hpp>)

#include <boost/container/small_vector.hpp>

#if BOOST_VERSION >= 106000

namespace utils {

template <typename T, size_t N>
using small_vector = boost::container::small_vector<T, N>;

}

#else

#include <vector>

namespace utils {

// Older versions of boost have a double-free bug, so use std::vector instead.
template<typename T, size_t N>
using small_vector = std::vector<T>;

}

#endif

#else

namespace utils {

#include <vector>
template <typename T, size_t N>
using small_vector = std::vector<T>;

}

#endif
