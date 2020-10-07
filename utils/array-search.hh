/*
 * Copyright (C) 2020 ScyllaDB
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

#include <cstdint>
#include <limits>

namespace utils {

static constexpr int64_t simple_key_unused_value = std::numeric_limits<int64_t>::min();

/*
 * array_search_gt(value, array, capacity, size)
 *
 * Returns the index of the first element in the array that's greater
 * than the given value.
 */
int array_search_gt(int64_t val, const int64_t* array, const int capacity, const int size);

}
