/*
 * Copyright (C) 2020-present ScyllaDB
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
 *
 * To accomodate the single-instruction-multiple-data variant, observe
 * the following:
 *  - capacity must be a multiple of 4
 *  - any items with indexes in [size, capacity) must be initialized
 *    to std::numeric_limits<int64_t>::min()
 */
int array_search_gt(int64_t val, const int64_t* array, const int capacity, const int size);

static inline unsigned array_search_4_eq(uint8_t val, const uint8_t* array) {
    // Unrolled loop is few %s faster
    if (array[0] == val) {
        return 0;
    } else if (array[1] == val) {
        return 1;
    } else if (array[2] == val) {
        return 2;
    } else if (array[3] == val) {
        return 3;
    } else {
        return 4;
    }
}

static inline unsigned array_search_8_eq(uint8_t val, const uint8_t* array) {
    for (unsigned i = 0; i < 8; i++) {
        if (array[i] == val) {
            return i;
        }
    }
    return 8;
}

unsigned array_search_16_eq(uint8_t val, const uint8_t* array);
unsigned array_search_32_eq(uint8_t val, const uint8_t* array);
unsigned array_search_x32_eq(uint8_t val, const uint8_t* array, int nr);

}
