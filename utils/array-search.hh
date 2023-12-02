/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
 * To accommodate the single-instruction-multiple-data variant, observe
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
