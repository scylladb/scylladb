/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
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
inline int array_search_gt(int64_t val, const int64_t* array, const int capacity, const int size) noexcept {
#if defined(__x86_64__) || defined(__aarch64__)
    using v4i64 = int64_t __attribute__((vector_size(32)));
    const v4i64 k = v4i64{} + val;
    v4i64 acc = {};
    for (int i = 0; i < capacity; i += 4) {
        v4i64 a;
        __builtin_memcpy(&a, array + i, sizeof(a));
        // Lanes with array[i] > val are -1; unused slots never match
        acc += a > k;
    }
    return size + static_cast<int>(__builtin_reduce_add(acc));
#else
    int i;
    for (i = 0; i < size; i++) {
        if (val < array[i]) {
            break;
        }
    }
    return i;
#endif
}

inline unsigned array_search_4_eq(uint8_t val, const uint8_t* array) {
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

inline unsigned array_search_8_eq(uint8_t val, const uint8_t* array) {
    for (unsigned i = 0; i < 8; i++) {
        if (array[i] == val) {
            return i;
        }
    }
    return 8;
}

#if defined(__x86_64__)

inline unsigned array_search_16_eq(uint8_t val, const uint8_t* arr) noexcept {
    using v16u8 = uint8_t __attribute__((vector_size(16)));
    using v16i8 = char __attribute__((vector_size(16)));
    v16u8 a;
    __builtin_memcpy(&a, arr, sizeof(a));
    unsigned m = __builtin_ia32_pmovmskb128((v16i8)(a == v16u8{} + val));
    return __builtin_ctz(m | 0x10000);
}

// AVX2 only: x86-64-v3 is the build and runtime baseline
inline unsigned array_search_32_eq(uint8_t val, const uint8_t* arr) noexcept {
    using v32u8 = uint8_t __attribute__((vector_size(32)));
    using v32i8 = char __attribute__((vector_size(32)));
    v32u8 a;
    __builtin_memcpy(&a, arr, sizeof(a));
    unsigned m = __builtin_ia32_pmovmskb256((v32i8)(a == v32u8{} + val));
    return __builtin_ctzll(m | 0x100000000ull);
}

#elif defined(__aarch64__)

inline unsigned array_search_16_eq(uint8_t val, const uint8_t* arr) noexcept {
    using v16u8 = uint8_t __attribute__((vector_size(16)));
    using v8u16 = uint16_t __attribute__((vector_size(16)));
    using v8u8 = uint8_t __attribute__((vector_size(8)));
    v16u8 a;
    __builtin_memcpy(&a, arr, sizeof(a));
    // shrn by 4 turns the 0x00/0xff lanes into a 64-bit mask, 4 bits per lane
    v8u8 n = __builtin_convertvector((v8u16)(a == v16u8{} + val) >> 4, v8u8);
    uint64_t m;
    __builtin_memcpy(&m, &n, sizeof(m));
    return m ? __builtin_ctzll(m) / 4 : 16;
}

#else

inline unsigned array_search_16_eq(uint8_t val, const uint8_t* arr) noexcept {
    return std::find(arr, arr + 16, val) - arr;
}

#endif

#ifndef __x86_64__

inline unsigned array_search_32_eq(uint8_t val, const uint8_t* arr) noexcept {
    unsigned i = array_search_16_eq(val, arr);
    return i < 16 ? i : 16 + array_search_16_eq(val, arr + 16);
}

#endif

inline unsigned array_search_x32_eq(uint8_t val, const uint8_t* arr, int nr) noexcept {
    for (unsigned off = 0, len = 32 * nr; off < len; off += 32) {
        unsigned i = array_search_32_eq(val, arr + off);
        if (i < 32) {
            return off + i;
        }
    }
    return 32 * nr;
}

}
