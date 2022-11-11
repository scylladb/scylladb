/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 */

#pragma once

#include <cstdint>
#include <type_traits>

inline
constexpr uint64_t clmul_u32_constexpr(uint32_t p1, uint32_t p2) {
    uint64_t result = 0;
    for (unsigned i = 0; i < 32; ++i) {
        result ^= (((p1 >> i) & 1) * uint64_t(p2)) << i;
    }
    return result;
}

// returns the low half of the result
inline
constexpr uint64_t clmul_u64_low_constexpr(uint64_t p1, uint64_t p2) {
    uint64_t result = 0;
    for (unsigned i = 0; i < 64; ++i) {
        result ^= (((p1 >> i) & 1) * p2) << i;
    }
    return result;
}

#if defined(__x86_64__) || defined(__i386__)

#include <wmmintrin.h>
#include <smmintrin.h>

// Performs a carry-less multiplication of two integers.
inline
uint64_t clmul_u32(uint32_t p1, uint32_t p2) {
    __m128i p = _mm_set_epi64x(p1, p2);
    p = _mm_clmulepi64_si128(p, p, 0x01);
    return _mm_extract_epi64(p, 0);
}

constexpr
inline
uint64_t clmul(uint32_t p1, uint32_t p2) {
    return std::is_constant_evaluated() ? clmul_u32_constexpr(p1, p2) : clmul_u32(p1, p2);
}

#elif defined(__aarch64__)

#include <arm_neon.h>

// Performs a carry-less multiplication of two integers.
inline
uint64_t clmul_u32(uint32_t p1, uint32_t p2) {
    return vmull_p64(p1, p2);
}

constexpr
inline
uint64_t clmul(uint32_t p1, uint32_t p2) {
    return std::is_constant_evaluated() ? clmul_u32_constexpr(p1, p2) : clmul_u32(p1, p2);
}

#endif
