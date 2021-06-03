/*
 * Copyright (C) 2018-present ScyllaDB
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
 *
 */

#pragma once

#include <cstdint>

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

inline
uint64_t clmul(uint32_t p1, uint32_t p2) {
    return clmul_u32(p1, p2);
}

#elif defined(__aarch64__)

#include <arm_neon.h>

// Performs a carry-less multiplication of two integers.
inline
uint64_t clmul_u32(uint32_t p1, uint32_t p2) {
    return vmull_p64(p1, p2);
}

inline
uint64_t clmul(uint32_t p1, uint32_t p2) {
    return clmul_u32(p1, p2);
}

#endif
