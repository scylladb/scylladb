/*
 * Extracted from libdeflate/lib/x86/crc32_pclmul_template.h
 * with minor modifications.
 *
 * Copyright 2016 Eric Biggers
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

#include <cstdint>
#include "utils/clmul.hh"

inline
constexpr uint64_t barrett_reduction_constants[2] = { 0x00000001F7011641, 0x00000001DB710641 };

/*
 * Calculates representation of p(x) mod G(x) using Barrett reduction.
 *
 * p(x) is a polynomial of degree 64.
 *
 * The parameter p is a bit-reversed representation of the polynomial,
 * the least significant bit corresponds to the coefficient of x^63.
 */
inline constexpr uint32_t crc32_fold_barrett_u64_constexpr(uint64_t p) {
    auto x0 = p;
    auto x1 = x0;
    uint64_t mask32 = 0xffff'ffff;
    x0 = clmul_u64_low_constexpr(x0 & mask32, barrett_reduction_constants[0]);
    x0 = clmul_u64_low_constexpr(x0 & mask32, barrett_reduction_constants[1]);
    return (x0 ^ x1) >> 32;
}

#if defined(__x86_64__) || defined(__i386__)

#include <wmmintrin.h>

inline
uint32_t crc32_fold_barrett_u64_in_m128(__m128i x0) {
    __m128i x1;
    const __m128i mask32 = (__m128i)(__v4si){ int32_t(0xFFFFFFFF) };
    const __v2di brc =
        (__v2di){ barrett_reduction_constants[0], barrett_reduction_constants[1] };

    /*
     * Reduce 64 => 32 bits using Barrett reduction.
     *
     * Let M(x) = A(x)*x^32 + B(x) be the remaining message.  The goal is to
     * compute R(x) = M(x) mod G(x).  Since degree(B(x)) < degree(G(x)):
     *
     *	R(x) = (A(x)*x^32 + B(x)) mod G(x)
     *	     = (A(x)*x^32) mod G(x) + B(x)
     *
     * Then, by the Division Algorithm there exists a unique q(x) such that:
     *
     *	A(x)*x^32 mod G(x) = A(x)*x^32 - q(x)*G(x)
     *
     * Since the left-hand side is of maximum degree 31, the right-hand side
     * must be too.  This implies that we can apply 'mod x^32' to the
     * right-hand side without changing its value:
     *
     *	(A(x)*x^32 - q(x)*G(x)) mod x^32 = q(x)*G(x) mod x^32
     *
     * Note that '+' is equivalent to '-' in polynomials over GF(2).
     *
     * We also know that:
     *
     *	              / A(x)*x^32 \
     *	q(x) = floor (  ---------  )
     *	              \    G(x)   /
     *
     * To compute this efficiently, we can multiply the top and bottom by
     * x^32 and move the division by G(x) to the top:
     *
     *	              / A(x) * floor(x^64 / G(x)) \
     *	q(x) = floor (  -------------------------  )
     *	              \           x^32            /
     *
     * Note that floor(x^64 / G(x)) is a constant.
     *
     * So finally we have:
     *
     *	                          / A(x) * floor(x^64 / G(x)) \
     *	R(x) = B(x) + G(x)*floor (  -------------------------  )
     *	                          \           x^32            /
     *
     */
    x1 = x0;
    x0 = _mm_clmulepi64_si128(x0 & mask32, brc, 0x00);
    x0 = _mm_clmulepi64_si128(x0 & mask32, brc, 0x10);
    return _mm_cvtsi128_si32(_mm_srli_si128(x0 ^ x1, 4));
}

inline
uint32_t crc32_fold_barrett_u64_native(uint64_t p) {
    return crc32_fold_barrett_u64_in_m128(_mm_set_epi64x(0, p));
}

#elif defined(__aarch64__)

#include <arm_neon.h>

inline
uint32_t crc32_fold_barrett_u64_in_u64x2(uint64x2_t x0) {
    uint64x2_t x1;
    const uint64_t barrett_reduction_constant_lo = barrett_reduction_constants[0];
    const uint64_t barrett_reduction_constant_hi = barrett_reduction_constants[1];

    x1 = x0;
    x0 = vreinterpretq_u64_p128(
            vmull_p64(vgetq_lane_u32(vreinterpretq_u32_u64(x0), 0),
                      barrett_reduction_constant_lo));
    x0 = vreinterpretq_u64_p128(
            vmull_p64(vgetq_lane_u32(vreinterpretq_u32_u64(x0), 0),
                      barrett_reduction_constant_hi));
    return vgetq_lane_u64(vshrq_n_u64(x0 ^ x1, 32), 0);
}

inline
uint32_t crc32_fold_barrett_u64_native(uint64_t p) {
    return crc32_fold_barrett_u64_in_u64x2(
            vcombine_u64((uint64x1_t)p, (uint64x1_t)0UL));
}

#else

#error "Not implemented for this arch"

#endif

inline
constexpr
uint32_t crc32_fold_barrett_u64(uint64_t p) {
    return std::is_constant_evaluated() ? crc32_fold_barrett_u64_constexpr(p) : crc32_fold_barrett_u64_native(p);
}
