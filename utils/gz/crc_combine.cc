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

/*
 * CRC32(M(x)) = M(x) * x^32 mod G(x)
 *
 * Where M(x) is a polynomial with binary coefficients from an integer field modulo 2.
 *
 *   M(X) = a_0 * x^0 + a_1 * x^1 + ... + a_n * x^N
 *
 *   a_i in { 0, 1 }
 *
 * Since it's a modulo-2 field, addition of coefficients is done by XORing.
 *
 * G(x) is a constant polynomial of degree 32:
 *
 *   G(x) = x^32 + x^26 + x^23 + x^22 + x^16 + x^12 + x^11 + x^10 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1
 *
 * Polynomials are represented in memory as sequences of bits, where each
 * bit represents the coefficient of some x^N term. Bit-reversed form is used,
 * meaning that the coefficients for higher powers are stored in less significant bits:
 *
 *            MSB                           LSB
 *    bit:    31  30   29            2   1   0
 *
 *           | 1 | 0 | 1 |   ...   | 1 | 0 | 1 |
 *
 *    P(x) =  x^0 +   x^2 + ... + x^29   + x^31
 *
 */

#include "crc_combine.hh"
#include "crc_combine_table.hh"
#include "utils/clmul.hh"

using u32 = uint32_t;
using u64 = uint64_t;

#if defined(__x86_64__) || defined(__i386__) || defined(__aarch64__)

#include "barett.hh"

/*
 * Calculates:
 *
 *   P1(x) * P2(x)
 */
static
u64 pmul(u32 p1, u32 p2) {
    // Because the representation is bit-reversed, we need to shift left
    // one bit after multiplying so that the highest bit of the 64-bit result
    // corresponds to the coefficient of x^63 and not x^62.
    return clmul(p1, p2) << 1;
}

/*
 * Calculates:
 *
 *   P1(x) * P2(x) mod G(x)
 */
static
u32 pmul_mod(u32 p1, u32 p2) {
    return crc32_fold_barett_u64(pmul(p1, p2));
}

/*
 * Calculates:
 *
 *   R(x) = P(x) * x^(e*8) mod G(x)
 *
 */
static
u32 mul_by_x_pow_mul8(u32 p, u64 e) {
    /*
     * Let's expand e as a series with coefficients corresponding to the binary form:
     *
     *   e = e_0 * 2^0 + e_1 * 2^1 + ... + e_(n-1) * 2^(n-1) + e_n * 2^n
     *
     * for short:
     *
     *   e = SUM(e_i * 2^i)
     *   i in 0..63
     *
     * then:
     *
     *   R(X) = P(x) * x^(e*8) mod G(x)
     *        = P(x) * x^(SUM(e_i * 2^i)*8) mod G(x)
     *        = P(x) * x^(SUM(e_i * 2^(i+3)))) mod G(x)
     *        = P(x) * MUL(x^(e_i * 2^(i+3)))) mod G(x)
     *
     * To simplify notation:
     *
     *   t_i(x, u) = x^(u * 2^(i+3))
     *
     * then:
     *
     *   R(X) = P(x) * MUL(t_i(x, e_i)) mod G(x)
     *
     * One observation is that we could apply modulo G(x) to each of the t_i term without
     * changing the result.
     *
     * Also, each t_i can have 2 possible values:
     *
     *   t_i(x, u) = { x^(2^(i+3)) mod G(x) for u = 1,
     *                                    1 for u = 0 }
     *
     * So t_i(x, u) is a polynomial of degree up to 31 and its 2 possible values could be precomputed.
     *
     * There are 64 t_i terms, selected for multiplication by the bits of e. That means we have to
     * do up to 64 multiplications. Each multiplication doubles the order of the polynomial and
     * multiplication is natively supported only for up to degree 64, so we would have to also
     * fold (apply the modulo operator) along the way.
     *
     * We can reduce the number of multiplications by grouping the terms, e.g. by 8:
     *
     *   R(X) = P(x) * (t_0(x, e_0) * t_1(x, e_1) * ... * t_7(x, e_7) mod G(x))
     *               * (t_8(x, e_8) * t_9(x, e_9) * ... * t_15(x, e_15) mod G(x))
     *               ...
     *               * (t_56(x, e_56) * t_57(x, e_57) * ... * t_63(x, e_63) mod G(x)) mod G(x)
     *
     * Each group multiplies 8 t_i terms and can have 256 possible values depending on 8 bits of e.
     * The trick is to pre-compute the values. They are stored in crc32_x_pow_radix_8_table_base_{0,8,16,24}.
     * We can also observe that t_(i+32)(x, u) = t_i(x, u), so we actually need to compute values for
     * only the first 4 groups.
     *
     * When we know that all bits in e relevant for given group are 0, that group will evaluate to 1
     * and thus doesn't participate in the end result. So for small values of e we only have to multiply
     * lower-order groups.
     */

    u32 x0 = crc32_x_pow_radix_8_table_base_0[e & 0xff];

    if (__builtin_expect(e < 0x100, false)) {
        return pmul_mod(p, x0);
    }

    u32 x1 = crc32_x_pow_radix_8_table_base_8[(e >> 8) & 0xff];
    u32 x2 = crc32_x_pow_radix_8_table_base_16[(e >> 16) & 0xff];

    u64 y0 = pmul(p, x0);
    u64 y1 = pmul(x1, x2);
    u32 z0 = crc32_fold_barett_u64(y0);
    u32 z1 = crc32_fold_barett_u64(y1);

    if (__builtin_expect(e < 0x1000000, true)) {
        return pmul_mod(z0, z1);
    }

    u32 x3 = crc32_x_pow_radix_8_table_base_24[(e >> 24) & 0xff];
    z1 = pmul_mod(z1, x3);

    if (__builtin_expect(e < 0x100000000, true)) {
        return pmul_mod(z0, z1);
    }

    u32 x4 = crc32_x_pow_radix_8_table_base_0[(e >> 32) & 0xff];
    u32 x5 = crc32_x_pow_radix_8_table_base_8[(e >> 40) & 0xff];
    u32 x6 = crc32_x_pow_radix_8_table_base_16[(e >> 48) & 0xff];
    u32 x7 = crc32_x_pow_radix_8_table_base_24[(e >> 56) & 0xff];

    u64 y2 = pmul(x4, x5);
    u64 y3 = pmul(x6, x7);
    u64 u0 = pmul(z0, z1);

    u32 z2 = crc32_fold_barett_u64(y2);
    u32 z3 = crc32_fold_barett_u64(y3);

    u64 u1 = pmul(z2, z3);
    u32 v0 = crc32_fold_barett_u64(u0);
    u32 v1 = crc32_fold_barett_u64(u1);

    return pmul_mod(v0, v1);
}

u32 fast_crc32_combine(u32 crc, u32 crc2, ssize_t len2) {
    if (len2 == 0) {
        return crc;
    }
    /*
     * We need to calculate:
     *
     *   CRC(A(x) * x^|B(x)| + B(x))
     *
     * given:
     *
     *   CRC(A(x)) = crc
     *   CRC(B(x)) = crc2
     *   |B(x)|    = len2 * 8
     *
     *   CRC(A(x)) = A(x) * x^32 mod G(x)
     *   CRC(B(x)) = B(x) * x^32 mod G(x)
     *
     * CRC(A(x) * x^|B(x)| + B(x))
     *   = (A(x) * x^|B(x)| + B(x)) * x^32 mod G(x)
     *   = (A(x) * x^32 mod G(x) * x^|B(x)|) + (B(x) * x^32 mod G(x)) mod G(x)
     *   = (crc * x^(len2 * 8)) + crc2 mod G(x)
     *
     */
    return mul_by_x_pow_mul8(crc, len2) ^ crc2;
}

#else

// FIXME: Optimize for other archs
// That boils down to implementing crc32_fold_barett_u64() and clmul()
// and reusing the algorithm above. For now, delegate to zlib.

#include <zlib.h>

u32 fast_crc32_combine(u32 crc, u32 crc2, ssize_t len2) {
    return crc32_combine(crc, crc2, len2);
}

#endif
