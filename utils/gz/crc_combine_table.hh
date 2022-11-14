/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 */

#pragma once

#include <cstdint>
#include <array>

/*
 * Let t_i be the following polynomial depending on i and u:
 *
 *   t_i(x, u) = x^(u * 2^(i+3))
 *
 * where:
 *
 *   u in { 0, 1 }
 *
 * Let g_k be a multiplication modulo G(x) of t_i(x) for 8 consecutive values of i and 8 values of u (u0 ... u7):
 *
 *   g_k(x, u0, u1, ..., u7) = t_(k+0)(x, u_0) * t_(k+1)(x, u_1) * ... * t_(k+7)(x, u_7) mod G(x)
 *
 * The tables below contain representations of g_k(x) polynomials, where the bits of the index
 * correspond to coefficients of u:
 *
 * crc32_x_pow_radix_8_table_base_<k>[u] = g_k(x, (u >> 0) & 1,
 *                                                (u >> 1) & 1,
 *                                                (u >> 2) & 1,
 *                                                (u >> 3) & 1,
 *                                                (u >> 4) & 1,
 *                                                (u >> 5) & 1,
 *                                                (u >> 6) & 1,
 *                                                (u >> 7) & 1)
 */
extern std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_0;
extern std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_8;
extern std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_16;
extern std::array<uint32_t, 256> crc32_x_pow_radix_8_table_base_24;
