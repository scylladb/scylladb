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
extern uint32_t crc32_x_pow_radix_8_table_base_0[256];
extern uint32_t crc32_x_pow_radix_8_table_base_8[256];
extern uint32_t crc32_x_pow_radix_8_table_base_16[256];
extern uint32_t crc32_x_pow_radix_8_table_base_24[256];
