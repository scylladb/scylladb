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
#include <sys/types.h>
#include <memory>

/*
 * Computes CRC32 (gzip format, RFC 1952) of a compound bitstream M composed by
 * concatenating bitstream A (first) and bitstream B (second), where:
 *
 *  - crc  is the CRC32 of bitstream A
 *  - crc2 is the CRC32 of bitstream B
 *  - len2 is length of bitstream B in bytes
 */
uint32_t fast_crc32_combine(uint32_t crc, uint32_t crc2, ssize_t len2);

static constexpr bool fast_crc32_combine_optimized() {
#if defined(__x86_64__) || defined(__i386__) || defined(__aarch64__)
    return true;
#else
    return false;
#endif
}
