/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
