/*
 * Copyright (C) 2018 ScyllaDB
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
 */

#pragma once

#include <zlib.h>
#include <seastar/util/gcc6-concepts.hh>

GCC6_CONCEPT(
template<typename Checksum>
concept bool ChecksumUtils = requires(const char* input, size_t size, uint32_t checksum) {
    { Checksum::init_checksum() } -> uint32_t;
    { Checksum::checksum(input, size) } -> uint32_t;
    { Checksum::checksum(checksum, input, size) } -> uint32_t;
    { Checksum::checksum_combine(checksum, checksum, size) } -> uint32_t;
};
)

struct adler32_utils {
    inline static uint32_t init_checksum() {
        return adler32(0, Z_NULL, 0);
    }

    inline static uint32_t checksum(const char* input, size_t input_len) {
        auto init = adler32(0, Z_NULL, 0);
        return checksum(init, input, input_len);
    }

    inline static uint32_t checksum(uint32_t prev, const char* input, size_t input_len) {
        // yuck, zlib uses unsigned char while we use char :-(
        return adler32(prev, reinterpret_cast<const unsigned char *>(input),
                input_len);
    }

    inline static uint32_t checksum_combine(uint32_t first, uint32_t second, size_t input_len2) {
        return adler32_combine(first, second, input_len2);
    }
};

struct crc32_utils {
    inline static uint32_t init_checksum() {
        return crc32(0, Z_NULL, 0);
    }

    inline static uint32_t checksum(const char* input, size_t input_len) {
        auto init = crc32(0, Z_NULL, 0);
        return checksum(init, input, input_len);
    }

    inline static uint32_t checksum(uint32_t prev, const char* input, size_t input_len) {
        // yuck, zlib uses unsigned char while we use char :-(
        return crc32(prev, reinterpret_cast<const unsigned char *>(input),
                input_len);
    }

    inline static uint32_t checksum_combine(uint32_t first, uint32_t second, size_t input_len2) {
        return crc32_combine(first, second, input_len2);
    }
};

