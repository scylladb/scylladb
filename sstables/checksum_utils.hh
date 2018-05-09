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

inline uint32_t init_checksum_adler32() {
    return adler32(0, Z_NULL, 0);
}

inline uint32_t checksum_adler32(const char* input, size_t input_len) {
    auto init = adler32(0, Z_NULL, 0);
    // yuck, zlib uses unsigned char while we use char :-(
    return adler32(init, reinterpret_cast<const unsigned char *>(input),
            input_len);
}

inline uint32_t checksum_adler32(uint32_t adler, const char* input, size_t input_len) {
    return adler32(adler, reinterpret_cast<const unsigned char *>(input),
            input_len);
}

inline uint32_t checksum_adler32_combine(uint32_t adler1, uint32_t adler2, size_t input_len2) {
    return adler32_combine(adler1, adler2, input_len2);
}

