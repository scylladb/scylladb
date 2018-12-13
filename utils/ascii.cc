/*
 * Fast ASCII string validataion.
 *
 * Copyright (c) 2018, Arm Limited.
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

#include "ascii.hh"
#include <seastar/core/byteorder.hh>

namespace utils {

namespace ascii {

bool validate(const uint8_t *data, size_t len) {
    // OR all bytes
    uint8_t orall = 0;

    // Fast OR by 8-bytes and two independent streams
    if (len >= 16) {
        uint64_t or1 = 0, or2 = 0;

        do {
            or1 |= seastar::read_le<uint64_t>((const char *)data);
            or2 |= seastar::read_le<uint64_t>((const char *)data+8);

            data += 16;
            len -= 16;
        } while (len >= 16);

        // Idea from Benny Halevy <bhalevy@scylladb.com>
        // - 7-th bit set   ==> orall = !(non-zero) - 1 = 0 - 1 = 0xFF
        // - 7-th bit clear ==> orall = !0 - 1          = 1 - 1 = 0x00
        orall = !((or1 | or2) & 0x8080808080808080ULL) - 1;
    }

    // OR remaining bytes
    while (len--) {
        orall |= *data++;
    }

    // 7-th bit should be 0
    return orall < 0x80;
}

} // namespace ascii

} // namespace utils
