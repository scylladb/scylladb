/*
 * Fast ASCII string validation.
 *
 * Copyright (c) 2018, Arm Limited.
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
