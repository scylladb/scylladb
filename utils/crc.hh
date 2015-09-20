/*
 * Copyright 2015 Cloudius Systems
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

#include <cstdint>
#include <smmintrin.h>

class crc32 {
    uint32_t _r = 0;
public:
    // All process() functions assume input is in
    // host byte order (i.e. equivalent to storing
    // the value in a buffer and crcing the buffer).
    void process(int8_t in) {
        _r = _mm_crc32_u8(_r, in);
    }
    void process(uint8_t in) {
        _r = _mm_crc32_u8(_r, in);
    }
    void process(int16_t in) {
        _r = _mm_crc32_u16(_r, in);
    }
    void process(uint16_t in) {
        _r = _mm_crc32_u16(_r, in);
    }
    void process(int32_t in) {
        _r = _mm_crc32_u32(_r, in);
    }
    void process(uint32_t in) {
        _r = _mm_crc32_u32(_r, in);
    }
    void process(int64_t in) {
        _r = _mm_crc32_u64(_r, in);
    }
    void process(uint64_t in) {
        _r = _mm_crc32_u64(_r, in);
    }
    void process(const uint8_t* in, size_t size) {
        if ((reinterpret_cast<uintptr_t>(in) & 1) && size >= 1) {
            process(*in);
            ++in;
            --size;
        }
        if ((reinterpret_cast<uintptr_t>(in) & 3) && size >= 2) {
            process(*reinterpret_cast<const uint16_t*>(in));
            in += 2;
            size -= 2;
        }
        if ((reinterpret_cast<uintptr_t>(in) & 7) && size >= 4) {
            process(*reinterpret_cast<const uint32_t*>(in));
            in += 4;
            size -= 4;
        }
        // FIXME: do in three parallel loops
        while (size >= 8) {
            process(*reinterpret_cast<const uint64_t*>(in));
            in += 8;
            size -= 8;
        }
        if (size >= 4) {
            process(*reinterpret_cast<const uint32_t*>(in));
            in += 4;
            size -= 4;
        }
        if (size >= 2) {
            process(*reinterpret_cast<const uint16_t*>(in));
            in += 2;
            size -= 2;
        }
        if (size >= 1) {
            process(*in);
        }

    }
    uint32_t get() const {
        return _r;
    }
};
