/*
 * Copyright (C) 2015 ScyllaDB
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

#if defined(__x86_64__) || defined(__i386__)
#include <smmintrin.h>
#else
#include <zlib.h>
#endif

namespace utils {

class crc32 {
    uint32_t _r = 0;
public:
    // All process() functions assume input is in
    // host byte order (i.e. equivalent to storing
    // the value in a buffer and crcing the buffer).
#if defined(__x86_64__) || defined(__i386__)
    // On x86 use the crc32 instruction added in SSE 4.2.
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
#else
    // On non-x86 platforms use the zlib implementation of crc32.
    // TODO: these should be changed to use platform-specific
    // assembly and also the Castagnoli polynomial to match x86.
    template <class T>
    void process(T in) {
        static_assert(std::is_integral<T>::value, "T must be integral type.");
        _r = ::crc32(_r, reinterpret_cast<const uint8_t*>(&in), sizeof(T));
    }
    void process(const uint8_t* in, size_t size) {
        _r = ::crc32(_r, in, size);
    }
#endif
    uint32_t get() const {
        return _r;
    }
};

}
