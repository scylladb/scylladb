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
 *
 * A crc32 calculation for __PPC64__ uses the code from https://github.com/antonblanchard/crc32-vpmsum
 * written by Anton Blanchard <anton@au.ibm.com>, IBM
 */

#pragma once

#include <cstdint>
#include <type_traits>
#include <seastar/net/byteorder.hh>

#if defined(__x86_64__) || defined(__i386__)
#include <smmintrin.h>
#else
#include <zlib.h>
#endif

#include "utils/clmul.hh"
#include "utils/fragment_range.hh"

namespace utils {

class crc32 {
    uint32_t _r = 0;
public:
    // All process() functions assume input is in
    // host byte order (i.e. equivalent to storing
    // the value in a buffer and crcing the buffer).
#if defined(__x86_64__) || defined(__i386__)
    // On x86 use the crc32 instruction added in SSE 4.2.
    void process_le(int8_t in) {
        _r = _mm_crc32_u8(_r, in);
    }
    void process_le(uint8_t in) {
        _r = _mm_crc32_u8(_r, in);
    }
    void process_le(int16_t in) {
        _r = _mm_crc32_u16(_r, in);
    }
    void process_le(uint16_t in) {
        _r = _mm_crc32_u16(_r, in);
    }
    void process_le(int32_t in) {
        _r = _mm_crc32_u32(_r, in);
    }
    void process_le(uint32_t in) {
        _r = _mm_crc32_u32(_r, in);
    }
    void process_le(int64_t in) {
        _r = _mm_crc32_u64(_r, in);
    }
    void process_le(uint64_t in) {
        _r = _mm_crc32_u64(_r, in);
    }

    template <typename T>
    void process_be(T in) {
        in = seastar::net::hton(in);
        process_le(in);
    }

    void process(const uint8_t* in, size_t size) {
        if ((reinterpret_cast<uintptr_t>(in) & 1) && size >= 1) {
            process_le(*in);
            ++in;
            --size;
        }
        if ((reinterpret_cast<uintptr_t>(in) & 3) && size >= 2) {
            process_le(*reinterpret_cast<const uint16_t*>(in));
            in += 2;
            size -= 2;
        }
        if ((reinterpret_cast<uintptr_t>(in) & 7) && size >= 4) {
            process_le(*reinterpret_cast<const uint32_t*>(in));
            in += 4;
            size -= 4;
        }
        // FIXME: do in three parallel loops
        while (size >= 8) {
            process_le(*reinterpret_cast<const uint64_t*>(in));
            in += 8;
            size -= 8;
        }
        if (size >= 4) {
            process_le(*reinterpret_cast<const uint32_t*>(in));
            in += 4;
            size -= 4;
        }
        if (size >= 2) {
            process_le(*reinterpret_cast<const uint16_t*>(in));
            in += 2;
            size -= 2;
        }
        if (size >= 1) {
            process_le(*in);
        }
    }
#elif defined(__PPC64__)
    uint32_t crc32_vpmsum(uint32_t crc, const uint8_t* p, size_t len);

    template <class T>
    void process_le(T in) {
        static_assert(std::is_integral<T>::value, "T must be integral type.");
#if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        switch (sizeof(T)) {
        case 2: in = __builtin_bswap16(in); break;
        case 4: in = __builtin_bswap32(in); break;
        case 8: in = __builtin_bswap64(in); break;
        }
#endif
        _r = crc32_vpmsum(_r, reinterpret_cast<const uint8_t*>(&in), sizeof(T));
    }

    template <class T>
    void process_be(T in) {
        static_assert(std::is_integral<T>::value, "T must be integral type.");
        in = seastar::net::hton(in);
        _r = crc32_vpmsum(_r, reinterpret_cast<const uint8_t*>(&in), sizeof(T));
    }

    void process(const uint8_t* in, size_t size) {
        _r = crc32_vpmsum(_r, in, size);
    }
#else
    template <class T>
    void process_le(T in) {
        static_assert(std::is_integral<T>::value, "T must be integral type.");
#if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        switch (sizeof(T)) {
        case 2: in = __builtin_bswap16(in); break;
        case 4: in = __builtin_bswap32(in); break;
        case 8: in = __builtin_bswap64(in); break;
        }
#endif
        _r = ::crc32(_r, reinterpret_cast<const uint8_t*>(&in), sizeof(T));
    }

    template <class T>
    void process_be(T in) {
        static_assert(std::is_integral<T>::value, "T must be integral type.");
        in = seastar::net::hton(in);
        _r = ::crc32(_r, reinterpret_cast<const uint8_t*>(&in), sizeof(T));
    }

    void process(const uint8_t* in, size_t size) {
        _r = ::crc32(_r, in, size);
    }
#endif

    template<typename FragmentedBuffer>
    GCC6_CONCEPT(requires FragmentRange<FragmentedBuffer>)
    void process_fragmented(const FragmentedBuffer& buffer) {
        using boost::range::for_each;
        for_each(buffer, [this] (bytes_view bv) {
            process(reinterpret_cast<const uint8_t*>(bv.data()), bv.size());
        });
    }

    uint32_t get() const {
        return _r;
    }
};

}
