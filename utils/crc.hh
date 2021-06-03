/*
 * Copyright (C) 2015-present ScyllaDB
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
#include <seastar/core/byteorder.hh>

#include <boost/range/algorithm/for_each.hpp>

#if defined(__x86_64__) || defined(__i386__)
#include <smmintrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
/* Implement x86-64 intrinsics with according aarch64 ones */
static inline uint32_t _mm_crc32_u8(uint32_t crc, uint8_t in)
{
    return __crc32cb(crc, in);
}
static inline uint32_t _mm_crc32_u16(uint32_t crc, uint16_t in)
{
    return __crc32ch(crc, in);
}
static inline uint32_t _mm_crc32_u32(uint32_t crc, uint32_t in)
{
    return __crc32cw(crc, in);
}
static inline uint32_t _mm_crc32_u64(uint32_t crc, uint64_t in)
{
    return __crc32cd(crc, in);
}
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
#if defined(__x86_64__) || defined(__i386__) || defined(__aarch64__)
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
            process_le(seastar::read_le<uint16_t>(reinterpret_cast<const char*>(in)));
            in += 2;
            size -= 2;
        }
        if ((reinterpret_cast<uintptr_t>(in) & 7) && size >= 4) {
            process_le(seastar::read_le<uint32_t>(reinterpret_cast<const char*>(in)));
            in += 4;
            size -= 4;
        }

        // do in three parallel loops
        while (size >= 1024) {
            uint32_t crc0 = _r, crc1 = 0, crc2 = 0;

            // calculate three blocks in parallel
            // - crc0: in64[ 0,  1, ...,  41]
            // - crc1: in64[42, 43, ...,  83]
            // - crc2: in64[84, 85, ..., 125]
            for (int i = 0; i < 42; ++i, in += 8) {
                crc0 = _mm_crc32_u64(crc0, seastar::read_le<uint64_t>((const char*)in));
                crc1 = _mm_crc32_u64(crc1, seastar::read_le<uint64_t>((const char*)in + 42*8));
                crc2 = _mm_crc32_u64(crc2, seastar::read_le<uint64_t>((const char*)in + 42*2*8));
            }
            in += 42*2*8;

            // combine three blocks' crc and last two u64
            // - CRC32(crc0 * CRC32(x^(42*64*2)))
            crc0 = _mm_crc32_u64(0, clmul_u32(crc0, 0xe417f38a));
            // - CRC32(crc1 * CRC32(x^(42*64)))
            crc1 = _mm_crc32_u64(0, clmul_u32(crc1, 0x8f158014));
            // - CRC32(crc2 * x^32 + u64[-2])
            crc2 = _mm_crc32_u64(crc2, seastar::read_le<uint64_t>((const char*)in));
            in += 8;
            // - Last u64
            _r = _mm_crc32_u64(crc0^crc1^crc2, seastar::read_le<uint64_t>((const char*)in));
            in += 8;

            size -= 1024;
        }

        while (size >= 8) {
            process_le(seastar::read_le<uint64_t>(reinterpret_cast<const char*>(in)));
            in += 8;
            size -= 8;
        }
        if (size >= 4) {
            process_le(seastar::read_le<uint32_t>(reinterpret_cast<const char*>(in)));
            in += 4;
            size -= 4;
        }
        if (size >= 2) {
            process_le(seastar::read_le<uint16_t>(reinterpret_cast<const char*>(in)));
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
        case 1: break;
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
        case 1: break;
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
    requires FragmentRange<FragmentedBuffer>
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
