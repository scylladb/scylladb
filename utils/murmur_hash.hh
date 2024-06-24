/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <cstdint>
#include <array>

#include "bytes.hh"

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ (Murmur Hash 2) and
 * https://code.google.com/p/smhasher/wiki/MurmurHash3.
 *
 * This code is not based on the original Murmur Hash C code, but rather
 * a translation of Cassandra's Java version back to C.
 **/

namespace utils {

namespace murmur_hash {

uint32_t hash32(bytes_view data, int32_t seed);
uint64_t hash2_64(bytes_view key, uint64_t seed);

template<typename InputIterator>
inline
uint64_t read_block(InputIterator& in) {
    typename std::iterator_traits<InputIterator>::value_type tmp[8];
    for (int i = 0; i < 8; ++i) {
        tmp[i] = *in;
        ++in;
    }
    return ((uint64_t) tmp[0] & 0xff) + (((uint64_t) tmp[1] & 0xff) << 8) +
           (((uint64_t) tmp[2] & 0xff) << 16) + (((uint64_t) tmp[3] & 0xff) << 24) +
           (((uint64_t) tmp[4] & 0xff) << 32) + (((uint64_t) tmp[5] & 0xff) << 40) +
           (((uint64_t) tmp[6] & 0xff) << 48) + (((uint64_t) tmp[7] & 0xff) << 56);
}

static inline
uint64_t fmix(uint64_t k) {
    k ^= (uint64_t)k >> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= (uint64_t)k >> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= (uint64_t)k >> 33;

    return k;
}

template <typename InputIterator>
void hash3_x64_128(InputIterator in, uint32_t length, uint64_t seed, std::array<uint64_t, 2>& result) {
    const uint32_t nblocks = length >> 4; // Process as 128-bit blocks.

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    uint64_t c1 = 0x87c37b91114253d5L;
    uint64_t c2 = 0x4cf5ad432745937fL;

    //----------
    // body

    for(uint32_t i = 0; i < nblocks; i++)
    {
        uint64_t k1 = read_block(in);
        uint64_t k2 = read_block(in);

        k1 *= c1; k1 = std::rotl(k1,31); k1 *= c2; h1 ^= k1;

        h1 = std::rotl(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

        k2 *= c2; k2  = std::rotl(k2,33); k2 *= c1; h2 ^= k2;

        h2 = std::rotl(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
    }

    //----------
    // tail

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    typename std::iterator_traits<InputIterator>::value_type tmp[15];
    std::copy_n(in, length & 15, tmp);

    switch (length & 15)
    {
        case 15: k2 ^= ((uint64_t) tmp[14]) << 48;
            [[fallthrough]];
        case 14: k2 ^= ((uint64_t) tmp[13]) << 40;
            [[fallthrough]];
        case 13: k2 ^= ((uint64_t) tmp[12]) << 32;
            [[fallthrough]];
        case 12: k2 ^= ((uint64_t) tmp[11]) << 24;
            [[fallthrough]];
        case 11: k2 ^= ((uint64_t) tmp[10]) << 16;
            [[fallthrough]];
        case 10: k2 ^= ((uint64_t) tmp[9]) << 8;
            [[fallthrough]];
        case  9: k2 ^= ((uint64_t) tmp[8]) << 0;
            k2 *= c2; k2  = std::rotl(k2,33); k2 *= c1; h2 ^= k2;
            [[fallthrough]];
        case  8: k1 ^= ((uint64_t) tmp[7]) << 56;
            [[fallthrough]];
        case  7: k1 ^= ((uint64_t) tmp[6]) << 48;
            [[fallthrough]];
        case  6: k1 ^= ((uint64_t) tmp[5]) << 40;
            [[fallthrough]];
        case  5: k1 ^= ((uint64_t) tmp[4]) << 32;
            [[fallthrough]];
        case  4: k1 ^= ((uint64_t) tmp[3]) << 24;
            [[fallthrough]];
        case  3: k1 ^= ((uint64_t) tmp[2]) << 16;
            [[fallthrough]];
        case  2: k1 ^= ((uint64_t) tmp[1]) << 8;
            [[fallthrough]];
        case  1: k1 ^= ((uint64_t) tmp[0]);
            k1 *= c1; k1  = std::rotl(k1,31); k1 *= c2; h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;
    h2 += h1;

    result[0] = h1;
    result[1] = h2;
}

void hash3_x64_128(bytes_view key, uint64_t seed, std::array<uint64_t, 2>& result);

} // namespace murmur_hash

} // namespace utils
