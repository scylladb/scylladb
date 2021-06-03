/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
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
uint64_t rotl64(uint64_t v, uint32_t n) {
    return ((v << n) | ((uint64_t)v >> (64 - n)));
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

        k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;

        h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

        k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

        h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
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
        case 14: k2 ^= ((uint64_t) tmp[13]) << 40;
        case 13: k2 ^= ((uint64_t) tmp[12]) << 32;
        case 12: k2 ^= ((uint64_t) tmp[11]) << 24;
        case 11: k2 ^= ((uint64_t) tmp[10]) << 16;
        case 10: k2 ^= ((uint64_t) tmp[9]) << 8;
        case  9: k2 ^= ((uint64_t) tmp[8]) << 0;
            k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;
        case  8: k1 ^= ((uint64_t) tmp[7]) << 56;
        case  7: k1 ^= ((uint64_t) tmp[6]) << 48;
        case  6: k1 ^= ((uint64_t) tmp[5]) << 40;
        case  5: k1 ^= ((uint64_t) tmp[4]) << 32;
        case  4: k1 ^= ((uint64_t) tmp[3]) << 24;
        case  3: k1 ^= ((uint64_t) tmp[2]) << 16;
        case  2: k1 ^= ((uint64_t) tmp[1]) << 8;
        case  1: k1 ^= ((uint64_t) tmp[0]);
            k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
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
