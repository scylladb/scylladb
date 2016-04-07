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
 * Copyright (C) 2015 ScyllaDB
 *
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

#include "murmur_hash.hh"

namespace utils {

namespace murmur_hash {

uint32_t hash32(bytes_view data, uint32_t seed)
{
    uint32_t length = data.size();
    uint32_t m = 0x5bd1e995;
    uint32_t r = 24;

    uint32_t h = seed ^ length;

    uint32_t len_4 = length >> 2;

    for (uint32_t i = 0; i < len_4; i++)
    {
        uint32_t i_4 = i << 2;
        uint32_t k = data[i_4 + 3];
        k = k << 8;
        k = k | (data[i_4 + 2] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 1] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 0] & 0xff);
        k *= m;
        k ^= (uint32_t)k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    // avoid calculating modulo
    uint32_t len_m = len_4 << 2;
    uint32_t left = length - len_m;

    if (left != 0)
    {
        if (left >= 3)
        {
            h ^= (uint32_t) data[length - 3] << 16;
        }
        if (left >= 2)
        {
            h ^= (uint32_t) data[length - 2] << 8;
        }
        if (left >= 1)
        {
            h ^= (uint32_t) data[length - 1];
        }

        h *= m;
    }

    h ^= (uint32_t)h >> 13;
    h *= m;
    h ^= (uint32_t)h >> 15;

    return h;
}

uint64_t hash2_64(bytes_view key, uint64_t seed)
{
    uint32_t length = key.size();
    uint64_t m64 = 0xc6a4a7935bd1e995L;
    uint32_t r64 = 47;

    uint64_t h64 = (seed & 0xffffffffL) ^ (m64 * length);

    uint32_t lenLongs = length >> 3;

    for (uint32_t i = 0; i < lenLongs; ++i)
    {
        uint32_t i_8 = i << 3;

        uint64_t k64 =  ((uint64_t)  key[i_8+0] & 0xff)      + (((uint64_t) key[i_8+1] & 0xff)<<8)  +
                (((uint64_t) key[i_8+2] & 0xff)<<16) + (((uint64_t) key[i_8+3] & 0xff)<<24) +
                (((uint64_t) key[i_8+4] & 0xff)<<32) + (((uint64_t) key[i_8+5] & 0xff)<<40) +
                (((uint64_t) key[i_8+6] & 0xff)<<48) + (((uint64_t) key[i_8+7] & 0xff)<<56);

        k64 *= m64;
        k64 ^= k64 >> r64;
        k64 *= m64;

        h64 ^= k64;
        h64 *= m64;
    }

    uint32_t rem = length & 0x7;

    switch (rem)
    {
    case 0:
        break;
    case 7:
        h64 ^= (uint64_t) key[length - rem + 6] << 48;
    case 6:
        h64 ^= (uint64_t) key[length - rem + 5] << 40;
    case 5:
        h64 ^= (uint64_t) key[length - rem + 4] << 32;
    case 4:
        h64 ^= (uint64_t) key[length - rem + 3] << 24;
    case 3:
        h64 ^= (uint64_t) key[length - rem + 2] << 16;
    case 2:
        h64 ^= (uint64_t) key[length - rem + 1] << 8;
    case 1:
        h64 ^= (uint64_t) key[length - rem];
        h64 *= m64;
    }

    h64 ^= (uint64_t)h64 >> r64;
    h64 *= m64;
    h64 ^= (uint64_t)h64 >> r64;

    return h64;
}

static inline uint64_t getblock(bytes_view key, uint32_t index)
{
    uint32_t i_8 = index << 3;
    auto p = reinterpret_cast<const uint8_t*>(key.data() + i_8);
    return uint64_t(p[0])
            | (uint64_t(p[1]) << 8)
            | (uint64_t(p[2]) << 16)
            | (uint64_t(p[3]) << 24)
            | (uint64_t(p[4]) << 32)
            | (uint64_t(p[5]) << 40)
            | (uint64_t(p[6]) << 48)
            | (uint64_t(p[7]) << 56);
}

void hash3_x64_128(bytes_view key, uint64_t seed, std::array<uint64_t,2> &result)
{
    uint32_t length = key.size();
    const uint32_t nblocks = length >> 4; // Process as 128-bit blocks.

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    uint64_t c1 = 0x87c37b91114253d5L;
    uint64_t c2 = 0x4cf5ad432745937fL;

    //----------
    // body

    for(uint32_t i = 0; i < nblocks; i++)
    {
        uint64_t k1 = getblock(key, i*2+0);
        uint64_t k2 = getblock(key, i*2+1);

        k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;

        h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

        k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

        h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
    }

    //----------
    // tail

    // Advance offset to the unprocessed tail of the data.
    key.remove_prefix(nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch(length & 15)
    {
    case 15: k2 ^= ((uint64_t) key[14]) << 48;
    case 14: k2 ^= ((uint64_t) key[13]) << 40;
    case 13: k2 ^= ((uint64_t) key[12]) << 32;
    case 12: k2 ^= ((uint64_t) key[11]) << 24;
    case 11: k2 ^= ((uint64_t) key[10]) << 16;
    case 10: k2 ^= ((uint64_t) key[9]) << 8;
    case  9: k2 ^= ((uint64_t) key[8]) << 0;
        k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;
    case  8: k1 ^= ((uint64_t) key[7]) << 56;
    case  7: k1 ^= ((uint64_t) key[6]) << 48;
    case  6: k1 ^= ((uint64_t) key[5]) << 40;
    case  5: k1 ^= ((uint64_t) key[4]) << 32;
    case  4: k1 ^= ((uint64_t) key[3]) << 24;
    case  3: k1 ^= ((uint64_t) key[2]) << 16;
    case  2: k1 ^= ((uint64_t) key[1]) << 8;
    case  1: k1 ^= ((uint64_t) key[0]);
        k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= length; h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;
    h2 += h1;

    result[0] = h1;
    result[1] = h2;
}

} // namespace murmur_hash
} // namespace utils
