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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 *
 */

#include "murmur_hash.hh"

namespace util {

namespace murmur_hash {

int32_t hash32(const bytes &data, int32_t offset, int32_t length, int32_t seed)
{
    int32_t m = 0x5bd1e995;
    int32_t r = 24;

    int32_t h = seed ^ length;

    int32_t len_4 = length >> 2;

    for (int32_t i = 0; i < len_4; i++)
    {
        int32_t i_4 = i << 2;
        int32_t k = data[offset + i_4 + 3];
        k = k << 8;
        k = k | (data[offset + i_4 + 2] & 0xff);
        k = k << 8;
        k = k | (data[offset + i_4 + 1] & 0xff);
        k = k << 8;
        k = k | (data[offset + i_4 + 0] & 0xff);
        k *= m;
        k ^= (uint32_t)k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    // avoid calculating modulo
    int32_t len_m = len_4 << 2;
    int32_t left = length - len_m;

    if (left != 0)
    {
        if (left >= 3)
        {
            h ^= (int32_t) data[offset + length - 3] << 16;
        }
        if (left >= 2)
        {
            h ^= (int32_t) data[offset + length - 2] << 8;
        }
        if (left >= 1)
        {
            h ^= (int32_t) data[offset + length - 1];
        }

        h *= m;
    }

    h ^= (uint32_t)h >> 13;
    h *= m;
    h ^= (uint32_t)h >> 15;

    return h;
}

int64_t hash2_64(const bytes &key, int32_t offset, int32_t length, int64_t seed)
{
    int64_t m64 = 0xc6a4a7935bd1e995L;
    int32_t r64 = 47;

    int64_t h64 = (seed & 0xffffffffL) ^ (m64 * length);

    int32_t lenLongs = length >> 3;

    for (int32_t i = 0; i < lenLongs; ++i)
    {
        int32_t i_8 = i << 3;

        int64_t k64 =  ((int64_t)  key[offset+i_8+0] & 0xff)      + (((int64_t) key[offset+i_8+1] & 0xff)<<8)  +
                (((int64_t) key[offset+i_8+2] & 0xff)<<16) + (((int64_t) key[offset+i_8+3] & 0xff)<<24) +
                (((int64_t) key[offset+i_8+4] & 0xff)<<32) + (((int64_t) key[offset+i_8+5] & 0xff)<<40) +
                (((int64_t) key[offset+i_8+6] & 0xff)<<48) + (((int64_t) key[offset+i_8+7] & 0xff)<<56);

        k64 *= m64;
        k64 ^= (uint64_t)k64 >> r64;
        k64 *= m64;

        h64 ^= k64;
        h64 *= m64;
    }

    int32_t rem = length & 0x7;

    switch (rem)
    {
    case 0:
        break;
    case 7:
        h64 ^= (int64_t) key[offset + length - rem + 6] << 48;
    case 6:
        h64 ^= (int64_t) key[offset + length - rem + 5] << 40;
    case 5:
        h64 ^= (int64_t) key[offset + length - rem + 4] << 32;
    case 4:
        h64 ^= (int64_t) key[offset + length - rem + 3] << 24;
    case 3:
        h64 ^= (int64_t) key[offset + length - rem + 2] << 16;
    case 2:
        h64 ^= (int64_t) key[offset + length - rem + 1] << 8;
    case 1:
        h64 ^= (int64_t) key[offset + length - rem];
        h64 *= m64;
    }

    h64 ^= (uint64_t)h64 >> r64;
    h64 *= m64;
    h64 ^= (uint64_t)h64 >> r64;

    return h64;
}

static int64_t getblock(const bytes &key, int32_t offset, int32_t index)
{
    int32_t i_8 = index << 3;
    int32_t blockOffset = offset + i_8;
    return ((int64_t) key[blockOffset + 0] & 0xff) + (((int64_t) key[blockOffset + 1] & 0xff) << 8) +
            (((int64_t) key[blockOffset + 2] & 0xff) << 16) + (((int64_t) key[blockOffset + 3] & 0xff) << 24) +
            (((int64_t) key[blockOffset + 4] & 0xff) << 32) + (((int64_t) key[blockOffset + 5] & 0xff) << 40) +
            (((int64_t) key[blockOffset + 6] & 0xff) << 48) + (((int64_t) key[blockOffset + 7] & 0xff) << 56);
}

static int64_t rotl64(int64_t v, int32_t n)
{
    return ((v << n) | ((uint64_t)v >> (64 - n)));
}

static int64_t fmix(int64_t k)
{
    k ^= (uint64_t)k >> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= (uint64_t)k >> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= (uint64_t)k >> 33;

    return k;
}

void hash3_x64_128(const bytes &key, int32_t offset, int32_t length, int64_t seed, std::array<int64_t,2> &result)
{
    const int32_t nblocks = length >> 4; // Process as 128-bit blocks.

    int64_t h1 = seed;
    int64_t h2 = seed;

    int64_t c1 = 0x87c37b91114253d5L;
    int64_t c2 = 0x4cf5ad432745937fL;

    //----------
    // body

    for(int32_t i = 0; i < nblocks; i++)
    {
        int64_t k1 = getblock(key, offset, i*2+0);
        int64_t k2 = getblock(key, offset, i*2+1);

        k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;

        h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

        k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

        h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
    }

    //----------
    // tail

    // Advance offset to the unprocessed tail of the data.
    offset += nblocks * 16;

    int64_t k1 = 0;
    int64_t k2 = 0;

    switch(length & 15)
    {
    case 15: k2 ^= ((int64_t) key[offset+14]) << 48;
    case 14: k2 ^= ((int64_t) key[offset+13]) << 40;
    case 13: k2 ^= ((int64_t) key[offset+12]) << 32;
    case 12: k2 ^= ((int64_t) key[offset+11]) << 24;
    case 11: k2 ^= ((int64_t) key[offset+10]) << 16;
    case 10: k2 ^= ((int64_t) key[offset+9]) << 8;
    case  9: k2 ^= ((int64_t) key[offset+8]) << 0;
        k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;
    case  8: k1 ^= ((int64_t) key[offset+7]) << 56;
    case  7: k1 ^= ((int64_t) key[offset+6]) << 48;
    case  6: k1 ^= ((int64_t) key[offset+5]) << 40;
    case  5: k1 ^= ((int64_t) key[offset+4]) << 32;
    case  4: k1 ^= ((int64_t) key[offset+3]) << 24;
    case  3: k1 ^= ((int64_t) key[offset+2]) << 16;
    case  2: k1 ^= ((int64_t) key[offset+1]) << 8;
    case  1: k1 ^= ((int64_t) key[offset]);
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
