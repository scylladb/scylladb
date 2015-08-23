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
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "i_filter.hh"
#include "fb_utilities.hh"
#include "bytes.hh"
#include "utils/murmur_hash.hh"
#include "core/shared_ptr.hh"
#include "utils/large_bitset.hh"
#include <array>
#include <cstdlib>
#include "bloom_filter.hh"

namespace utils {
namespace filter {
static thread_local auto reusable_indexes = std::vector<long>();

void bloom_filter::set_indexes(int64_t base, int64_t inc, int count, long max, std::vector<long>& results) {
    for (int i = 0; i < count; i++) {
        results[i] = abs(base % max);
        base = static_cast<int64_t>(static_cast<uint64_t>(base) + static_cast<uint64_t>(inc));
    }
}

std::vector<long> bloom_filter::get_hash_buckets(const bytes_view& key, int hash_count, long max) {
    std::array<uint64_t, 2> h;
    hash(key, 0, h);

    auto indexes = std::vector<long>();

    indexes.resize(hash_count);
    set_indexes(h[0], h[1], hash_count, max, indexes);
    return indexes;
}

std::vector<long> bloom_filter::indexes(const bytes_view& key) {
    // we use the same array both for storing the hash result, and for storing the indexes we return,
    // so that we do not need to allocate two arrays.
    auto& idx = reusable_indexes;
    std::array<uint64_t, 2> h;
    hash(key, 0, h);

    idx.resize(_hash_count);
    set_indexes(h[0], h[1], _hash_count, _bitset.size(), idx);
    return idx;
}

filter_ptr create_filter(int hash, large_bitset&& bitset) {
    return std::make_unique<murmur3_bloom_filter>(hash, std::move(bitset));
}

filter_ptr create_filter(int hash, long num_elements, int buckets_per) {
    long num_bits = (num_elements * buckets_per) + bloom_calculations::EXCESS;
    num_bits = align_up<long>(num_bits, 64);  // Seems to be implied in origin
    large_bitset bitset(num_bits);
    return std::make_unique<murmur3_bloom_filter>(hash, std::move(bitset));
}
}
}
