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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "i_filter.hh"
#include "fb_utilities.hh"
#include "bytes.hh"
#include "utils/murmur_hash.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/align.hh>
#include "utils/large_bitset.hh"
#include <array>
#include <cstdlib>
#include "bloom_filter.hh"

namespace utils {
namespace filter {

thread_local bloom_filter::stats bloom_filter::_shard_stats;

template<typename Func>
void for_each_index(hashed_key hk, int count, int64_t max, filter_format format, Func&& func) {
    auto h = hk.hash();
    int64_t base = (format == filter_format::k_l_format) ? h[0] : h[1];
    int64_t inc = (format == filter_format::k_l_format) ? h[1] : h[0];
    for (int i = 0; i < count; i++) {
        if (func(std::abs(base % max)) == stop_iteration::yes) {
            break;
        }
        base = static_cast<int64_t>(static_cast<uint64_t>(base) + static_cast<uint64_t>(inc));
    }
}

bloom_filter::bloom_filter(int hashes, bitmap&& bs, filter_format format) noexcept
    : _bitset(std::move(bs))
    , _hash_count(hashes)
    , _format(format)
{
    _stats.memory_size += memory_size();
}

bloom_filter::~bloom_filter() noexcept {
    _stats.memory_size -= memory_size();
}

bool bloom_filter::is_present(hashed_key key) {
    bool result = true;
    for_each_index(key, _hash_count, _bitset.size(), _format, [this, &result] (auto i) {
        if (!_bitset.test(i)) {
            result = false;
            return stop_iteration::yes;
        }
        return stop_iteration::no;
    });
    return result;
}

void bloom_filter::add(const bytes_view& key) {
    for_each_index(make_hashed_key(key), _hash_count, _bitset.size(), _format, [this] (auto i) {
        _bitset.set(i);
        return stop_iteration::no;
    });
}

bool bloom_filter::is_present(const bytes_view& key) {
    return is_present(make_hashed_key(key));
}

filter_ptr create_filter(int hash, large_bitset&& bitset, filter_format format) {
    return std::make_unique<murmur3_bloom_filter>(hash, std::move(bitset), format);
}

filter_ptr create_filter(int hash, int64_t num_elements, int buckets_per, filter_format format) {
    int64_t num_bits = (num_elements * buckets_per) + bloom_calculations::EXCESS;
    num_bits = align_up<int64_t>(num_bits, 64);  // Seems to be implied in origin
    large_bitset bitset(num_bits);
    return std::make_unique<murmur3_bloom_filter>(hash, std::move(bitset), format);
}
}
}
