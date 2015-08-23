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
 *
 * This file contains code from a variety of .java files from the
 * Cassandra project, adapted to our needs.
 */
#pragma once
#include "i_filter.hh"
#include "utils/murmur_hash.hh"
#include "utils/large_bitset.hh"

#include <vector>

namespace utils {
namespace filter {

class bloom_filter: public i_filter {
public:
    using bitmap = large_bitset;

private:
    bitmap _bitset;
    int _hash_count;

    void set_indexes(int64_t base, int64_t inc, int count, long max, std::vector<long>& results);
    std::vector<long> get_hash_buckets(const bytes_view& key, int hash_count, long max);
    std::vector<long> indexes(const bytes_view& key);

public:
    int num_hashes() { return _hash_count; }
    bitmap& bits() { return _bitset; }

    bloom_filter(int hashes, bitmap&& bs) : _bitset(std::move(bs)), _hash_count(hashes) {
    }

    virtual void hash(const bytes_view& b, long seed, std::array<uint64_t, 2>& result) = 0;

    virtual void add(const bytes_view& key) override {

        auto idx = indexes(key);
        for (int i = 0; i < _hash_count; i++) {
            _bitset.set(idx[i]);
        }
    }

    virtual bool is_present(const bytes_view& key) override {

        auto idx = indexes(key);
        for (int i = 0; i < _hash_count; i++) {
            if (!_bitset.test(idx[i])) {
                return false;
            }
        }
        return true;
    }

    virtual void clear() override {
        _bitset.clear();
    }

    virtual void close() override { }
};

struct murmur3_bloom_filter: public bloom_filter {

    murmur3_bloom_filter(int hashes, bitmap&& bs) : bloom_filter(hashes, std::move(bs)) {}

    virtual void hash(const bytes_view& b, long seed, std::array<uint64_t, 2>& result) {
        utils::murmur_hash::hash3_x64_128(b, seed, result);
    }
};

struct always_present_filter: public i_filter {

    virtual bool is_present(const bytes_view& key) override {
        return true;
    }

    virtual void add(const bytes_view& key) override { }

    virtual void clear() override { }

    virtual void close() override { }
};

filter_ptr create_filter(int hash, large_bitset&& bitset);
filter_ptr create_filter(int hash, long num_elements, int buckets_per);
}
}
