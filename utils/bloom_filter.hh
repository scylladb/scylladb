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
 *
 * This file contains code from a variety of .java files from the
 * Cassandra project, adapted to our needs.
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
    filter_format _format;

    static thread_local struct stats {
        uint64_t memory_size = 0;
    } _shard_stats;
    stats& _stats = _shard_stats;

public:
    int num_hashes() { return _hash_count; }
    bitmap& bits() { return _bitset; }

    bloom_filter(int hashes, bitmap&& bs, filter_format format) noexcept;
    ~bloom_filter() noexcept;

    virtual void add(const bytes_view& key) override;

    virtual bool is_present(const bytes_view& key) override;

    virtual bool is_present(hashed_key key) override;

    virtual void clear() override {
        _bitset.clear();
    }

    virtual void close() override { }

    virtual size_t memory_size() override {
        return sizeof(_hash_count) + _bitset.memory_size();
    }

    static const stats& get_shard_stats() noexcept {
        return _shard_stats;
    }
};

struct murmur3_bloom_filter: public bloom_filter {

    murmur3_bloom_filter(int hashes, bitmap&& bs, filter_format format)
        : bloom_filter(hashes, std::move(bs), format)
    {}
};

struct always_present_filter: public i_filter {

    virtual bool is_present(const bytes_view& key) override {
        return true;
    }

    virtual bool is_present(hashed_key key) override {
        return true;
    }

    virtual void add(const bytes_view& key) override { }

    virtual void clear() override { }

    virtual void close() override { }

    virtual size_t memory_size() override {
        return 0;
    }
};

filter_ptr create_filter(int hash, large_bitset&& bitset, filter_format format);
filter_ptr create_filter(int hash, int64_t num_elements, int buckets_per, filter_format format);
}
}
