/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 *
 * This file contains code from a variety of .java files from the
 * Cassandra project, adapted to our needs.
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once
#include "i_filter.hh"
#include "utils/large_bitset.hh"

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

// Get the size of the bitset (in bits, not bytes) for the specific parameters.
size_t get_bitset_size(int64_t num_elements, int buckets_per);

filter_ptr create_filter(int hash, large_bitset&& bitset, filter_format format);
filter_ptr create_filter(int hash, int64_t num_elements, int buckets_per, filter_format format);
}
}
