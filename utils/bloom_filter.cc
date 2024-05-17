/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "i_filter.hh"
#include "bytes.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/align.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/on_internal_error.hh>
#include "utils/large_bitset.hh"
#include <array>
#include <cstdlib>
#include "utils/bloom_calculations.hh"
#include "bloom_filter.hh"
#include "log.hh"

namespace utils {
namespace filter {
static logging::logger filterlog("bloom_filter");

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

void bloom_filter::fold(const size_t new_num_bits) {
    const auto curr_num_bits = _bitset.size();
    if (curr_num_bits % new_num_bits != 0) {
        seastar::on_internal_error(filterlog, std::format("cannot fold down bloom filter as the new number of bits({}) is not a factor of existing bitset size({})", new_num_bits, curr_num_bits));
        return;
    }

    // The folding will happen inplace (i.e) the first new_num_bits of the current
    // bitmap will be updated with the folded filter's bits. A bit b[i] where 'i'
    // belonging to [0, new_num_bits), will be set if any of its input bits :
    // b[i], b[i + new_num_bits], b[i + 2*new_num_bits]... is set in the current bitmap.
    for (uint64_t new_bitset_idx = 0; new_bitset_idx < new_num_bits; new_bitset_idx++) {
        if (_bitset.test(new_bitset_idx)) {
            // the target bit is already set - no need to look at other input bits
            continue;
        }

        // set the target bit if any of its input bit is set
        auto old_bitset_idx = new_bitset_idx + new_num_bits;
        while (old_bitset_idx < curr_num_bits) {
            if (_bitset.test(old_bitset_idx)) {
                _bitset.set(new_bitset_idx);
                break;
            }
            old_bitset_idx += new_num_bits;
        }
    }

    // The first new_num_bits has the folded filter bits.
    // Shrink the bitmap storage to remove the older bits.
    _bitset.shrink(new_num_bits);
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
