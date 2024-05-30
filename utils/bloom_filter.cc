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
#include <cmath>
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

size_t get_bitset_size(int64_t num_elements, int buckets_per) {
    int64_t num_bits = (num_elements * buckets_per) + bloom_calculations::EXCESS;
    num_bits = align_up<int64_t>(num_bits, 64);  // Seems to be implied in origin
    return num_bits;
}

filter_ptr create_filter(int hash, large_bitset&& bitset, filter_format format) {
    return std::make_unique<murmur3_bloom_filter>(hash, std::move(bitset), format);
}

filter_ptr create_filter(int hash, int64_t num_elements, int buckets_per, filter_format format) {
    return std::make_unique<murmur3_bloom_filter>(hash, large_bitset(get_bitset_size(num_elements, buckets_per)), format);
}

void maybe_fold_filter(filter_ptr& filter, int64_t num_elements, int buckets_per) {
    auto bf = static_cast<bloom_filter*>(filter.get());
    auto curr_num_bits = bf->bits().size();
    size_t ideal_num_bits = get_bitset_size(num_elements, buckets_per);

    if (curr_num_bits <= ideal_num_bits * 1.5) {
        // No need to fold if the difference between current size and the ideal
        // size is not more than 50% of the ideal size. This also handles
        // curr_num_bits < ideal_num_bits case where folding is not possible.
        return;
    }

    // The current bloom filter needs to be folded to a size that is
    // 1. a factor of curr_num_bits (only then folding is possible)
    // 2. closer to ideal_num_bits
    // 3. aligned up to 64 (format requirement)
    //
    // From (1) and (2), the possible fold sizes are :
    // a. the smallest factor of curr_num_bits that is >= ideal_num_bits (or)
    // b. the greatest factor of curr_num_bits that is <= ideal_num_bits.
    // (a) is used as the folding size here as that will have the lowest FP rate among these two.
    size_t fold_num_bits = curr_num_bits;
    size_t curr_num_bits_sqrt = std::floor(std::sqrt(curr_num_bits));
    for (size_t i = 1; i <= curr_num_bits_sqrt; i++) {
        if (curr_num_bits % i == 0) {
            // i is a factor
            for (auto factor : {i, curr_num_bits / i}) {
                if (factor % 64 == 0 && ideal_num_bits <= factor && factor < fold_num_bits) {
                    fold_num_bits = factor;
                }
            }
        }
    }

    if (fold_num_bits == curr_num_bits) {
        // Not possible to fold
        return;
    }

    // fold the bloom filter to fold_num_bits
    bf->fold(fold_num_bits);
}
}
}
