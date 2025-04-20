/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/bitops.hh>
#include <seastar/core/align.hh>
#include <ranges>

#include "utils/dynamic_bitset.hh"
#include "seastarx.hh"

namespace utils {

void dynamic_bitset::set(size_t n) noexcept {
    for (auto& level : _bits) {
        auto idx = n / bits_per_int;
        auto old = level[idx];
        level[idx] |= int_type(1u) << (n % bits_per_int);
        if (old) {
            break;
        }
        n = idx; // prepare for next level
    }
}

void dynamic_bitset::clear(size_t n) noexcept {
    for (auto& level : _bits) {
        auto idx = n / bits_per_int;
        auto old = level[idx];
        level[idx] &= ~(int_type(1u) << (n % bits_per_int));
        if (!old || level[idx]) {
            break;
        }
        n = idx; // prepare for next level
    }
}

size_t dynamic_bitset::find_first_set() const noexcept
{
    size_t pos = 0;
    for (auto& vv : _bits | std::views::reverse) {
        auto v = vv[pos];
        pos *= bits_per_int;
        if (v) {
            pos += count_trailing_zeros(v);
        } else {
            return npos;
        }
    }
    return pos;
}

size_t dynamic_bitset::find_next_set(size_t n) const noexcept
{
    ++n;

    unsigned level = 0;
    unsigned nlevels = _bits.size();

    // Climb levels until we find a set bit in the right place
    while (level != nlevels) {
        if (n >= _bits_count) {
            return npos;
        }
        auto v = _bits[level][level_idx(level, n)];
        v &= ~mask_lower_bits(level_remainder(level, n));
        if (v) {
            break;
        }
        ++level;
        n = align_up(n, size_t(1) << (level_shift * level));
    }

    if (level == nlevels) {
        return npos;
    }

    // Descend levels until we reach level 0
    do {
        auto v = _bits[level][level_idx(level, n)];
        v &= ~mask_lower_bits(level_remainder(level, n));
        n = align_down(n, size_t(1) << (level_shift * (level + 1)));
        n += count_trailing_zeros(v) << (level_shift * level);
    } while (level-- != 0);

    return n;
}

size_t dynamic_bitset::find_last_set() const noexcept
{
    size_t pos = 0;
    for (auto& vv : _bits | std::views::reverse) {
        auto v = vv[pos];
        pos *= bits_per_int;
        if (v) {
            pos += bits_per_int - 1 - count_leading_zeros(v);
        } else {
            return npos;
        }
    }
    return pos;
}

dynamic_bitset::dynamic_bitset(size_t nr_bits)
    : _bits_count(nr_bits)
{
    auto div_ceil = [] (size_t num, size_t den) {
        return (num + den - 1) / den;
    };
    // 1-64: 1 level
    // 65-4096: 2 levels
    // 4097-262144: 3 levels
    // etc.
    unsigned nr_levels = div_ceil(log2ceil(align_up(nr_bits, size_t(bits_per_int))), level_shift);
    _bits.resize(nr_levels);
    size_t level_bits = nr_bits;
    for (unsigned level = 0; level != nr_levels; ++level) {
        auto level_words = align_up(level_bits, bits_per_int) / bits_per_int;
        _bits[level].resize(level_words);
        level_bits = level_words; // for next iteration
    }
}

}
