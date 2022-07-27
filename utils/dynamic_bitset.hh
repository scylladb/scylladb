/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <limits>
#include <vector>

#include <seastar/core/align.hh>
#include <seastar/core/bitops.hh>

namespace utils {

class dynamic_bitset {
    using int_type = uint64_t;
    static constexpr size_t bits_per_int = std::numeric_limits<int_type>::digits;
    static constexpr int_type all_set = std::numeric_limits<int_type>::max();
    static constexpr unsigned level_shift = seastar::log2ceil(bits_per_int);
private:
    std::vector<std::vector<int_type>> _bits; // level n+1 = 64:1 summary of level n
    size_t _bits_count = 0;
private:
    // For n in range 0..(bits_per_int-1), produces a mask with all bits < n set
    static int_type mask_lower_bits(size_t n) noexcept {
        return (int_type(1) << n) - 1;
    }
    // For n in range 0..(bits_per_int-1), produces a mask with all bits >= n set
    static int_type mask_higher_bits(size_t n) noexcept {
        return ~mask_lower_bits(n);
    }
    // For bit n, produce index into _bits[level]
    static size_t level_idx(unsigned level, size_t n) noexcept {
        return n >> ((level + 1) * level_shift);
    }
    // For bit n, produce bit number in _bits[level][level_idx]
    static unsigned level_remainder(unsigned level, size_t n) noexcept {
        return (n >> (level * level_shift)) & (bits_per_int - 1);
    }
public:
    enum : size_t {
        npos = std::numeric_limits<size_t>::max()
    };
public:
    explicit dynamic_bitset(size_t nr_bits);

    // undefined if n >= size
    bool test(size_t n) const noexcept {
        auto idx = n / bits_per_int;
        return _bits[0][idx] & (int_type(1u) << (n % bits_per_int));
    }
    // undefined if n >= size
    void set(size_t n) noexcept;
    // undefined if n >= size
    void clear(size_t n) noexcept;

    size_t size() const noexcept { return _bits_count; }

    size_t find_first_set() const noexcept;
    size_t find_next_set(size_t n) const noexcept;
    size_t find_last_set() const noexcept;
};

}
