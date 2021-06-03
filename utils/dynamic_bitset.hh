/*
 * Copyright 2015-present ScyllaDB
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
    unsigned _nlevels = 0;
private:
    // For n in range 0..(bits_per_int-1), produces a mask with all bits < n set
    static int_type mask_lower_bits(size_t n) {
        return (int_type(1) << n) - 1;
    }
    // For n in range 0..(bits_per_int-1), produces a mask with all bits >= n set
    static int_type mask_higher_bits(size_t n) {
        return ~mask_lower_bits(n);
    }
    // For bit n, produce index into _bits[level]
    static size_t level_idx(unsigned level, size_t n) {
        return n >> ((level + 1) * level_shift);
    }
    // For bit n, produce bit number in _bits[level][level_idx]
    static unsigned level_remainder(unsigned level, size_t n) {
        return (n >> (level * level_shift)) & (bits_per_int - 1);
    }
    void do_resize(size_t n, bool set);
public:
    enum : size_t {
        npos = std::numeric_limits<size_t>::max()
    };
public:
    explicit dynamic_bitset(size_t nr_bits);

    bool test(size_t n) const {
        auto idx = n / bits_per_int;
        return _bits[0][idx] & (int_type(1u) << (n % bits_per_int));
    }
    void set(size_t n);
    void clear(size_t n);

    size_t size() const { return _bits_count; }

    size_t find_first_set() const;
    size_t find_next_set(size_t n) const;
    size_t find_last_set() const;
};

}
