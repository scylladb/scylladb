/*
 * Copyright 2015 ScyllaDB
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

namespace utils {

class dynamic_bitset {
    using int_type = uint64_t;
    static constexpr size_t bits_per_int = std::numeric_limits<int_type>::digits;
    static constexpr int_type all_set = std::numeric_limits<int_type>::max();
private:
    std::vector<int_type> _bits;
    size_t _bits_count = 0;
private:
    static int_type mask_lower_bits(size_t n) {
        if (n != bits_per_int) {
            return (int_type(1u) << n) - 1;
        } else {
            return all_set;
        }
    }
    static int_type mask_higher_bits(size_t n) {
        return ~mask_lower_bits(bits_per_int - n);
    }
    template<typename Transform>
    size_t do_find_first(Transform&& transform) const;
    template<typename Transform>
    size_t do_find_next(size_t n, Transform&& transform) const;
    template<typename Transform>
    size_t do_find_last(Transform&& transform) const;
    template<typename Transform>
    size_t do_find_previous(size_t n, Transform&& transform) const;
public:
    enum : size_t {
        npos = std::numeric_limits<size_t>::max()
    };
public:
    bool test(size_t n) const {
        auto idx = n / bits_per_int;
        return _bits[idx] & (int_type(1u) << (n % bits_per_int));
    }
    void set(size_t n) {
        auto idx = n / bits_per_int;
        _bits[idx] |= int_type(1u) << (n % bits_per_int);
    }
    void clear(size_t n) {
        auto idx = n / bits_per_int;
        _bits[idx] &= ~(int_type(1u) << (n % bits_per_int));
    }

    size_t size() const { return _bits_count; }

    size_t find_first_set() const;
    size_t find_first_clear() const;
    size_t find_next_set(size_t n) const;
    size_t find_next_clear(size_t n) const;
    size_t find_last_set() const;
    size_t find_previous_set(size_t n) const;
    size_t find_last_clear() const;
    size_t find_previous_clear(size_t n) const;

    void resize(size_t n, bool set = false);
};

}
