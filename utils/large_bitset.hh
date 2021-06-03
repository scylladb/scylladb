/*
 * Copyright (C) 2015-present ScyllaDB
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

// A bitset containing a very large number of bits, so it uses fragmented
// storage in order not to stress the memory allocator.

#pragma once

#include <memory>
#include <vector>
#include <limits>
#include <iterator>
#include <algorithm>
#include <seastar/core/thread.hh>
#include <seastar/core/preempt.hh>
#include "utils/chunked_vector.hh"

using namespace seastar;

class large_bitset {
    using int_type = uint64_t;
    static constexpr size_t bits_per_int() {
        return std::numeric_limits<int_type>::digits;
    }
    size_t _nr_bits = 0;
    utils::chunked_vector<int_type> _storage;
public:
    explicit large_bitset(size_t nr_bits);
    explicit large_bitset(size_t nr_bits, utils::chunked_vector<int_type> storage) : _nr_bits(nr_bits), _storage(std::move(storage)) {}
    large_bitset(large_bitset&&) = default;
    large_bitset(const large_bitset&) = delete;
    large_bitset& operator=(const large_bitset&) = delete;
    size_t size() const {
        return _nr_bits;
    }

    size_t memory_size() const {
        return _storage.memory_size();
    }

    bool test(size_t idx) const {
        auto idx1 = idx / bits_per_int();
        idx %= bits_per_int();
        auto idx2 = idx;
        return (_storage[idx1] >> idx2) & 1;
    }
    void set(size_t idx) {
        auto idx1 = idx / bits_per_int();
        idx %= bits_per_int();
        auto idx2 = idx;
        _storage[idx1] |= int_type(1) << idx2;
    }
    void clear(size_t idx) {
        auto idx1 = idx / bits_per_int();
        idx %= bits_per_int();
        auto idx2 = idx;
        _storage[idx1] &= ~(int_type(1) << idx2);
    }
    void clear();

    const utils::chunked_vector<int_type>& get_storage() const {
        return _storage;
    }
};
