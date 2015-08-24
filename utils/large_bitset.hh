/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

// A bitset containing a very large number of bits, so it uses fragmented
// storage in order not to stress the memory allocator.

#pragma once

#include <memory>
#include <vector>
#include <limits>

class large_bitset {
    static constexpr size_t block_size() { return 128 * 1024; }
    using int_type = unsigned long;
    static constexpr size_t bits_per_int() {
        return std::numeric_limits<int_type>::digits;
    }
    static constexpr size_t ints_per_block() {
        return block_size() / sizeof(int_type);
    }
    static constexpr size_t bits_per_block() {
        return ints_per_block() * bits_per_int();
    }
    size_t _nr_bits = 0;
    std::vector<std::unique_ptr<int_type[]>> _storage;
public:
    explicit large_bitset(size_t nr_bits);
    large_bitset(large_bitset&&) = default;
    large_bitset(const large_bitset&) = delete;
    large_bitset& operator=(const large_bitset&) = delete;
    size_t size() const {
        return _nr_bits;
    }
    bool test(size_t idx) const {
        auto idx1 = idx / bits_per_block();
        idx %= bits_per_block();
        auto idx2 = idx / bits_per_int();
        idx %= bits_per_int();
        auto idx3 = idx;
        return (_storage[idx1][idx2] >> idx3) & 1;
    }
    void set(size_t idx) {
        auto idx1 = idx / bits_per_block();
        idx %= bits_per_block();
        auto idx2 = idx / bits_per_int();
        idx %= bits_per_int();
        auto idx3 = idx;
        _storage[idx1][idx2] |= int_type(1) << idx3;
    }
    void clear(size_t idx) {
        auto idx1 = idx / bits_per_block();
        idx %= bits_per_block();
        auto idx2 = idx / bits_per_int();
        idx %= bits_per_int();
        auto idx3 = idx;
        _storage[idx1][idx2] &= ~(int_type(1) << idx3);
    }
    void clear();
};
