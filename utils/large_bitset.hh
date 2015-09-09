/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

// A bitset containing a very large number of bits, so it uses fragmented
// storage in order not to stress the memory allocator.

#pragma once

#include <memory>
#include <vector>
#include <limits>
#include <iterator>
#include <algorithm>

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
    // load data from host bitmap (in host byte order); returns end bit position
    template <typename IntegerIterator>
    size_t load(IntegerIterator start, IntegerIterator finish, size_t position = 0);
    template <typename IntegerIterator>
    IntegerIterator save(IntegerIterator out, size_t position = 0, size_t n = std::numeric_limits<size_t>::max());
};

template <typename IntegerIterator>
size_t
large_bitset::load(IntegerIterator start, IntegerIterator finish, size_t position) {
    using input_int_type = typename std::iterator_traits<IntegerIterator>::value_type;
    if (position % bits_per_int() == 0 && sizeof(input_int_type) == sizeof(int_type)) {
        auto idx = position;
        auto idx1 = idx / bits_per_block();
        idx %= bits_per_block();
        auto idx2 = idx / bits_per_int();
        while (start != finish) {
            auto now = std::min<size_t>(ints_per_block() - idx2, std::distance(start, finish));
            std::copy_n(start, now, _storage[idx1].get() + idx2);
            start += now;
            ++idx1;
            idx2 = 0;
        }
    } else {
        while (start != finish) {
            auto bitmask = *start++;
            for (size_t i = 0; i < std::numeric_limits<input_int_type>::digits; ++i) {
                if (bitmask & 1) {
                    set(position);
                } else {
                    clear(position);
                }
                bitmask >>= 1;
                ++position;
            }
        }
    }
    return position;
}

template <typename IntegerIterator>
IntegerIterator
large_bitset::save(IntegerIterator out, size_t position, size_t n) {
    n = std::min(n, size() - position);
    using output_int_type = typename std::iterator_traits<IntegerIterator>::value_type;
    if (position % bits_per_int() == 0
            && n % bits_per_int() == 0
            && sizeof(output_int_type) == sizeof(int_type)) {
        auto idx = position;
        auto idx1 = idx / bits_per_block();
        idx %= bits_per_block();
        auto idx2 = idx / bits_per_int();
        auto n_ints = n / bits_per_int();
        while (n_ints) {
            auto now = std::min(ints_per_block() - idx2, n_ints);
            out = std::copy_n(_storage[idx1].get() + idx2, now, out);
            ++idx1;
            idx2 = 0;
            n_ints -= now;
        }
    } else {
        output_int_type result = 0;
        unsigned bitpos = 0;
        while (n) {
            result |= output_int_type(test(position)) << bitpos;
            ++position;
            ++bitpos;
            --n;
            if (bitpos == std::numeric_limits<output_int_type>::digits) {
                *out = result;
                ++out;
                result = 0;
                bitpos = 0;
            }
        }
        if (bitpos) {
            *out = result;
            ++out;
        }
    }
    return out;
}
