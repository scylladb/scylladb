/*
 * Copyright (C) 2017-present ScyllaDB
 *
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

#include <boost/intrusive/list.hpp>
#include <limits>
#include <seastar/core/bitops.hh>
#include "seastarx.hh"

namespace bi = boost::intrusive;

// Returns largest N such that 2^N <= v.
// Undefined for v == 0.
inline size_t pow2_rank(size_t v) {
    return std::numeric_limits<size_t>::digits - 1 - count_leading_zeros(v);
}
 

// Returns largest N such that 2^N <= v.
// Undefined for v == 0.
inline constexpr size_t pow2_rank_constexpr(size_t v) {
    return v <= 1 ? 1 : 1 + pow2_rank_constexpr(v >> 1);
}

// Configures log_heap.
// Only values <= max_size can be inserted into the histogram.
// log_heap::contains_above_min() returns true if and only if the histogram contains any value >= min_size.
struct log_heap_options {
    const size_t min_size;
    const size_t sub_bucket_shift;
    const size_t max_size;

    constexpr log_heap_options(const size_t min_size, size_t sub_bucket_shift, size_t max_size)
            : min_size(min_size)
            , sub_bucket_shift(sub_bucket_shift)
            , max_size(max_size) {
    }

    size_t bucket_of(size_t value) const {
#ifdef SANITIZE
        // ubsan will otherwise complain about pow2_rank(0)
        if (value < min_size) {
            return 0;
        }
#endif
        const auto min_mask = -size_t(value >= min_size); // 0 when below min_size, all bits on otherwise
        value = value - min_size + 1;
        const auto pow2_index = pow2_rank(value);
        const auto bucket = (pow2_index + 1) & min_mask;
        const auto unmasked_sub_bucket_index = (value << sub_bucket_shift) >> pow2_index;
        const auto mask = ((1 << sub_bucket_shift) - 1) & min_mask;
        const auto sub_bucket_index = unmasked_sub_bucket_index & mask;
        return (bucket << sub_bucket_shift) - mask + sub_bucket_index;
    }

    constexpr size_t number_of_buckets() const {
        const auto min_mask = -size_t(max_size >= min_size); // 0 when below min_size, all bits on otherwise
        const auto value = max_size - min_size + 1;
        const auto pow2_index = pow2_rank_constexpr(value);
        const auto bucket = (pow2_index + 1) & min_mask;
        const auto unmasked_sub_bucket_index = (value << sub_bucket_shift) >> pow2_index;
        const auto mask = ((1 << sub_bucket_shift) - 1) & min_mask;
        const auto sub_bucket_index = unmasked_sub_bucket_index & mask;
        const auto ret = (bucket << sub_bucket_shift) - mask + sub_bucket_index;
        return ret + 1;
    }
};

template<const log_heap_options& opts>
struct log_heap_bucket_index {
    using type = std::conditional_t<(opts.number_of_buckets() > ((1 << 16) - 1)), uint32_t,
          std::conditional_t<(opts.number_of_buckets() > ((1 << 8) - 1)), uint16_t, uint8_t>>;
};

template<const log_heap_options& opts>
struct log_heap_hook : public bi::list_base_hook<> {
    typename log_heap_bucket_index<opts>::type cached_bucket;
};

template<typename T>
size_t hist_key(const T&);

template<typename T, const log_heap_options& opts, bool = std::is_base_of<log_heap_hook<opts>, T>::value>
struct log_heap_element_traits {
    using bucket_type = bi::list<T, bi::constant_time_size<false>>;
    static void cache_bucket(T& v, typename log_heap_bucket_index<opts>::type b) {
        v.cached_bucket = b;
    }
    static size_t cached_bucket(const T& v) {
        return v.cached_bucket;
    }
    static size_t hist_key(const T& v) {
        return ::hist_key<T>(v);
    }
};

template<typename T, const log_heap_options& opts>
struct log_heap_element_traits<T, opts, false> {
    using bucket_type = typename T::bucket_type;
    static void cache_bucket(T&, typename log_heap_bucket_index<opts>::type);
    static size_t cached_bucket(const T&);
    static size_t hist_key(const T&);
};

/*
 * Histogram that stores elements in different buckets according to their size.
 * Values are mapped to a sequence of power-of-two ranges that are split in
 * 1 << opts.sub_bucket_shift sub-buckets. Values less than opts.min_size
 * are placed in bucket 0, whereas values bigger than opts.max_size are
 * not admitted. The histogram gives bigger precision to smaller values, with
 * precision decreasing as values get larger.
 */
template<typename T, const log_heap_options& opts>
requires requires() {
    typename log_heap_element_traits<T, opts>;
}
class log_heap final {
    // Ensure that (value << sub_bucket_index) in bucket_of() doesn't overflow
    static_assert(pow2_rank_constexpr(opts.max_size - opts.min_size + 1) + opts.sub_bucket_shift < std::numeric_limits<size_t>::digits, "overflow");
private:
    using traits = log_heap_element_traits<T, opts>;
    using bucket = typename traits::bucket_type;

    struct hist_size_less_compare {
        inline bool operator()(const T& v1, const T& v2) const {
            return traits::hist_key(v1) < traits::hist_key(v2);
        }
    };

    std::array<bucket, opts.number_of_buckets()> _buckets;
    ssize_t _watermark = -1;
public:
    template <bool IsConst>
    class hist_iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::conditional_t<IsConst, const T, T>;
        using difference_type = std::ptrdiff_t;
        using pointer = std::conditional_t<IsConst, const T, T>*;
        using reference = std::conditional_t<IsConst, const T, T>&;
    private:
        using hist_type = std::conditional_t<IsConst, const log_heap, log_heap>;
        using iterator_type = std::conditional_t<IsConst, typename bucket::const_iterator, typename bucket::iterator>;

        hist_type& _h;
        ssize_t _b;
        iterator_type _it;
    public:
        struct end_tag {};
        hist_iterator(hist_type& h)
            : _h(h)
            , _b(h._watermark)
            , _it(_b >= 0 ? h._buckets[_b].begin() : h._buckets[0].end()) {
        }
        hist_iterator(hist_type& h, end_tag)
            : _h(h)
            , _b(-1)
            , _it(h._buckets[0].end()) {
        }
        std::conditional_t<IsConst, const T, T>& operator*() {
            return *_it;
        }
        hist_iterator& operator++() {
            if (++_it == _h._buckets[_b].end()) {
                do {
                    --_b;
                } while (_b >= 0 && (_it = _h._buckets[_b].begin()) == _h._buckets[_b].end());
            }
            return *this;
        }
        bool operator==(const hist_iterator& other) const {
            return _b == other._b && _it == other._it;
        }
        bool operator!=(const hist_iterator& other) const {
            return !(*this == other);
        }
    };
    using iterator = hist_iterator<false>;
    using const_iterator = hist_iterator<true>;
public:
    bool empty() const {
        return _watermark == -1;
    }
    // Returns true if and only if contains any value >= opts.min_size.
    bool contains_above_min() const {
        return _watermark > 0;
    }
    const_iterator begin() const {
        return const_iterator(*this);
    }
    const_iterator end() const {
        return const_iterator(*this, typename const_iterator::end_tag());
    }
    iterator begin() {
        return iterator(*this);
    }
    iterator end() {
        return iterator(*this, typename iterator::end_tag());
    }
    // Returns a range of buckets starting from that with the smaller values.
    // Each bucket is a range of const T&.
    const auto& buckets() const {
        return _buckets;
    }
    // Pops one of the largest elements in the histogram.
    void pop_one_of_largest() {
        _buckets[_watermark].pop_front();
        maybe_adjust_watermark();
    }
    // Returns one of the largest elements in the histogram.
    const T& one_of_largest() const {
        return _buckets[_watermark].front();
    }
    // Returns one of the largest elements in the histogram.
    T& one_of_largest() {
        return _buckets[_watermark].front();
    }
    // Pushes a new element onto the histogram.
    void push(T& v) {
        auto b = opts.bucket_of(traits::hist_key(v));
        traits::cache_bucket(v, b);
        _buckets[b].push_front(v);
        _watermark = std::max(ssize_t(b), _watermark);
    }
    // Adjusts the histogram when the specified element becomes larger.
    void adjust_up(T& v) {
        auto b = traits::cached_bucket(v);
        auto nb = opts.bucket_of(traits::hist_key(v));
        if (nb != b) {
            traits::cache_bucket(v, nb);
            _buckets[nb].splice(_buckets[nb].begin(), _buckets[b], _buckets[b].iterator_to(v));
            _watermark = std::max(ssize_t(nb), _watermark);
        }
    }
    // Removes the specified element from the histogram.
    void erase(T& v) {
        auto& b = _buckets[traits::cached_bucket(v)];
        b.erase(b.iterator_to(v));
        maybe_adjust_watermark();
    }
    // Merges the specified histogram, moving all elements from it into this.
    void merge(log_heap& other) {
        for (size_t i = 0; i < opts.number_of_buckets(); ++i) {
            _buckets[i].splice(_buckets[i].begin(), other._buckets[i]);
        }
        _watermark = std::max(_watermark, other._watermark);
        other._watermark = -1;
    }
private:
    void maybe_adjust_watermark() {
        while (_buckets[_watermark].empty() && --_watermark >= 0) ;
    }
};
