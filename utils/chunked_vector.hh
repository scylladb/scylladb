/*
 * Copyright (C) 2017 ScyllaDB
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

// A vector-like container that uses discontiguous storage. Provides fast random access,
// the ability to append at the end, and limited contiguous allocations.
//
// std::deque would be a good fit, except the backing array can grow quite large.

#include "utils/small_vector.hh"

#include <boost/range/algorithm/equal.hpp>
#include <boost/algorithm/clamp.hpp>
#include <boost/version.hpp>
#include <memory>
#include <type_traits>
#include <iterator>
#include <utility>
#include <algorithm>
#include <stdexcept>

namespace utils {

struct chunked_vector_free_deleter {
    void operator()(void* x) const { ::free(x); }
};

template <typename T, size_t max_contiguous_allocation = 128*1024>
class chunked_vector {
    static_assert(std::is_nothrow_move_constructible<T>::value, "T must be nothrow move constructible");
    using chunk_ptr = std::unique_ptr<T[], chunked_vector_free_deleter>;
    // Each chunk holds max_chunk_capacity() items, except possibly the last
    utils::small_vector<chunk_ptr, 1> _chunks;
    size_t _size = 0;
    size_t _capacity = 0;
private:
    static size_t max_chunk_capacity() {
        return std::max(max_contiguous_allocation / sizeof(T), size_t(1));
    }
    void reserve_for_push_back() {
        if (_size == _capacity) {
            do_reserve_for_push_back();
        }
    }
    void do_reserve_for_push_back();
    void make_room(size_t n);
    chunk_ptr new_chunk(size_t n);
    T* addr(size_t i) const {
        return &_chunks[i / max_chunk_capacity()][i % max_chunk_capacity()];
    }
    void check_bounds(size_t i) const {
        if (i >= _size) {
            throw std::out_of_range("chunked_vector out of range access");
        }
    }
    static void migrate(T* begin, T* end, T* result);
public:
    using value_type = T;
    using size_type = size_t;
    using difference_type = ssize_t;
    using reference = T&;
    using const_reference = const T&;
    using pointer = T*;
    using const_pointer = const T*;
public:
    chunked_vector() = default;
    chunked_vector(const chunked_vector& x);
    chunked_vector(chunked_vector&& x) noexcept;
    template <typename Iterator>
    chunked_vector(Iterator begin, Iterator end);
    explicit chunked_vector(size_t n, const T& value = T());
    ~chunked_vector();
    chunked_vector& operator=(const chunked_vector& x);
    chunked_vector& operator=(chunked_vector&& x) noexcept;

    bool empty() const {
        return !_size;
    }
    size_t size() const {
        return _size;
    }
    T& operator[](size_t i) {
        return *addr(i);
    }
    const T& operator[](size_t i) const {
        return *addr(i);
    }
    T& at(size_t i) {
        check_bounds(i);
        return *addr(i);
    }
    const T& at(size_t i) const {
        check_bounds(i);
        return *addr(i);
    }

    void push_back(const T& x) {
        reserve_for_push_back();
        new (addr(_size)) T(x);
        ++_size;
    }
    void push_back(T&& x) {
        reserve_for_push_back();
        new (addr(_size)) T(std::move(x));
        ++_size;
    }
    template <typename... Args>
    T& emplace_back(Args&&... args) {
        reserve_for_push_back();
        auto& ret = *new (addr(_size)) T(std::forward<Args>(args)...);
        ++_size;
        return ret;
    }
    void pop_back() {
        --_size;
        addr(_size)->~T();
    }
    const T& back() const {
        return *addr(_size - 1);
    }
    T& back() {
        return *addr(_size - 1);
    }

    void clear();
    void shrink_to_fit();
    void resize(size_t n);
    void reserve(size_t n) {
        if (n > _capacity) {
            make_room(n);
        }
    }

    size_t memory_size() const {
        return _capacity * sizeof(T);
    }
public:
    template <class ValueType>
    class iterator_type {
        const chunk_ptr* _chunks;
        size_t _i;
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = ValueType;
        using difference_type = ssize_t;
        using pointer = ValueType*;
        using reference = ValueType&;
    private:
        pointer addr() const {
            return &_chunks[_i / max_chunk_capacity()][_i % max_chunk_capacity()];
        }
        iterator_type(const chunk_ptr* chunks, size_t i) : _chunks(chunks), _i(i) {}
    public:
        iterator_type() = default;
        iterator_type(const iterator_type<std::remove_const_t<ValueType>>& x) : _chunks(x._chunks), _i(x._i) {} // needed for iterator->const_iterator conversion
        reference operator*() const {
            return *addr();
        }
        pointer operator->() const {
            return addr();
        }
        reference operator[](ssize_t n) const {
            return *(*this + n);
        }
        iterator_type& operator++() {
            ++_i;
            return *this;
        }
        iterator_type operator++(int) {
            auto x = *this;
            ++_i;
            return x;
        }
        iterator_type& operator--() {
            --_i;
            return *this;
        }
        iterator_type operator--(int) {
            auto x = *this;
            --_i;
            return x;
        }
        iterator_type& operator+=(ssize_t n) {
            _i += n;
            return *this;
        }
        iterator_type& operator-=(ssize_t n) {
            _i -= n;
            return *this;
        }
        iterator_type operator+(ssize_t n) const {
            auto x = *this;
            return x += n;
        }
        iterator_type operator-(ssize_t n) const {
            auto x = *this;
            return x -= n;
        }
        friend iterator_type operator+(ssize_t n, iterator_type a) {
            return a + n;
        }
        friend ssize_t operator-(iterator_type a, iterator_type b) {
            return a._i - b._i;
        }
        bool operator==(iterator_type x) const {
            return _i == x._i;
        }
        bool operator!=(iterator_type x) const {
            return _i != x._i;
        }
        bool operator<(iterator_type x) const {
            return _i < x._i;
        }
        bool operator<=(iterator_type x) const {
            return _i <= x._i;
        }
        bool operator>(iterator_type x) const {
            return _i > x._i;
        }
        bool operator>=(iterator_type x) const {
            return _i >= x._i;
        }
        friend class chunked_vector;
    };
    using iterator = iterator_type<T>;
    using const_iterator = iterator_type<const T>;
public:
    const T& front() const { return *cbegin(); }
    T& front() { return *begin(); }
    iterator begin() const { return iterator(_chunks.data(), 0); }
    iterator end() const { return iterator(_chunks.data(), _size); }
    const_iterator cbegin() const { return const_iterator(_chunks.data(), 0); }
    const_iterator cend() const { return const_iterator(_chunks.data(), _size); }
public:
    bool operator==(const chunked_vector& x) const {
        return boost::equal(*this, x);
    }
    bool operator!=(const chunked_vector& x) const {
        return !operator==(x);
    }
};


template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>::chunked_vector(const chunked_vector& x)
        : chunked_vector() {
    reserve(x.size());
    std::copy(x.begin(), x.end(), std::back_inserter(*this));
}

template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>::chunked_vector(chunked_vector&& x) noexcept
        : _chunks(std::exchange(x._chunks, {}))
        , _size(std::exchange(x._size, 0))
        , _capacity(std::exchange(x._capacity, 0)) {
}

template <typename T, size_t max_contiguous_allocation>
template <typename Iterator>
chunked_vector<T, max_contiguous_allocation>::chunked_vector(Iterator begin, Iterator end)
        : chunked_vector() {
    auto is_random_access = std::is_base_of<std::random_access_iterator_tag, typename std::iterator_traits<Iterator>::iterator_category>::value;
    if (is_random_access) {
        reserve(std::distance(begin, end));
    }
    std::copy(begin, end, std::back_inserter(*this));
    if (!is_random_access) {
        shrink_to_fit();
    }
}

template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>::chunked_vector(size_t n, const T& value) {
    reserve(n);
    std::fill_n(std::back_inserter(*this), n, value);
}


template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>&
chunked_vector<T, max_contiguous_allocation>::operator=(const chunked_vector& x) {
    auto tmp = chunked_vector(x);
    return *this = std::move(tmp);
}

template <typename T, size_t max_contiguous_allocation>
inline
chunked_vector<T, max_contiguous_allocation>&
chunked_vector<T, max_contiguous_allocation>::operator=(chunked_vector&& x) noexcept {
    if (this != &x) {
        this->~chunked_vector();
        new (this) chunked_vector(std::move(x));
    }
    return *this;
}

template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>::~chunked_vector() {
    if constexpr (!std::is_trivially_destructible_v<T>) {
        for (auto i = size_t(0); i != _size; ++i) {
            addr(i)->~T();
        }
    }
}

template <typename T, size_t max_contiguous_allocation>
typename chunked_vector<T, max_contiguous_allocation>::chunk_ptr
chunked_vector<T, max_contiguous_allocation>::new_chunk(size_t n) {
    auto p = malloc(n * sizeof(T));
    if (!p) {
        throw std::bad_alloc();
    }
    return chunk_ptr(reinterpret_cast<T*>(p));
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::migrate(T* begin, T* end, T* result) {
    while (begin != end) {
        new (result) T(std::move(*begin));
        begin->~T();
        ++begin;
        ++result;
    }
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::make_room(size_t n) {
    // First, if the last chunk is below max_chunk_capacity(), enlarge it

    auto last_chunk_capacity_deficit = _chunks.size() * max_chunk_capacity() - _capacity;
    if (last_chunk_capacity_deficit) {
        auto last_chunk_capacity = max_chunk_capacity() - last_chunk_capacity_deficit;
        auto capacity_increase = std::min(last_chunk_capacity_deficit, n - _capacity);
        auto new_last_chunk_capacity = last_chunk_capacity + capacity_increase;
        // FIXME: realloc? maybe not worth the complication; only works for PODs
        auto new_last_chunk = new_chunk(new_last_chunk_capacity);
        migrate(addr(_capacity - last_chunk_capacity), addr(_size), new_last_chunk.get());
        _chunks.back() = std::move(new_last_chunk);
        _capacity += capacity_increase;
    }

    // Reduce reallocations in the _chunks vector

    auto nr_chunks = (n + max_chunk_capacity() - 1) / max_chunk_capacity();
    _chunks.reserve(nr_chunks);

    // Add more chunks as needed

    while (_capacity < n) {
        auto now = std::min(n - _capacity, max_chunk_capacity());
        _chunks.push_back(new_chunk(now));
        _capacity += now;
    }
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::do_reserve_for_push_back() {
    if (_capacity == 0) {
        // allocate a bit of room in case utilization will be low
        reserve(boost::algorithm::clamp(512 / sizeof(T), 1, max_chunk_capacity()));
    } else if (_capacity < max_chunk_capacity() / 2) {
        // exponential increase when only one chunk to reduce copying
        reserve(_capacity * 2);
    } else {
        // add a chunk at a time later, since no copying will take place
        reserve((_capacity / max_chunk_capacity() + 1) * max_chunk_capacity());
    }
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::resize(size_t n) {
    reserve(n);
    // FIXME: construct whole chunks at once
    while (_size > n) {
        pop_back();
    }
    while (_size < n) {
        push_back(T{});
    }
    shrink_to_fit();
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::shrink_to_fit() {
    if (_chunks.empty()) {
        return;
    }
    while (!_chunks.empty() && _size <= (_chunks.size() - 1) * max_chunk_capacity()) {
        _chunks.pop_back();
        _capacity = _chunks.size() * max_chunk_capacity();
    }

    auto overcapacity = _size - _capacity;
    if (overcapacity) {
        auto new_last_chunk_capacity = _size - (_chunks.size() - 1) * max_chunk_capacity();
        // FIXME: realloc? maybe not worth the complication; only works for PODs
        auto new_last_chunk = new_chunk(new_last_chunk_capacity);
        migrate(addr((_chunks.size() - 1) * max_chunk_capacity()), addr(_size), new_last_chunk.get());
        _chunks.back() = std::move(new_last_chunk);
        _capacity = _size;
    }
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::clear() {
    while (_size > 0) {
        pop_back();
    }
    shrink_to_fit();
}

}
