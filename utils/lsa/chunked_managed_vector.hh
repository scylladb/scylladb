/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// A vector-like container that uses discontiguous storage. Provides fast random access,
// the ability to append at the end, and limited contiguous allocations.
//
// std::deque would be a good fit, except the backing array can grow quite large.

#include "utils/logalloc.hh"
#include "utils/managed_vector.hh"

#include <boost/range/algorithm/equal.hpp>
#include <boost/version.hpp>
#include <type_traits>
#include <iterator>
#include <utility>
#include <algorithm>
#include <stdexcept>

namespace lsa {

template <typename T>
class chunked_managed_vector {
    static const size_t max_contiguous_allocation = logalloc::segment_size * 0.1;
    static_assert(std::is_nothrow_move_constructible<T>::value, "T must be nothrow move constructible");
    using chunk_ptr = managed_vector<T>;
    // Each chunk holds max_chunk_capacity() items, except possibly the last
    managed_vector<chunk_ptr, 1> _chunks;
    size_t _size = 0;
    size_t _capacity = 0;
public:
    static size_t max_chunk_capacity() {
        return std::max(max_contiguous_allocation / sizeof(T), size_t(1));
    }
private:
    void reserve_for_push_back() {
        if (_size == _capacity) {
            do_reserve_for_push_back();
        }
    }
    void do_reserve_for_push_back();
    void make_room(size_t n, bool stop_after_one);
    chunk_ptr new_chunk(size_t n);
    const T* addr(size_t i) const {
        return &_chunks[i / max_chunk_capacity()][i % max_chunk_capacity()];
    }
    T* addr(size_t i) {
        return &_chunks[i / max_chunk_capacity()][i % max_chunk_capacity()];
    }
    void check_bounds(size_t i) const {
        if (i >= _size) {
            throw std::out_of_range("chunked_managed_vector out of range access");
        }
    }
    chunk_ptr& back_chunk() {
        return _chunks[_size / max_chunk_capacity()];
    }
    static void migrate(T* begin, T* end, managed_vector<T>& result);
public:
    using value_type = T;
    using size_type = size_t;
    using difference_type = ssize_t;
    using reference = T&;
    using const_reference = const T&;
    using pointer = T*;
    using const_pointer = const T*;
public:
    chunked_managed_vector() = default;
    chunked_managed_vector(const chunked_managed_vector& x);
    chunked_managed_vector(chunked_managed_vector&& x) noexcept;
    template <typename Iterator>
    chunked_managed_vector(Iterator begin, Iterator end);
    explicit chunked_managed_vector(size_t n, const T& value = T());
    ~chunked_managed_vector();
    chunked_managed_vector& operator=(const chunked_managed_vector& x);
    chunked_managed_vector& operator=(chunked_managed_vector&& x) noexcept;

    bool empty() const {
        return !_size;
    }
    size_t size() const {
        return _size;
    }
    size_t capacity() const {
        return _capacity;
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
        back_chunk().emplace_back(x);
        ++_size;
    }
    void push_back(T&& x) {
        reserve_for_push_back();
        back_chunk().emplace_back(std::move(x));
        ++_size;
    }
    template <typename... Args>
    T& emplace_back(Args&&... args) {
        reserve_for_push_back();
        auto& ret = back_chunk().emplace_back(std::forward<Args>(args)...);
        ++_size;
        return ret;
    }
    void pop_back() {
        --_size;
        back_chunk().pop_back();
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
            make_room(n, false);
        }
    }
    /// Reserve some of the memory.
    ///
    /// Allows reserving the memory chunk-by-chunk, avoiding stalls when a lot of
    /// chunks are needed. To drive the reservation to completion, call this
    /// repeatedly until the vector's capacity reaches the expected size, yielding
    /// between calls when necessary. Example usage:
    ///
    ///     return do_until([&my_vector, size] { return my_vector.capacity() == size; }, [&my_vector, size] () mutable {
    ///         my_vector.reserve_partial(size);
    ///     });
    ///
    /// Here, `do_until()` takes care of yielding between iterations when
    /// necessary.
    ///
    /// The recommended way to use this method is by calling utils::reserve_gently() from stall_free.hh
    /// instead of looping with this method directly. utils::reserve_gently() will repeatedly call this
    /// method to reserve the required quantity, yielding between calls when necessary.
    void reserve_partial(size_t n) {
        if (n > _capacity) {
            return make_room(n, true);
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
            return &const_cast<chunk_ptr*>(_chunks)[_i / max_chunk_capacity()][_i % max_chunk_capacity()];
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
        std::strong_ordering operator<=>(iterator_type x) const {
            return _i <=> x._i;
        }
        friend class chunked_managed_vector;
    };
    using iterator = iterator_type<T>;
    using const_iterator = iterator_type<const T>;
public:
    const T& front() const { return *cbegin(); }
    T& front() { return *begin(); }
    iterator begin() { return iterator(_chunks.data(), 0); }
    iterator end() { return iterator(_chunks.data(), _size); }
    const_iterator begin() const { return const_iterator(_chunks.data(), 0); }
    const_iterator end() const { return const_iterator(_chunks.data(), _size); }
    const_iterator cbegin() const { return const_iterator(_chunks.data(), 0); }
    const_iterator cend() const { return const_iterator(_chunks.data(), _size); }
    std::reverse_iterator<iterator> rbegin() { return std::reverse_iterator(end()); }
    std::reverse_iterator<iterator> rend() { return std::reverse_iterator(begin()); }
    std::reverse_iterator<const_iterator> rbegin() const { return std::reverse_iterator(end()); }
    std::reverse_iterator<const_iterator> rend() const { return std::reverse_iterator(begin()); }
    std::reverse_iterator<const_iterator> crbegin() const { return std::reverse_iterator(cend()); }
    std::reverse_iterator<const_iterator> crend() const { return std::reverse_iterator(cbegin()); }
public:
    bool operator==(const chunked_managed_vector& x) const {
        return boost::equal(*this, x);
    }

    // Returns the amount of external memory used to hold inserted items.
    // Takes into account reserved space.
    size_t external_memory_usage() const {
        // This ignores per-chunk sizeof(managed_vector::external) but should
        // be close-enough.
        return _chunks.external_memory_usage() + _capacity * sizeof(T);
    }

    // Clears the vector and ensures that the destructor will not have to free any memory so
    // that the object can be destroyed under any allocator.
    void clear_and_release() noexcept {
        clear();
        _chunks.clear_and_release();
    }
};


template <typename T>
chunked_managed_vector<T>::chunked_managed_vector(const chunked_managed_vector& x)
        : chunked_managed_vector() {
    reserve(x.size());
    std::copy(x.begin(), x.end(), std::back_inserter(*this));
}

template <typename T>
chunked_managed_vector<T>::chunked_managed_vector(chunked_managed_vector&& x) noexcept
        : _chunks(std::exchange(x._chunks, {}))
        , _size(std::exchange(x._size, 0))
        , _capacity(std::exchange(x._capacity, 0)) {
}

template <typename T>
template <typename Iterator>
chunked_managed_vector<T>::chunked_managed_vector(Iterator begin, Iterator end)
        : chunked_managed_vector() {
    auto is_random_access = std::is_base_of<std::random_access_iterator_tag, typename std::iterator_traits<Iterator>::iterator_category>::value;
    if (is_random_access) {
        reserve(std::distance(begin, end));
    }
    std::copy(begin, end, std::back_inserter(*this));
    if (!is_random_access) {
        shrink_to_fit();
    }
}

template <typename T>
chunked_managed_vector<T>::chunked_managed_vector(size_t n, const T& value) {
    reserve(n);
    std::fill_n(std::back_inserter(*this), n, value);
}


template <typename T>
chunked_managed_vector<T>&
chunked_managed_vector<T>::operator=(const chunked_managed_vector& x) {
    auto tmp = chunked_managed_vector(x);
    return *this = std::move(tmp);
}

template <typename T>
inline
chunked_managed_vector<T>&
chunked_managed_vector<T>::operator=(chunked_managed_vector&& x) noexcept {
    if (this != &x) {
        this->~chunked_managed_vector();
        new (this) chunked_managed_vector(std::move(x));
    }
    return *this;
}

template <typename T>
chunked_managed_vector<T>::~chunked_managed_vector() {
}

template <typename T>
typename chunked_managed_vector<T>::chunk_ptr
chunked_managed_vector<T>::new_chunk(size_t n) {
    managed_vector<T> p;
    p.reserve(n);
    return p;
}

template <typename T>
void
chunked_managed_vector<T>::migrate(T* begin, T* end, managed_vector<T>& result) {
    while (begin != end) {
        result.emplace_back(std::move(*begin));
        ++begin;
    }
}

template <typename T>
void
chunked_managed_vector<T>::make_room(size_t n, bool stop_after_one) {
    // First, if the last chunk is below max_chunk_capacity(), enlarge it

    auto last_chunk_capacity_deficit = _chunks.size() * max_chunk_capacity() - _capacity;
    if (last_chunk_capacity_deficit) {
        auto last_chunk_capacity = max_chunk_capacity() - last_chunk_capacity_deficit;
        auto capacity_increase = std::min(last_chunk_capacity_deficit, n - _capacity);
        auto new_last_chunk_capacity = last_chunk_capacity + capacity_increase;
        // FIXME: realloc? maybe not worth the complication; only works for PODs
        auto new_last_chunk = new_chunk(new_last_chunk_capacity);
        if (_size > _capacity - last_chunk_capacity) {
            migrate(addr(_capacity - last_chunk_capacity), addr(_size), new_last_chunk);
        }
        _chunks.back() = std::move(new_last_chunk);
        _capacity += capacity_increase;
    }

    // Reduce reallocations in the _chunks vector

    auto nr_chunks = (n + max_chunk_capacity() - 1) / max_chunk_capacity();
    _chunks.reserve(nr_chunks);

    // Add more chunks as needed

    bool stop = false;
    while (_capacity < n && !stop) {
        auto now = std::min(n - _capacity, max_chunk_capacity());
        _chunks.push_back(new_chunk(now));
        _capacity += now;
        stop = stop_after_one;
    }
}

template <typename T>
void
chunked_managed_vector<T>::do_reserve_for_push_back() {
    if (_capacity == 0) {
        // allocate a bit of room in case utilization will be low
        reserve(std::clamp(512 / sizeof(T), size_t(1), max_chunk_capacity()));
    } else if (_capacity < max_chunk_capacity() / 2) {
        // exponential increase when only one chunk to reduce copying
        reserve(_capacity * 2);
    } else {
        // add a chunk at a time later, since no copying will take place
        reserve((_capacity / max_chunk_capacity() + 1) * max_chunk_capacity());
    }
}

template <typename T>
void
chunked_managed_vector<T>::resize(size_t n) {
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

template <typename T>
void
chunked_managed_vector<T>::shrink_to_fit() {
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
        migrate(addr((_chunks.size() - 1) * max_chunk_capacity()), addr(_size), new_last_chunk);
        _chunks.back() = std::move(new_last_chunk);
        _capacity = _size;
    }
}

template <typename T>
void
chunked_managed_vector<T>::clear() {
    while (_size > 0) {
        pop_back();
    }
    shrink_to_fit();
}

}
