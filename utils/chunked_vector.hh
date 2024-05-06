/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// chunked_vector is a vector-like container that uses discontiguous storage.
// It provides fast random access, the ability to append at the end, and aims
// to avoid large contiguous allocations - unlike std::vector which allocates
// all the data in one contiguous allocation.
//
// std::deque aims to achieve the same goals, but its implementation in
// libstdc++ still results in large contiguous allocations: std::deque
// keeps the items in small (512-byte) chunks, and then keeps a contiguous
// vector listing these chunks. This chunk vector can grow pretty big if the
// std::deque grows big: When an std::deque contains just 8 MB of data, it
// needs 16384 chunks, and the vector listing those needs 128 KB.
//
// Therefore, in chunked_vector we use much larger 128 KB chunks (this is
// configurable, with the max_contiguous_allocation template parameter).
// With 128 KB chunks, the contiguous vector listing them is 256 times
// smaller than it would be in std::dequeue with its 512-byte chunks.
//
// In particular, when a chunked_vector stores up to 2 GB of data, the
// largest contiguous allocation is guaranteed to be 128 KB: 2 GB of data
// fits in 16384 chunks of 128 KB each, and the vector of 16384 8-byte
// pointers requires another 128 KB allocation.
//
// Remember, however, that when the chunked_vector grows beyond 2 GB, its
// largest contiguous allocation (used to store the chunk list) continues to
// grow as O(N). This is not a problem for current real-world uses of
// chunked_vector which never reach 2 GB.
//
// Always allocating large 128 KB chunks can be wasteful for small vectors;
// This is why std::deque chose small 512-byte chunks. chunked_vector solves
// this problem differently: It makes the last chunk variable in size,
// possibly smaller than a full 128 KB.

#include "utils/small_vector.hh"

#include <boost/range/algorithm/equal.hpp>
#include <boost/version.hpp>
#include <memory>
#include <type_traits>
#include <iterator>
#include <utility>
#include <algorithm>
#include <stdexcept>
#include <malloc.h>
#include <fmt/ostream.h>

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
public:
    // Maximum number of T elements fitting in a single chunk.
    static constexpr size_t max_chunk_capacity() {
        return std::max(max_contiguous_allocation / sizeof(T), size_t(1));
    }
    // Minimum number of T elements allocated in the first chunk.
    static constexpr size_t min_chunk_capacity() {
        return std::clamp(512 / sizeof(T), size_t(1), max_chunk_capacity());
    }
private:
    std::pair<chunk_ptr, size_t> get_chunk_before_emplace_back();
    void set_chunk_after_emplace_back(std::pair<chunk_ptr, size_t>) noexcept;
    void make_room(size_t n, bool stop_after_one);
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
    // Moving a chunked_vector invalidates all iterators to it
    chunked_vector(chunked_vector&& x) noexcept;
    template <typename Iterator>
    chunked_vector(Iterator begin, Iterator end);
    chunked_vector(std::initializer_list<T> x);
    explicit chunked_vector(size_t n, const T& value = T());
    ~chunked_vector();
    chunked_vector& operator=(const chunked_vector& x);
    // Moving a chunked_vector invalidates all iterators to it
    chunked_vector& operator=(chunked_vector&& x) noexcept;

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
        emplace_back(x);
    }
    void push_back(T&& x) {
        emplace_back(std::move(x));
    }
    template <typename... Args>
    T& emplace_back(Args&&... args) {
        pointer ret;
        if (_capacity > _size) {
            ret = new (addr(_size)) T(std::forward<Args>(args)...);
        } else {
            // Allocate an empty new chunk for the new element.
            // It's ok to lose it if `new` below throws as
            // there are still no side effects on existing elements.
            auto x = get_chunk_before_emplace_back();
            // Emplace the new element in the newly allocated chunk.
            ret = new (x.first.get() + _size % max_chunk_capacity()) T(std::forward<Args>(args)...);
            // Setting the new chunk back is noexcept,
            // as throwing at this stage may lose data if emplacing used the move-constructor.
            set_chunk_after_emplace_back(std::move(x));
        }
        ++_size;
        return *ret;
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
            make_room(n, true);
        }
    }

    size_t memory_size() const {
        return _capacity * sizeof(T);
    }

    size_t external_memory_usage() const;
public:
    template <class ValueType>
    class iterator_type {
        // Note that _chunks points to the chunked_vector::_chunks data
        // and therefore it is invalidated when the chunked_vector is moved
        const chunk_ptr* _chunks = nullptr;
        size_t _i = 0;
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
    bool operator==(const chunked_vector& x) const {
        return boost::equal(*this, x);
    }
};

template<typename T, size_t max_contiguous_allocation>
size_t chunked_vector<T, max_contiguous_allocation>::external_memory_usage() const {
    size_t result = 0;
    for (auto&& chunk : _chunks) {
        result += ::malloc_usable_size(chunk.get());
    }
    return result;
}

template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>::chunked_vector(const chunked_vector& x)
        : chunked_vector() {
    auto size = x.size();
    reserve(size);
    for (size_t i = 0; _size < size; ++i) {
        const T* src = x._chunks[i].get();
        T* dst = _chunks[i].get();
        auto now = std::min(size - _size, max_chunk_capacity());
        std::uninitialized_copy_n(src, now, dst);
        // Update _size incrementally to let the destructor
        // know how much data to destroy on exception
        _size += now;
    }
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
chunked_vector<T, max_contiguous_allocation>::chunked_vector(std::initializer_list<T> x)
        : chunked_vector() {
    reserve(x.size());
    std::copy(x.begin(), x.end(), std::back_inserter(*this));
}

template <typename T, size_t max_contiguous_allocation>
chunked_vector<T, max_contiguous_allocation>::chunked_vector(size_t n, const T& value)
        : chunked_vector() {
    reserve(n);
    for (auto cp = _chunks.begin(); _size < n; ++cp) {
        auto now = std::min(n - _size, max_chunk_capacity());
        std::uninitialized_fill_n(cp->get(), now, value);
        _size += now;
    }
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
        for (auto cp = _chunks.begin(); _size; ++cp) {
            auto now = std::min(_size, max_chunk_capacity());
            std::destroy_n(cp->get(), now);
            _size -= now;
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
    std::uninitialized_move(begin, end, result);
    std::destroy(begin, end);
}

template <typename T, size_t max_contiguous_allocation>
void
chunked_vector<T, max_contiguous_allocation>::make_room(size_t n, bool stop_after_one) {
    // First, if the last chunk is below max_chunk_capacity(), enlarge it

    auto last_chunk_capacity_deficit = _chunks.size() * max_chunk_capacity() - _capacity;
    if (last_chunk_capacity_deficit) {
        auto last_chunk_capacity = max_chunk_capacity() - last_chunk_capacity_deficit;
        auto capacity_increase = std::min(last_chunk_capacity_deficit, n - _capacity);
        auto new_last_chunk_capacity = last_chunk_capacity + capacity_increase;
        // FIXME: realloc? maybe not worth the complication; only works for PODs
        auto new_last_chunk = new_chunk(new_last_chunk_capacity);
        if (_size > _capacity - last_chunk_capacity) {
            migrate(addr(_capacity - last_chunk_capacity), addr(_size), new_last_chunk.get());
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

template <typename T, size_t max_contiguous_allocation>
std::pair<typename chunked_vector<T, max_contiguous_allocation>::chunk_ptr, size_t> chunked_vector<T, max_contiguous_allocation>::get_chunk_before_emplace_back() {
    // Allocate a new chunk that will either replace the first, non-full chunk
    // or will be appended to the chunks list
    size_t new_chunk_capacity;
    if (_capacity == 0) {
        // This is the first allocation in the chunked_vector, therefore
        // allocate a bit of room in case utilization will be low
        new_chunk_capacity = min_chunk_capacity();
    } else if (_capacity < max_chunk_capacity() / 2) {
        // We're expanding the first chunk now,
        // exponential increase when only one chunk to reduce copying
        new_chunk_capacity = _capacity * 2;
    } else {
        // This case is for expanding the first chunk to max_chunk_capacity, or when adding more chunks, so
        // add a chunk at a time later, since no copying will take place
        new_chunk_capacity = max_chunk_capacity();
        if (_chunks.capacity() == _chunks.size()) {
            // Ensure place for the new chunk before emplacing the new element
            // Since emplacing the new chunk_ptr into _chunks below must not throw.
            _chunks.reserve(_chunks.size() * 2);
        }
    }
    return std::make_pair(new_chunk(new_chunk_capacity), new_chunk_capacity);
}

template <typename T, size_t max_contiguous_allocation>
void chunked_vector<T, max_contiguous_allocation>::set_chunk_after_emplace_back(std::pair<chunk_ptr, size_t> x) noexcept {
    auto new_chunk_ptr = std::move(x.first);
    auto new_chunk_capacity = x.second;
    // If the new chunk is replacing the first chunk, migrate the existing elements onto it.
    // Otherwise, just append it to the _chunks vector.
    // Note that this part must not throw, since we've already emplaced the new element into the
    // vector. If we lose the new_chunk now, we might lose data if we the new element was move-constructed.
    auto last_chunk_size = _size % max_chunk_capacity();
    if (last_chunk_size) {
        // We're reallocating the last chunk, so migrate the existing elements to the newly allocated chunk.
        // This is safe since we require that the values are nothrow_move_constructible.
        migrate(_chunks.back().get(), _chunks.back().get() + last_chunk_size, new_chunk_ptr.get());
        _chunks.back() = std::move(new_chunk_ptr);
    } else {
        // If all (or none) of the existing chunks are full,
        // the new chunk will contain only the emplaced element,
        // and so we just need to append it to _chunks,
        // without migrating any existing elements.
        _chunks.emplace_back(std::move(new_chunk_ptr));
    }
    // `(_chunks.size() - 1) * max_chunk_capacity()` is the capacity of all chunks except the last chunk.
    // If this is the first chunk - that part would be 0.
    // Add to it the last chunk `new_chunk_capacity`.
    _capacity = (_chunks.size() - 1) * max_chunk_capacity() + new_chunk_capacity;
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

template <typename T, size_t max_contiguous_allocation>
std::ostream& operator<<(std::ostream& os, const chunked_vector<T, max_contiguous_allocation>& v) {
    fmt::print(os, "{}", v);
    return os;
}

}
