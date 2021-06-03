/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <new>
#include <utility>
#include <algorithm>
#include <initializer_list>
#include <memory>
#include <stdexcept>

namespace utils {

/// A vector with small buffer optimisation
///
/// small_vector is a variation of std::vector<> that reserves a configurable
/// amount of storage internally, without the need for memory allocation.
/// This can bring measurable gains if the expected number of elements is
/// small. The drawback is that moving such small_vector is more expensive
/// and invalidates iterators as well as references which disqualifies it in
/// some cases.
///
/// All member functions of small_vector provide strong exception guarantees.
///
/// It is unspecified when small_vector is going to use internal storage, except
/// for the obvious case when size() > N. In other situations user must not
/// attempt to guess if data is stored internally or externally. The same applies
/// to capacity(). Apart from the obvious fact that capacity() >= size() the user
/// must not assume anything else. In particular it may not always hold that
/// capacity() >= N.
///
/// Unless otherwise specified (e.g. move ctor and assignment) small_vector
/// provides guarantees at least as strong as those of std::vector<>.
template<typename T, size_t N>
class small_vector {
    static_assert(N > 0);
    static_assert(std::is_nothrow_move_constructible_v<T>);
    static_assert(std::is_nothrow_move_assignable_v<T>);
    static_assert(std::is_nothrow_destructible_v<T>);

private:
    T* _begin;
    T* _end;
    T* _capacity_end;

    // Use union instead of std::aligned_storage so that debuggers can see
    // the contained objects without needing any pretty printers.
    union internal {
        internal() { }
        ~internal() { }
        T storage[N];
    };
    internal _internal;

private:
    bool uses_internal_storage() const noexcept {
        return _begin == _internal.storage;
    }

    [[gnu::cold]] [[gnu::noinline]]
    void expand(size_t new_capacity) {
        auto ptr = static_cast<T*>(::aligned_alloc(alignof(T), new_capacity * sizeof(T)));
        if (!ptr) {
            throw std::bad_alloc();
        }
        auto n_end = std::uninitialized_move(begin(), end(), ptr);
        std::destroy(begin(), end());
        if (!uses_internal_storage()) {
            std::free(_begin);
        }
        _begin = ptr;
        _end = n_end;
        _capacity_end = ptr + new_capacity;
    }

    [[gnu::cold]] [[gnu::noinline]]
    void slow_copy_assignment(const small_vector& other) {
        auto ptr = static_cast<T*>(::aligned_alloc(alignof(T), other.size() * sizeof(T)));
        if (!ptr) {
            throw std::bad_alloc();
        }
        auto n_end = ptr;
        try {
            n_end = std::uninitialized_copy(other.begin(), other.end(), n_end);
        } catch (...) {
            std::free(ptr);
            throw;
        }
        std::destroy(begin(), end());
        if (!uses_internal_storage()) {
            std::free(_begin);
        }
        _begin = ptr;
        _end = n_end;
        _capacity_end = n_end;
    }

    void reserve_at_least(size_t n) {
        if (__builtin_expect(_begin + n > _capacity_end, false)) {
            expand(std::max(n, capacity() * 2));
        }
    }

    [[noreturn]] [[gnu::cold]] [[gnu::noinline]]
    void throw_out_of_range() {
        throw std::out_of_range("out of range small vector access");
    }

public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;

    using iterator = T*;
    using const_iterator = const T*;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    small_vector() noexcept
        : _begin(_internal.storage)
        , _end(_begin)
        , _capacity_end(_begin + N)
    { }

    template<typename InputIterator>
    small_vector(InputIterator first, InputIterator last) : small_vector() {
        if constexpr (std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<InputIterator>::iterator_category>) {
            reserve(std::distance(first, last));
            _end = std::uninitialized_copy(first, last, _end);
        } else {
            std::copy(first, last, std::back_inserter(*this));
        }
    }

    small_vector(std::initializer_list<T> list) : small_vector(list.begin(), list.end()) { }

    // May invalidate iterators and references.
    small_vector(small_vector&& other) noexcept {
        if (other.uses_internal_storage()) {
            _begin = _internal.storage;
            _capacity_end = _begin + N;
            if constexpr (std::is_trivially_copyable_v<T>) {
                // Compilers really like loops with the number of iterations known at
                // the compile time, the usually emit less code which can be more aggressively
                // optimised. Since we can assume that N is small it is most likely better
                // to just copy everything, regardless of how many elements are actually in
                // the vector.
                std::memcpy(_internal.storage, other._internal.storage, N * sizeof(T));
                _end = _begin + other.size();
            } else {
                _end = _begin;

                // What we would really like here is std::uninintialized_move_and_destroy.
                // It is beneficial to do move and destruction in a single pass since the compiler
                // may be able to merge those operations (e.g. the destruction of a move-from
                // std::unique_ptr is a no-op).
                for (auto& e : other) {
                    new (_end++) T(std::move(e));
                    e.~T();
                }
            }
            other._end = other._internal.storage;
        } else {
            _begin = std::exchange(other._begin, other._internal.storage);
            _end = std::exchange(other._end, other._internal.storage);
            _capacity_end = std::exchange(other._capacity_end, other._internal.storage + N);
        }
    }

    small_vector(const small_vector& other) noexcept : small_vector() {
        reserve(other.size());
        _end = std::uninitialized_copy(other.begin(), other.end(), _end);
    }

    // May invalidate iterators and references.
    small_vector& operator=(small_vector&& other) noexcept {
        clear();
        if (other.uses_internal_storage()) {
            if (__builtin_expect(!uses_internal_storage(), false)) {
                std::free(_begin);
                _begin = _internal.storage;
            }
            _capacity_end = _begin + N;
            if constexpr (std::is_trivially_copyable_v<T>) {
                std::memcpy(_internal.storage, other._internal.storage, N * sizeof(T));
                _end = _begin + other.size();
            } else {
                _end = _begin;

                // Better to use single pass than std::uninitialize_move + std::destroy.
                // See comment in move ctor for details.
                for (auto& e : other) {
                    new (_end++) T(std::move(e));
                    e.~T();
                }
            }
            other._end = other._internal.storage;
        } else {
            if (__builtin_expect(!uses_internal_storage(), false)) {
                std::free(_begin);
            }
            _begin = std::exchange(other._begin, other._internal.storage);
            _end = std::exchange(other._end, other._internal.storage);
            _capacity_end = std::exchange(other._capacity_end, other._internal.storage + N);
        }
        return *this;
    }

    small_vector& operator=(const small_vector& other) {
        if constexpr (std::is_nothrow_copy_constructible_v<T>) {
            if (capacity() >= other.size()) {
                clear();
                _end = std::uninitialized_copy(other.begin(), other.end(), _end);
                return *this;
            }
        }
        slow_copy_assignment(other);
        return *this;
    }

    ~small_vector() {
        clear();
        if (__builtin_expect(!uses_internal_storage(), false)) {
            std::free(_begin);
        }
    }

    void reserve(size_t n) {
        if (__builtin_expect(_begin + n > _capacity_end, false)) {
            expand(n);
        }
    }

    void clear() noexcept {
        std::destroy(_begin, _end);
        _end = _begin;
    }

    iterator begin() noexcept { return _begin; }
    const_iterator begin() const noexcept { return _begin; }
    const_iterator cbegin() const noexcept { return _begin; }

    iterator end() noexcept { return _end; }
    const_iterator end() const noexcept { return _end; }
    const_iterator cend() const noexcept { return _end; }

    reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }
    const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }
    const_reverse_iterator crbegin() const noexcept { return const_reverse_iterator(end()); }

    reverse_iterator rend() noexcept { return reverse_iterator(begin()); }
    const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }
    const_reverse_iterator crend() const noexcept { return const_reverse_iterator(begin()); }

    T* data() noexcept { return _begin; }
    const T* data() const noexcept { return _begin; }

    T& front() noexcept { return *begin(); }
    const T& front() const noexcept { return *begin(); }

    T& back() noexcept { return end()[-1]; }
    const T& back() const noexcept { return end()[-1]; }

    T& operator[](size_t idx) noexcept { return data()[idx]; }
    const T& operator[](size_t idx) const noexcept { return data()[idx]; }

    T& at(size_t idx) {
        if (__builtin_expect(idx >= size(), false)) {
            throw_out_of_range();
        }
        return operator[](idx);
    }
    const T& at(size_t idx) const {
        if (__builtin_expect(idx >= size(), false)) {
            throw_out_of_range();
        }
        return operator[](idx);
    }

    bool empty() const noexcept { return _begin == _end; }
    size_t size() const noexcept { return _end - _begin; }
    size_t capacity() const noexcept { return _capacity_end - _begin; }

    template<typename... Args>
    T& emplace_back(Args&&... args) {
        if (__builtin_expect(_end == _capacity_end, false)) {
            expand(std::max<size_t>(capacity() * 2, 1));
        }
        auto& ref = *new (_end) T(std::forward<Args>(args)...);
        ++_end;
        return ref;
    }

    T& push_back(const T& value) {
        return emplace_back(value);
    }

    T& push_back(T&& value) {
        return emplace_back(std::move(value));
    }

    template<typename InputIterator>
    iterator insert(const_iterator cpos, InputIterator first, InputIterator last) {
        if constexpr (std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<InputIterator>::iterator_category>) {
            if (first == last) {
                return const_cast<iterator>(cpos);
            }
            auto idx = cpos - _begin;
            auto new_count = std::distance(first, last);
            reserve_at_least(size() + new_count);
            auto pos = _begin + idx;
            auto after = std::distance(pos, end());
            if (__builtin_expect(pos == end(), true)) {
                _end = std::uninitialized_copy(first, last, end());
                return pos;
            } else if (after > new_count) {
                std::uninitialized_move(end() - new_count, end(), end());
                std::move_backward(pos, end() - new_count, end());
                try {
                    std::copy(first, last, pos);
                } catch (...) {
                    std::move(pos + new_count, end() + new_count, pos);
                    std::destroy(end(), end() + new_count);
                    throw;
                }
            } else {
                std::uninitialized_move(pos, end(), pos + new_count);
                auto mid = std::next(first, after);
                try {
                    std::uninitialized_copy(mid, last, end());
                    try {
                        std::copy(first, mid, pos);
                    } catch (...) {
                        std::destroy(end(), pos + new_count);
                        throw;
                    }
                } catch (...) {
                    std::move(pos + new_count, end() + new_count, pos);
                    std::destroy(pos + new_count, end() + new_count);
                    throw;
                }

            }
            _end += new_count;
            return pos;
        } else {
            auto start = cpos - _begin;
            auto idx = start;
            while (first != last) {
                try {
                    insert(begin() + idx, *first);
                    ++first;
                    ++idx;
                } catch (...) {
                    erase(begin() + start, begin() + idx);
                    throw;
                }
            }
            return begin() + idx;
        }
    }

    template<typename... Args>
    iterator emplace(const_iterator cpos, Args&&... args) {
        auto idx = cpos - _begin;
        reserve_at_least(size() + 1);
        auto pos = _begin + idx;
        if (pos != _end) {
            new (_end) T(std::move(_end[-1]));
            std::move_backward(pos, _end - 1, _end);
            pos->~T();
        }
        try {
            new (pos) T(std::forward<Args>(args)...);
        } catch (...) {
            if (pos != _end) {
                new (pos) T(std::move(pos[1]));
                std::move(pos + 2, _end + 1, pos + 1);
                _end->~T();
            }
            throw;
        }
        _end++;
        return pos;
    }

    iterator insert(const_iterator cpos, const T& obj) {
        return emplace(cpos, obj);
    }

    iterator insert(const_iterator cpos, T&& obj) {
        return emplace(cpos, std::move(obj));
    }

    void resize(size_t n) {
        if (n < size()) {
            erase(end() - (size() - n), end());
        } else if (n > size()) {
            reserve_at_least(n);
            _end = std::uninitialized_value_construct_n(_end, n - size());
        }
    }

    void resize(size_t n, const T& value) {
        if (n < size()) {
            erase(end() - (size() - n), end());
        } else if (n > size()) {
            reserve_at_least(n);
            auto nend = _begin + n;
            std::uninitialized_fill(_end, nend, value);
            _end = nend;
        }
    }

    void pop_back() noexcept {
        (--_end)->~T();
    }

    iterator erase(const_iterator cit) noexcept {
        return erase(cit, cit + 1);
    }

    iterator erase(const_iterator cfirst, const_iterator clast) noexcept {
        auto first = const_cast<iterator>(cfirst);
        auto last = const_cast<iterator>(clast);
        std::move(last, end(), first);
        auto nend = _end - (clast - cfirst);
        std::destroy(nend, _end);
        _end = nend;
        return first;
    }

    void swap(small_vector& other) noexcept {
        std::swap(*this, other);
    }

    bool operator==(const small_vector& other) const noexcept {
        return size() == other.size() && std::equal(_begin, _end, other.begin());
    }

    bool operator!=(const small_vector& other) const noexcept {
        return !(*this == other);
    }
};

template <typename T, size_t N>
std::ostream& operator<<(std::ostream& os, const utils::small_vector<T, N>& v) {
    os << "{";
    bool first = true;
    for (auto&& e : v) {
        if (!first) {
            os << ", ";
        }
        first = false;
        os << e;
    }
    os << "}";
    return os;
}

}
