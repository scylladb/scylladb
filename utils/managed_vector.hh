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

#pragma once

#include <array>
#include <type_traits>

#include "utils/allocation_strategy.hh"

template<typename T, unsigned InternalSize = 0, typename SizeType = size_t>
class managed_vector {
    static_assert(std::is_nothrow_move_constructible<T>::value,
        "objects stored in managed_vector need to be nothrow move-constructible");
public:
    using value_type = T;
    using size_type = SizeType;
    using iterator = T*;
    using const_iterator = const T*;
private:
    struct external {
        managed_vector* _backref;
        T _data[0];

        external(external&& other) noexcept : _backref(other._backref) {
            for (unsigned i = 0; i < _backref->size(); i++) {
                new (_data + i) T(std::move(other._data[i]));
                other._data[i].~T();
            }
            _backref->_data = _data;
        }
        size_t storage_size() const noexcept {
            return sizeof(*this) + sizeof(T[_backref->_capacity]);
        }
    };
    union maybe_constructed {
        maybe_constructed() { }
        ~maybe_constructed() { }
        T object;
    };
private:
    std::array<maybe_constructed, InternalSize> _internal;
    size_type _size = 0;
    size_type _capacity = InternalSize;
    T* _data = reinterpret_cast<T*>(_internal.data());
    friend class external;
private:
    bool is_external() const {
        return _data != reinterpret_cast<const T*>(_internal.data());
    }
    external* get_external() {
        auto ptr = reinterpret_cast<char*>(_data) - offsetof(external, _data);
        return reinterpret_cast<external*>(ptr);
    }
    void maybe_grow(size_type new_size) {
        if (new_size <= _capacity) {
            return;
        }
        auto new_capacity = std::max({ _capacity + std::min(_capacity, size_type(1024)), new_size, size_type(InternalSize + 8) });
        reserve(new_capacity);
    }
    void clear_and_release() noexcept {
        clear();
        if (is_external()) {
            current_allocator().free(get_external(), get_external()->storage_size());
        }
    }
public:
    managed_vector() = default;
    managed_vector(const managed_vector& other) {
        reserve(other._size);
        try {
            for (const auto& v : other) {
                push_back(v);
            }
        } catch (...) {
            clear_and_release();
            throw;
        }
    }
    managed_vector(managed_vector&& other) noexcept : _size(other._size), _capacity(other._capacity) {
        if (other.is_external()) {
            _data = other._data;
            other._data = reinterpret_cast<T*>(other._internal.data());
            get_external()->_backref = this;
        } else {
            for (unsigned i = 0; i < _size; i++) {
                new (_data + i) T(std::move(other._data[i]));
                other._data[i].~T();
            }
        }
        other._size = 0;
        other._capacity = InternalSize;
    }

    managed_vector& operator=(const managed_vector& other) {
        if (this != &other) {
            managed_vector tmp(other);
            this->~managed_vector();
            new (this) managed_vector(std::move(tmp));
        }
        return *this;
    }
    managed_vector& operator=(managed_vector&& other) noexcept {
        if (this != &other) {
            this->~managed_vector();
            new (this) managed_vector(std::move(other));
        }
        return *this;
    }

    ~managed_vector() {
        clear_and_release();
    }

    T& at(size_type pos) {
        if (pos >= _size) {
            throw std::out_of_range("out of range");
        }
        return operator[](pos);
    }
    const T& at(size_type pos) const {
        if (pos >= _size) {
            throw std::out_of_range("out of range");
        }
        return operator[](pos);
    }
    T& operator[](size_type pos) noexcept {
        return _data[pos];
    }
    const T& operator[](size_type pos) const noexcept {
        return _data[pos];
    }

    T& front() noexcept { return *_data; }
    const T& front() const noexcept { return *_data;  }
    T& back() noexcept { return _data[_size - 1]; }
    const T& back() const noexcept { return _data[_size - 1]; }

    T* data() noexcept { return _data; }
    const T* data() const noexcept { return _data; }

    iterator begin() noexcept { return _data; }
    const_iterator begin() const noexcept { return _data; }
    const_iterator cbegin() const noexcept { return _data; }
    iterator end() noexcept { return _data + _size; }
    const_iterator end() const noexcept { return _data + _size; }
    const_iterator cend() const noexcept { return _data + _size; }

    bool empty() const noexcept { return !_size; }
    size_type size() const noexcept { return _size; }
    size_type capacity() const noexcept { return _capacity; }

    void clear() {
        while (_size) {
            pop_back();
        }
    }
    void reserve(size_type new_capacity) {
        if (new_capacity <= _capacity) {
            return;
        }
        auto ptr = current_allocator().alloc<external>(sizeof(external) + sizeof(T) * new_capacity);
        auto ext = static_cast<external*>(ptr);
        ext->_backref = this;
        T* data_ptr = ext->_data;
        for (unsigned i = 0; i < _size; i++) {
            new (data_ptr + i) T(std::move(_data[i]));
            _data[i].~T();
        }
        if (is_external()) {
            current_allocator().free(get_external(), get_external()->storage_size());
        }
        _data = data_ptr;
        _capacity = new_capacity;
    }

    iterator erase(iterator it) {
        std::move(it + 1, end(), it);
        _data[_size - 1].~T();
        _size--;
        return it;
    }

    void push_back(const T& value) {
        emplace_back(value);
    }
    void push_back(T&& value) {
        emplace_back(std::move(value));
    }
    template<typename... Args>
    T& emplace_back(Args&&... args) {
        maybe_grow(_size + 1);
        T* elem = new (_data + _size) T(std::forward<Args>(args)...);
        _size++;
        return *elem;
    }
    void pop_back() {
        _data[_size - 1].~T();
        _size--;
    }

    void resize(size_type new_size) {
        maybe_grow(new_size);
        while (_size > new_size) {
            pop_back();
        }
        while (_size < new_size) {
            emplace_back();
        }
    }
    void resize(size_type new_size, const T& value) {
        maybe_grow(new_size);
        while (_size > new_size) {
            pop_back();
        }
        while (_size < new_size) {
            push_back(value);
        }
    }

    // Returns the amount of external memory used to hold inserted items.
    // Ignores reserved space.
    size_t used_space_external_memory_usage() const {
        if (is_external()) {
            return sizeof(external) + _size * sizeof(T);
        }
        return 0;
    }
};
