/*
 * Copyright (C) 2016 ScyllaDB
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

#include <iterator>

template<typename T>
class anchorless_list_base_hook {
    anchorless_list_base_hook<T>* _next = nullptr;
    anchorless_list_base_hook<T>* _prev = nullptr;
public:
    class iterator : std::iterator<std::bidirectional_iterator_tag, T> {
        anchorless_list_base_hook<T>* _position;
    public:
        explicit iterator(anchorless_list_base_hook<T>* pos) : _position(pos) { }
        T& operator*() { return *static_cast<T*>(_position); }
        T* operator->() { return static_cast<T*>(_position); }
        iterator& operator++() {
            _position = _position->_next;
            return *this;
        }
        iterator operator++(int) {
            iterator it = *this;
            operator++();
            return it;
        }
        iterator& operator--() {
            _position = _position->_prev;
            return *this;
        }
        iterator operator--(int) {
            iterator it = *this;
            operator--();
            return it;
        }
        bool operator==(const iterator& other) {
            return _position == other._position;
        }
        bool operator!=(const iterator& other) {
            return !(*this == other);
        }
    };

    class range {
        anchorless_list_base_hook<T>* _begin;
        anchorless_list_base_hook<T>* _end;
    public:
        range(anchorless_list_base_hook<T>* b, anchorless_list_base_hook<T>* e)
            : _begin(b), _end(e) { }
        iterator begin() { return iterator(_begin); }
        iterator end() { return iterator(_end); }
    };
public:
    anchorless_list_base_hook() = default;
    anchorless_list_base_hook(const anchorless_list_base_hook&) = delete;
    anchorless_list_base_hook(anchorless_list_base_hook&& other) noexcept
        : _next(other._next)
        , _prev(other._prev)
    {
        if (_next) {
            _next->_prev = this;
        }
        if (_prev) {
            _prev->_next = this;
        }
        other._next = nullptr;
        other._prev = nullptr;
    }
    anchorless_list_base_hook& operator=(const anchorless_list_base_hook&) = delete;
    anchorless_list_base_hook& operator=(anchorless_list_base_hook&& other) noexcept
    {
        if (this != &other) {
            this->~anchorless_list_base_hook();
            new (this) anchorless_list_base_hook(std::move(other));
        }
        return *this;
    }
    ~anchorless_list_base_hook() {
        erase();
    }
    // Inserts this after elem.
    void insert_after(T& elem) {
        auto e = static_cast<anchorless_list_base_hook*>(&elem);
        _next = e->_next;
        e->_next = this;
        _prev = e;
        if (_next) {
            _next->_prev = this;
        }
    }
    // Inserts this before elem.
    void insert_before(T& elem) {
        auto e = static_cast<anchorless_list_base_hook*>(&elem);
        _prev = e->_prev;
        e->_prev = this;
        _next = e;
        if (_prev) {
            _prev->_next = this;
        }
    }
    void erase() {
        if (_next) {
            _next->_prev = _prev;
        }
        if (_prev) {
            _prev->_next = _next;
        }
        _next = nullptr;
        _prev = nullptr;
    }
    bool is_front() const { return !_prev; }
    bool is_back() const { return !_next; }
    bool is_single() const { return is_front() && is_back(); }
    T* next() const {
        return static_cast<T*>(_next);
    }
    T* prev() const {
        return static_cast<T*>(_prev);
    }
    iterator iterator_to() {
        return iterator(this);
    }
    range all_elements() {
        auto begin = this;
        while (begin->_prev) {
            begin = begin->_prev;
        }
        return range(begin, nullptr);
    }
    range elements_from_this() {
        return range(this, nullptr);
    }
};
