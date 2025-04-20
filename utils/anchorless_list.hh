/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <iterator>

template<typename T>
class anchorless_list_base_hook {
    anchorless_list_base_hook<T>* _next = nullptr;
    anchorless_list_base_hook<T>* _prev = nullptr;
public:
    template <typename ValueType>
    class iterator {
        anchorless_list_base_hook<T>* _position;
    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = ValueType;
        using difference_type = ssize_t;
        using pointer = ValueType*;
        using reference = ValueType&;
    public:
        explicit iterator(anchorless_list_base_hook<T>* pos) : _position(pos) { }
        ValueType& operator*() { return *static_cast<ValueType*>(_position); }
        ValueType* operator->() { return static_cast<ValueType*>(_position); }
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
        bool operator==(const iterator&) const = default;
    };

    template <typename ValueType>
    class reverse_iterator {
        anchorless_list_base_hook<T>* _position;
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = ValueType;
        using difference_type = ssize_t;
        using pointer = ValueType*;
        using reference = ValueType&;
    public:
        explicit reverse_iterator(anchorless_list_base_hook<T>* pos) : _position(pos) { }
        ValueType& operator*() { return *static_cast<ValueType*>(_position); }
        ValueType* operator->() { return static_cast<ValueType*>(_position); }
        reverse_iterator& operator++() {
            _position = _position->_prev;
            return *this;
        }
        reverse_iterator operator++(int) {
            reverse_iterator it = *this;
            operator++();
            return it;
        }
        bool operator==(const reverse_iterator& other) const = default;
    };

    class range {
        anchorless_list_base_hook<T>* _begin;
        anchorless_list_base_hook<T>* _end;
    public:
        using iterator = anchorless_list_base_hook::iterator<T>;
        using const_iterator = anchorless_list_base_hook::iterator<const T>;
        range(anchorless_list_base_hook<T>* b, anchorless_list_base_hook<T>* e)
            : _begin(b), _end(e) { }
        iterator begin() { return iterator(_begin); }
        iterator end() { return iterator(_end); }
        const_iterator begin() const { return const_iterator(_begin); }
        const_iterator end() const { return const_iterator(_end); }
    };

    class reversed_range {
        anchorless_list_base_hook<T>* _begin;
        anchorless_list_base_hook<T>* _end;
    public:
        using iterator = anchorless_list_base_hook::reverse_iterator<T>;
        using const_iterator = anchorless_list_base_hook::reverse_iterator<const T>;
        reversed_range(anchorless_list_base_hook<T>* b, anchorless_list_base_hook<T>* e)
            : _begin(b), _end(e) { }
        iterator begin() { return iterator(_begin); }
        iterator end() { return iterator(_end); }
        const_iterator begin() const { return const_iterator(_begin); }
        const_iterator end() const { return const_iterator(_end); }
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
    // Inserts the chain starting at head after this elem.
    // Assumes !head.prev() and !next().
    void splice(T& head) {
        auto e = static_cast<anchorless_list_base_hook*>(&head);
        _next = e;
        e->_prev = this;
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
    T* last() const {
        // FIXME: Optimize
        auto v = this;
        while (v->_next) {
            v = v->_next;
        }
        return const_cast<T*>(static_cast<const T*>(v));
    }
    iterator<T> iterator_to() {
        return iterator<T>(this);
    }
    range all_elements() {
        auto begin = this;
        while (begin->_prev) {
            begin = begin->_prev;
        }
        return range(begin, nullptr);
    }
    reversed_range all_elements_reversed() {
        return reversed_range(last(), nullptr);
    }
    range elements_from_this() {
        return range(this, nullptr);
    }
};
