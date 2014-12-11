/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CIRCULAR_BUFFER_HH_
#define CIRCULAR_BUFFER_HH_

// A growable double-ended queue container that can be efficiently
// extended (and shrunk) from both ends.  Implementation is a single
// storage vector.
//
// Similar to libstdc++'s std::deque, except that it uses a single level
// store, and so is more efficient for simple stored items.
// Similar to boost::circular_buffer_space_optimized, except it uses
// uninitialized storage for unoccupied elements (and thus move/copy
// constructors instead of move/copy assignments, which are less efficient).

#include "transfer.hh"
#include <memory>
#include <algorithm>

template <typename T, typename Alloc = std::allocator<T>>
class circular_buffer {
    struct impl : Alloc {
        T* storage = nullptr;
        T* begin = nullptr;
        T* end = nullptr;  // never points at storage+capacity
        size_t size = 0;
        size_t capacity = 0;
    };
    impl _impl;
public:
    using value_type = T;
    using size_type = size_t;
    using reference = T&;
    using pointer = T*;
    using const_reference = const T&;
    using const_pointer = const T*;
public:
    circular_buffer() = default;
    circular_buffer(circular_buffer&& X);
    circular_buffer(const circular_buffer& X) = delete;
    ~circular_buffer();
    circular_buffer& operator=(const circular_buffer&) = delete;
    circular_buffer& operator=(circular_buffer&&) = delete;
    void push_front(const T& data);
    void push_front(T&& data);
    template <typename... A>
    void emplace_front(A... args);
    void push_back(const T& data);
    void push_back(T&& data);
    template <typename... A>
    void emplace_back(A... args);
    T& front();
    T& back();
    void pop_front();
    void pop_back();
    bool empty() const;
    size_t size() const;
    size_t capacity() const;
    T& operator[](size_t idx);
    template <typename Func>
    void for_each(Func func);
private:
    void expand();
    void maybe_expand(size_t nr = 1);
    T* pre_push_front();
    T* pre_push_back();
    void post_push_front(T* p);
    void post_push_back(T* p);
};

template <typename T, typename Alloc>
inline
bool
circular_buffer<T, Alloc>::empty() const {
    return _impl.begin == _impl.end;
}

template <typename T, typename Alloc>
inline
size_t
circular_buffer<T, Alloc>::size() const {
    return _impl.size;
}

template <typename T, typename Alloc>
inline
size_t
circular_buffer<T, Alloc>::capacity() const {
    // we never use all of the elements, since end == begin means empty
    return _impl.capacity ? _impl.capacity - 1 : 0;
}

template <typename T, typename Alloc>
inline
circular_buffer<T, Alloc>::circular_buffer(circular_buffer&& x)
    : _impl(std::move(x._impl)) {
    x._impl = {};
}

template <typename T, typename Alloc>
template <typename Func>
inline
void
circular_buffer<T, Alloc>::for_each(Func func) {
    auto p = _impl.begin;
    auto e = _impl.storage + _impl.capacity;
    if (p > _impl.end) {
        while (p < e) {
            func(*p++);
        }
        p = _impl.storage;
    }
    while (p < _impl.end) {
        func(*p++);
    }
}

template <typename T, typename Alloc>
inline
circular_buffer<T, Alloc>::~circular_buffer() {
    for_each([this] (T& obj) {
        _impl.destroy(&obj);
    });
    _impl.deallocate(_impl.storage, _impl.capacity);
}

template <typename T, typename Alloc>
void
circular_buffer<T, Alloc>::expand() {
    auto new_cap = std::max<size_t>(_impl.capacity * 2, 2);
    auto new_storage = _impl.allocate(new_cap);
    auto p = new_storage;
    try {
        for_each([this, &p] (T& obj) {
            transfer_pass1(_impl, &obj, p++);
        });
    } catch (...) {
        while (p != new_storage) {
            _impl.destroy(--p);
        }
        _impl.deallocate(new_storage, new_cap);
        throw;
    }
    p = new_storage;
    for_each([this, &p] (T& obj) {
        transfer_pass2(_impl, &obj, p++);
    });
    std::swap(_impl.storage, new_storage);
    std::swap(_impl.capacity, new_cap);
    _impl.begin = _impl.storage;
    _impl.end = p;
    _impl.deallocate(new_storage, new_cap);
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::maybe_expand(size_t nr) {
    // one item is always unused
    if (_impl.size + nr >= _impl.capacity) {
        expand();
    }
}

template <typename T, typename Alloc>
inline
T*
circular_buffer<T, Alloc>::pre_push_front() {
    maybe_expand();
    if (_impl.begin == _impl.storage) {
        return _impl.storage + _impl.capacity - 1;
    } else {
        return _impl.begin - 1;
    }
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::post_push_front(T* p) {
    _impl.begin = p;
    ++_impl.size;
}

template <typename T, typename Alloc>
inline
T*
circular_buffer<T, Alloc>::pre_push_back() {
    maybe_expand();
    return _impl.end;
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::post_push_back(T* p) {
    if (++p == _impl.storage + _impl.capacity) {
        p = _impl.storage;
    }
    _impl.end = p;
    ++_impl.size;
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::push_front(const T& data) {
    auto p = pre_push_front();
    _impl.construct(p, data);
    post_push_front(p);
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::push_front(T&& data) {
    auto p = pre_push_front();
    _impl.construct(p, std::move(data));
    post_push_front(p);
}

template <typename T, typename Alloc>
template <typename... Args>
inline
void
circular_buffer<T, Alloc>::emplace_front(Args... args) {
    auto p = pre_push_front();
    _impl.construct(p, std::forward<Args>(args)...);
    post_push_front(p);
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::push_back(const T& data) {
    auto p = pre_push_back();
    _impl.construct(p, data);
    post_push_back(p);
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::push_back(T&& data) {
    auto p = pre_push_back();
    _impl.construct(p, std::move(data));
    post_push_back(p);
}

template <typename T, typename Alloc>
template <typename... Args>
inline
void
circular_buffer<T, Alloc>::emplace_back(Args... args) {
    auto p = pre_push_back();
    _impl.construct(p, std::forward<Args>(args)...);
    post_push_back(p);
}

template <typename T, typename Alloc>
inline
T&
circular_buffer<T, Alloc>::front() {
    return *_impl.begin;
}

template <typename T, typename Alloc>
inline
T&
circular_buffer<T, Alloc>::back() {
    if (_impl.end == _impl.storage) {
        return *(_impl.storage + _impl.capacity - 1);
    } else {
        return *_impl.end;
    }
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::pop_front() {
    ++_impl.begin;
    if (_impl.begin == _impl.storage + _impl.capacity) {
        _impl.begin = _impl.storage;
    }
    --_impl.size;
}

template <typename T, typename Alloc>
inline
void
circular_buffer<T, Alloc>::pop_back() {
    if (_impl.end == _impl.begin) {
        _impl.end = _impl.storage + _impl.capacity;
    }
    --_impl.end;
    --_impl.size;
}

template <typename T, typename Alloc>
inline
T&
circular_buffer<T, Alloc>::operator[](size_t idx) {
    auto p = _impl.begin + idx;
    if (p >= _impl.storage + _impl.capacity) {
        p -= _impl.capacity;
    }
    return *p;
}

#endif /* CIRCULAR_BUFFER_HH_ */
