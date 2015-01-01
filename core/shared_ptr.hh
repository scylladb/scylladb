/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef SHARED_PTR_HH_
#define SHARED_PTR_HH_

#include <utility>
#include <type_traits>

template <typename T>
class shared_ptr;

template <typename T>
class enable_shared_from_this;

template <typename T, typename... A>
shared_ptr<T> make_shared(A&&... a);

template <typename T>
shared_ptr<T> make_shared(T&& a);

template <typename T>
shared_ptr<T> make_shared(T& a);

// We want to support two use cases for shared_ptr<T>:
//
//   1. T is any type (primitive or class type)
//
//   2. T is a class type that inherits from enable_shared_from_this<T>.
//
// In the first case, we must wrap T in an object containing the counter,
// since T may be a primitive type and cannot be a base class.
//
// In the second case, we want T to reach the counter through its
// enable_shared_from_this<> base class, so that we can implement
// shared_from_this().
//
// To implement those two conflicting requirements (T alongside its counter;
// T inherits from an object containing the counter) we use std::conditional<>
// and some accessor functions to select between two implementations.


// CRTP from this to enable shared_from_this:
template <typename T>
class enable_shared_from_this {
    long _count = 0;
    using ctor = T;
    T* to_value() { return static_cast<T*>(this); }
    T* to_internal_object() { return static_cast<T*>(this); }
protected:
    enable_shared_from_this& operator=(const enable_shared_from_this&) { return *this; }
    enable_shared_from_this& operator=(enable_shared_from_this&&) { return *this; }
public:
    shared_ptr<T> shared_from_this();
    template <typename X>
    friend class shared_ptr;
};

template <typename T>
struct shared_ptr_no_esft {
    long _count = 0;
    T _value;
    using ctor = shared_ptr_no_esft;

    T* to_value() { return &_value; }
    shared_ptr_no_esft* to_internal_object() { return this; }
    shared_ptr_no_esft() = default;
    shared_ptr_no_esft(const T& x) : _value(x) {}
    shared_ptr_no_esft(T&& x) : _value(std::move(x)) {}
    template <typename... A>
    shared_ptr_no_esft(A&&... a) : _value(std::forward<A>(a)...) {}
    template <typename X>
    friend class shared_ptr;
};

template <typename T>
using shared_ptr_impl
    = std::conditional_t<
        std::is_base_of<enable_shared_from_this<T>, T>::value,
        enable_shared_from_this<T>,
        shared_ptr_no_esft<T>
      >;

template <typename T>
class shared_ptr {
    mutable shared_ptr_impl<T>* _p = nullptr;
private:
    shared_ptr(shared_ptr_impl<T>* p) : _p(p) {
        if (_p) {
            ++_p->_count;
        }
    }
    template <typename... A>
    static shared_ptr make(A&&... a) {
        return shared_ptr(new typename shared_ptr_impl<T>::ctor(std::forward<A>(a)...));
    }
public:
    using element_type = T;

    shared_ptr() = default;
    shared_ptr(const shared_ptr& x) : _p(x._p) {
        if (_p) {
            ++_p->_count;
        }
    }
    shared_ptr(shared_ptr&& x) : _p(x._p) {
        x._p = nullptr;
    }
    ~shared_ptr() {
        if (_p && !--_p->_count) {
            delete _p->to_internal_object();
        }
    }
    shared_ptr& operator=(const shared_ptr& x) {
        if (_p != x._p) {
            this->~shared_ptr();
            new (this) shared_ptr(x);
        }
        return *this;
    }
    shared_ptr& operator=(shared_ptr&& x) {
        if (_p != x._p) {
            this->~shared_ptr();
            new (this) shared_ptr(std::move(x));
        }
        return *this;
    }
    shared_ptr& operator=(T&& x) {
        this->~shared_ptr();
        new (this) shared_ptr(make_shared<T>(std::move(x)));
        return *this;
    }

    T& operator*() const { return *_p->to_value(); }
    T* operator->() const { return _p->to_value(); }
    T* get() const { return _p->to_value(); }

    long int use_count() {
        if (_p) {
            return _p->_count;
        } else {
            return 0;
        }
    }

    operator shared_ptr<const T>() const {
        return shared_ptr<const T>(_p);
    }

    explicit operator bool() const {
        return _p;
    }

    bool owned() const {
        return _p->_count == 1;
    }

    template <typename X, typename... A>
    friend shared_ptr<X> make_shared(A&&...);

    template <typename U>
    friend shared_ptr<U> make_shared(U&&);

    template <typename U>
    friend shared_ptr<U> make_shared(U&);

    template <typename U>
    friend class enable_shared_from_this;
};

template <typename T, typename... A>
inline
shared_ptr<T> make_shared(A&&... a) {
    return shared_ptr<T>::make(std::forward<A>(a)...);
}

template <typename T>
inline
shared_ptr<T> make_shared(T&& a) {
    return shared_ptr<T>::make(std::move(a));
}

template <typename T>
inline
shared_ptr<T> make_shared(T& a) {
    return shared_ptr<T>::make(a);
}

template <typename T>
inline
shared_ptr<T>
enable_shared_from_this<T>::shared_from_this() {
    return shared_ptr<T>(this);
}

#endif /* SHARED_PTR_HH_ */
