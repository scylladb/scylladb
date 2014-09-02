/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef SHARED_PTR_HH_
#define SHARED_PTR_HH_

#include <utility>

template <typename T>
class shared_ptr;

template <typename T, typename... A>
shared_ptr<T> make_shared(A&&... a);

template <typename T>
class shared_ptr {
    struct data {
        long _count = 0;
        T _value;

        data() = default;
        data(const T& x) : _value(x) {}
        data(T&& x) : _value(std::move(x)) {}
        template <typename... A>
        data(A&&... a) : _value(std::forward<A>(a)...) {}
    };
    mutable data* _p = nullptr;
private:
    explicit shared_ptr(data* p) : _p(p) {
        if (_p) {
            ++_p->_count;
        }
    }
    template <typename... A>
    static shared_ptr make(A&&... a) {
        return shared_ptr(new data(std::forward<A>(a)...));
    }
public:
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
            delete _p;
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

    T& operator*() const { return _p->_value; }
    T* operator->() const { return &_p->_value; }
    T* get() const { return &_p->_value; }

    operator shared_ptr<const T>() const {
        return shared_ptr<const T>(_p);
    }

    explicit operator bool() const {
        return _p;
    }

    template <typename X, typename... A>
    friend shared_ptr<X> make_shared(A&&...);
};

template <typename T, typename... A>
shared_ptr<T> make_shared(A&&... a) {
    return shared_ptr<T>::make(std::forward<A>(a)...);
}

#endif /* SHARED_PTR_HH_ */
