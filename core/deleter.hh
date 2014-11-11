/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DELETER_HH_
#define DELETER_HH_

#include <memory>

class deleter {
public:
    struct impl;
private:
    impl* _impl = nullptr;
public:
    deleter() = default;
    deleter(const deleter&) = delete;
    deleter(deleter&& x) : _impl(x._impl) { x._impl = nullptr; }
    explicit deleter(impl* i) : _impl(i) {}
    ~deleter();
    deleter& operator=(deleter&& x);
    deleter& operator=(deleter&) = delete;
    impl& operator*() const { return *_impl; }
    impl* operator->() const { return _impl; }
    impl* get() const { return _impl; }
    void unshare();
    deleter share();
    explicit operator bool() const { return bool(_impl); }
    void reset(impl* i) {
        this->~deleter();
        new (this) deleter(i);
    }
};

struct deleter::impl {
    unsigned refs = 1;
    deleter next;
    impl(deleter next) : next(std::move(next)) {}
    virtual ~impl() {}
};

inline
deleter::~deleter() {
    if (_impl && --_impl->refs == 0) {
        delete _impl;
    }
}

inline
deleter& deleter::operator=(deleter&& x) {
    if (this != &x) {
        this->~deleter();
        new (this) deleter(std::move(x));
    }
    return *this;
}

template <typename Deleter>
struct lambda_deleter_impl final : deleter::impl {
    Deleter del;
    lambda_deleter_impl(deleter next, Deleter&& del)
        : impl(std::move(next)), del(std::move(del)) {}
    virtual ~lambda_deleter_impl() override { del(); }
};

template <typename Deleter>
deleter
make_deleter(deleter next, Deleter d) {
    return deleter(new lambda_deleter_impl<Deleter>(std::move(next), std::move(d)));
}

inline
deleter
deleter::share() {
    if (!_impl) {
        return deleter();
    }
    ++_impl->refs;
    return deleter(_impl);
}

inline
deleter
make_free_deleter(void* obj) {
    return make_deleter(deleter(), [obj] { free(obj); });
}

#endif /* DELETER_HH_ */
