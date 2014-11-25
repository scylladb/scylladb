/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DELETER_HH_
#define DELETER_HH_

#include <memory>
#include <cstdlib>
#include <type_traits>

class deleter {
public:
    struct impl;
    struct raw_object_tag {};
private:
    // if bit 0 set, point to object to be freed directly.
    impl* _impl = nullptr;
public:
    deleter() = default;
    deleter(const deleter&) = delete;
    deleter(deleter&& x) : _impl(x._impl) { x._impl = nullptr; }
    explicit deleter(impl* i) : _impl(i) {}
    deleter(raw_object_tag tag, void* object)
        : _impl(from_raw_object(object)) {}
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
private:
    bool is_raw_object() const {
        auto x = reinterpret_cast<uintptr_t>(_impl);
        return x & 1;
    }
    void* to_raw_object() const {
        auto x = reinterpret_cast<uintptr_t>(_impl);
        return reinterpret_cast<void*>(x & ~uintptr_t(1));
    }
    impl* from_raw_object(void* object) {
        auto x = reinterpret_cast<uintptr_t>(object);
        return reinterpret_cast<impl*>(x | 1);
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
    if (is_raw_object()) {
        std::free(to_raw_object());
        return;
    }
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

struct free_deleter_impl final : deleter::impl {
    void* obj;
    free_deleter_impl(void* obj) : impl(deleter()), obj(obj) {}
    virtual ~free_deleter_impl() override { std::free(obj); }
};

inline
deleter
deleter::share() {
    if (!_impl) {
        return deleter();
    }
    if (is_raw_object()) {
        _impl = new free_deleter_impl(to_raw_object());
    }
    ++_impl->refs;
    return deleter(_impl);
}

inline
deleter
make_free_deleter(void* obj) {
    return deleter(deleter::raw_object_tag(), obj);
}

#endif /* DELETER_HH_ */
