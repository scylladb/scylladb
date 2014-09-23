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
    std::unique_ptr<impl> _impl;
public:
    deleter() = default;
    deleter(deleter&&) = default;
    explicit deleter(impl* i) : _impl(i) {}
    deleter& operator=(deleter&&) = default;
    impl& operator*() const { return *_impl; }
    impl* operator->() const { return _impl.get(); }
    impl* get() const { return _impl.get(); }
    void reset(impl* n) { _impl.reset(n); }
    impl* release() { return _impl.release(); }
    explicit operator bool() const { return bool(_impl); }
};

struct deleter::impl {
    deleter next;
    impl(deleter next) : next(std::move(next)) {}
    virtual ~impl() {}
};

struct internal_deleter final : deleter::impl {
    // FIXME: make buf an std::array<>?
    char* buf;
    unsigned free_head;
    internal_deleter(deleter next, char* buf, unsigned free_head)
        : impl(std::move(next)), buf(buf), free_head(free_head) {}
    explicit internal_deleter(deleter next, size_t internal_data_size)
        : internal_deleter(std::move(next), new char[internal_data_size], internal_data_size) {}
    virtual ~internal_deleter() override { delete[] buf; }
};

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

struct shared_deleter_impl : deleter::impl {
    long* _count;
    deleter::impl* _deleter;
    shared_deleter_impl(deleter d)
        : impl(deleter()), _count(new long(1)), _deleter(d.release()) {}
    shared_deleter_impl(const shared_deleter_impl& x)
        : impl(deleter()), _count(x._count), _deleter(x._deleter) {
        if (_count) {
            ++*_count;
        }
    }
    shared_deleter_impl(shared_deleter_impl&& x)
        : impl(deleter()), _count(x._count), _deleter(x._deleter) {
        x._count = nullptr;
        x._deleter = nullptr;
    }
    void operator=(const shared_deleter_impl&) = delete;
    void operator=(shared_deleter_impl&&) = delete;
    virtual ~shared_deleter_impl() {
        if (_count && --*_count == 0) {
            delete _deleter;
            delete _count;
        }
    }
    bool owned() const { return *_count == 1; }
};

inline
deleter share(deleter& d) {
    auto sd = dynamic_cast<shared_deleter_impl*>(d.get());
    if (!sd) {
        sd = new shared_deleter_impl(std::move(d));
        d.reset(sd);
    }
    return deleter(new shared_deleter_impl(*sd));
}

#endif /* DELETER_HH_ */
