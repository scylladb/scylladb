/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DELETER_HH_
#define DELETER_HH_

#include <memory>

struct deleter {
    std::unique_ptr<deleter> next;
    deleter(std::unique_ptr<deleter> next) : next(std::move(next)) {}
    virtual ~deleter() {};
};

struct internal_deleter final : deleter {
    // FIXME: make buf an std::array<>?
    char* buf;
    unsigned free_head;
    internal_deleter(std::unique_ptr<deleter> next, char* buf, unsigned free_head)
        : deleter(std::move(next)), buf(buf), free_head(free_head) {}
    explicit internal_deleter(std::unique_ptr<deleter> next, size_t internal_data_size)
        : internal_deleter(std::move(next), new char[internal_data_size], internal_data_size) {}
    virtual ~internal_deleter() override { delete[] buf; }
};

template <typename Deleter>
struct lambda_deleter final : deleter {
    Deleter del;
    lambda_deleter(std::unique_ptr<deleter> next, Deleter&& del)
        : deleter(std::move(next)), del(std::move(del)) {}
    virtual ~lambda_deleter() override { del(); }
};

template <typename Deleter>
std::unique_ptr<deleter>
make_deleter(std::unique_ptr<deleter> next, Deleter d) {
    return std::unique_ptr<deleter>(new lambda_deleter<Deleter>(std::move(next), std::move(d)));
}

struct shared_deleter : deleter {
    long* _count;
    deleter* _deleter;
    shared_deleter(std::unique_ptr<deleter> d)
        : deleter(nullptr), _count(new long(1)), _deleter(d.release()) {}
    shared_deleter(const shared_deleter& x)
        : deleter(nullptr), _count(x._count), _deleter(x._deleter) {
        if (_count) {
            ++*_count;
        }
    }
    shared_deleter(shared_deleter&& x)
        : deleter(nullptr), _count(x._count), _deleter(x._deleter) {
        x._count = nullptr;
        x._deleter = nullptr;
    }
    void operator=(const shared_deleter&) = delete;
    void operator=(shared_deleter&&) = delete;
    virtual ~shared_deleter() {
        if (_count && --*_count == 0) {
            delete _deleter;
            delete _count;
        }
    }
    bool owned() const { return *_count == 1; }
};

inline
std::unique_ptr<deleter> share(std::unique_ptr<deleter>& d) {
    auto sd = dynamic_cast<shared_deleter*>(d.get());
    if (!sd) {
        sd = new shared_deleter(std::move(d));
        d.reset(sd);
    }
    return std::unique_ptr<deleter>(new shared_deleter(*sd));
}

#endif /* DELETER_HH_ */
