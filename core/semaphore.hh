/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CORE_SEMAPHORE_HH_
#define CORE_SEMAPHORE_HH_

#include "future.hh"
#include "circular_buffer.hh"
#include <stdexcept>

class broken_semaphore : public std::exception {
public:
    virtual const char* what() const noexcept {
        return "Semaphore broken";
    }
};

class semaphore {
private:
    size_t _count;
    circular_buffer<std::pair<promise<>, size_t>> _wait_list;
public:
    semaphore(size_t count = 1) : _count(count) {}
    future<> wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return make_ready_future<>();
        }
        promise<> pr;
        auto fut = pr.get_future();
        _wait_list.push_back({ std::move(pr), nr });
        return fut;
    }
    void signal(size_t nr = 1) {
        _count += nr;
        while (!_wait_list.empty() && _wait_list.front().second <= _count) {
            auto& x = _wait_list.front();
            _count -= x.second;
            x.first.set_value();
            _wait_list.pop_front();
        }
    }
    bool try_wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return true;
        } else {
            return false;
        }
    }
    size_t current() const { return _count; }

    // Signal to waiters that an error occured.  wait() will see
    // an exceptional future<> containing a broken_semaphore exception.
    //
    // This may only be used once per semaphore; after using it the
    // semaphore is in an indeterminite state and should not be waited on.
    void broken() { broken(broken_semaphore()); }

    // Signal to waiters that an error occured.  wait() will see
    // an exceptional future<> containing the provided exception parameter.
    //
    // This may only be used once per semaphore; after using it the
    // semaphore is in an indeterminite state and should not be waited on.
    template <typename Exception>
    void broken(const Exception& ex);
};

template <typename Exception>
void semaphore::broken(const Exception& ex) {
    auto xp = std::make_exception_ptr(ex);
    while (!_wait_list.empty()) {
        auto& x = _wait_list.front();
        x.first.set_exception(xp);
        _wait_list.pop_front();
    }
}

#endif /* CORE_SEMAPHORE_HH_ */
