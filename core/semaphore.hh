/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CORE_SEMAPHORE_HH_
#define CORE_SEMAPHORE_HH_

#include "future.hh"
#include <list>
#include <stdexcept>
#include "timer.hh"

class broken_semaphore : public std::exception {
public:
    virtual const char* what() const noexcept {
        return "Semaphore broken";
    }
};

class semaphore_timed_out : public std::exception {
public:
    virtual const char* what() const noexcept {
        return "Semaphore timedout";
    }
};

class semaphore {
private:
    size_t _count;
    struct entry {
        promise<> pr;
        size_t nr;
        timer<> tr;
        entry(promise<>&& pr_, size_t nr_) : pr(std::move(pr_)), nr(nr_) {}
    };
    std::list<entry> _wait_list;
public:
    semaphore(size_t count = 1) : _count(count) {}
    future<> wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return make_ready_future<>();
        }
        promise<> pr;
        auto fut = pr.get_future();
        _wait_list.push_back(entry(std::move(pr), nr));
        return fut;
    }
    future<> wait(typename timer<>::duration timeout, size_t nr = 1) {
        auto fut = wait(nr);
        if (!fut.available()) {
            auto& e = _wait_list.back();
            e.tr.set_callback([&e, this] {
                e.pr.set_exception(semaphore_timed_out());
                e.nr = 0;
                signal(0);
            });
            e.tr.arm(timeout);
        }
        return std::move(fut);
    }
    void signal(size_t nr = 1) {
        _count += nr;
        while (!_wait_list.empty() && _wait_list.front().nr <= _count) {
            auto& x = _wait_list.front();
            if (x.nr) {
               _count -= x.nr;
               x.pr.set_value();
               x.tr.cancel();
            }
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
        x.pr.set_exception(xp);
        x.tr.cancel();
        _wait_list.pop_front();
    }
}

#endif /* CORE_SEMAPHORE_HH_ */
