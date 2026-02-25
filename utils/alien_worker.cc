/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/alien_worker.hh"
#include <seastar/util/log.hh>

using namespace seastar;

namespace utils {

std::thread alien_worker::spawn(seastar::logger& log, int niceness, const seastar::sstring& name_suffix) {
    sigset_t newset;
    sigset_t oldset;
    sigfillset(&newset);
    auto r = ::pthread_sigmask(SIG_SETMASK, &newset, &oldset);
    assert(r == 0);
    auto thread_name = fmt::format("alien-{}", name_suffix);
    if (thread_name.size() > 15) {
        log.warn("Thread name '{}' is longer than 15 characters, truncating to fit", thread_name);
        thread_name.resize(15); // pthread_setname_np requires name to be <= 15 characters
    }
    auto thread = std::thread([this, &log, niceness, thread_name] () noexcept {
        errno = 0;
        int setname_value = pthread_setname_np(pthread_self(), thread_name.c_str());
        if (setname_value != 0) {
            log.error("Unable to set worker thread name '{}', setname_value={}", thread_name, setname_value);
            std::abort();
        }
        int nice_value = nice(niceness);
        if (nice_value == -1 && errno != 0) {
            log.warn("Unable to renice worker thread (system error number {}); the thread will compete with reactor, which can cause latency spikes. Try adding CAP_SYS_NICE", errno);
        }

        while (true) {
            std::unique_lock lk(_mut);
            _cv.wait(lk, [this] { return !_pending.empty() || !_running; });
            if (!_running) {
                return;
            }
            auto f = std::move(_pending.front());
            _pending.pop();
            lk.unlock();
            f();
        }
    });
    r = ::pthread_sigmask(SIG_SETMASK, &oldset, nullptr);
    assert(r == 0);
    return thread;
}

alien_worker::alien_worker(seastar::logger& log, int niceness, const seastar::sstring& name_suffix)
    : _thread(spawn(log, niceness, name_suffix))
{}

alien_worker::~alien_worker() {
    {
        std::unique_lock lk(_mut);
        _running = false;
    }
    _cv.notify_one();
    _thread.join();
}

} // namespace utils

