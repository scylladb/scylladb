/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <exception>
#include <seastar/core/alien.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <unistd.h>

#include "log.hh"
#include "lang/wasm.hh"
#include "lang/wasm_alien_thread_runner.hh"
#include <seastar/core/posix.hh>

extern logging::logger wasm_logger;

namespace wasm {

std::optional<wasm_compile_task> task_queue::pop_front() {
    std::unique_lock lock(_mut);
    _cv.wait(lock, [this] { return !_pending.empty(); });
    auto work_item = std::move(_pending.front());
    _pending.pop();
    return work_item;
}

void task_queue::push_back(std::optional<wasm_compile_task> work_item) {
    std::unique_lock lock(_mut);
    _pending.emplace(std::move(work_item));
    lock.unlock();
    _cv.notify_one();
}

alien_thread_runner::alien_thread_runner()
    : _thread([this] {
        sigset_t mask;
        sigfillset(&mask);
        auto r = ::pthread_sigmask(SIG_BLOCK, &mask, nullptr);
        throw_pthread_error(r);

        errno = 0;
        int nice_value = nice(10);
        if (nice_value == -1 && errno != 0) {
            wasm_logger.warn("Unable to renice the alien thread (system error number {}); the thread will compete with reactor. Try adding CAP_SYS_NICE", errno);
        }

        for (;;) {
            auto work_item = _pending_queue.pop_front();
            if (work_item) {
                work_item->func();
            } else {
                break;
            }
        }
    })
{ }

alien_thread_runner::~alien_thread_runner() {
    _pending_queue.push_back(std::nullopt);
    _thread.join();
}

void alien_thread_runner::submit(seastar::promise<rust::Box<wasmtime::Module>>& p, std::function<rust::Box<wasmtime::Module>()> f) {
    seastar::noncopyable_function<void()> packaged([f = std::move(f), &p, &alien = seastar::engine().alien(), shard = seastar::this_shard_id()] () mutable {
        try {
            rust::Box<wasmtime::Module> mod = f();
            seastar::alien::run_on(alien, shard, [&p, mod = std::move(mod)] () mutable noexcept {
                p.set_value(std::move(mod));
            });
        } catch (...) {
            seastar::alien::run_on(alien, shard, [&, eptr = std::current_exception()]() noexcept {
                p.set_exception(wasm::exception(format("Compilation failed: {}", eptr)));
            });
        }
    });
    _pending_queue.push_back(wasm_compile_task{.func = std::move(packaged), .done = p, .shard = seastar::this_shard_id()});
}

} // namespace wasm
