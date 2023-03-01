/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <thread>

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include "rust/cxx.h"
#include "rust/wasmtime_bindings.hh"

namespace wasm {

struct wasm_compile_task {
    seastar::noncopyable_function<void()> func;
    seastar::promise<rust::Box<wasmtime::Module>>& done;
    unsigned shard;
};

struct task_queue {
    std::mutex _mut;
    std::condition_variable _cv;
    std::queue<std::optional<wasm_compile_task>> _pending;
public:
    std::optional<wasm_compile_task> pop_front();
    void push_back(std::optional<wasm_compile_task> work_item);
};

class alien_thread_runner {
    task_queue _pending_queue;
    std::thread _thread;
public:
    alien_thread_runner();
    ~alien_thread_runner();
    alien_thread_runner(const alien_thread_runner&) = delete;
    alien_thread_runner& operator=(const alien_thread_runner&) = delete;
    void submit(seastar::promise<rust::Box<wasmtime::Module>>& p, std::function<rust::Box<wasmtime::Module>()> f);
};

} // namespace wasm
