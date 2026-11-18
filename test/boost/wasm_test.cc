/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "lang/wasm.hh"
#include "lang/wasm_instance_cache.hh"
#include "rust/wasmtime_bindings.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include <chrono>
#include <seastar/core/lowres_clock.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/core/coroutine.hh>
#include <atomic>
#include <dlfcn.h>

SEASTAR_TEST_CASE(test_long_udf_yields) {
    auto wasm_engine = wasmtime::create_engine(1024 * 1024);
    wasm::alien_thread_runner alien_runner;
    auto wasm_cache = std::make_unique<wasm::instance_cache>(100 * 1024 * 1024, 1024 * 1024, std::chrono::seconds(1));
    auto wasm_ctx = wasm::context(*wasm_engine, "fib", *wasm_cache, 100000, 100000000000);
    // Recursive fibonacci function
    co_await wasm::precompile(alien_runner, wasm_ctx, {}, R"(
(module
  (type (;0;) (func (param i64) (result i64)))
  (func (;0;) (type 0) (param i64) (result i64)
    (local i64 i32)
    local.get 0
    i64.const 2
    i64.lt_s
    if  ;; label = @1
      local.get 0
      return
    end
    loop  ;; label = @1
      local.get 0
      i64.const 1
      i64.sub
      call 0
      local.get 1
      i64.add
      local.set 1
      local.get 0
      i64.const 3
      i64.gt_s
      local.get 0
      i64.const 2
      i64.sub
      local.set 0
      br_if 0 (;@1;)
    end
    local.get 0
    local.get 1
    i64.add)
  (func (;1;) (type 0) (param i64) (result i64)
    local.get 0
    call 0)
  (memory (;0;) 2)
  (global (;0;) i32 (i32.const 1024))
  (export "memory" (memory 0))
  (export "fib" (func 1))
  (export "_scylla_abi" (global 0))
  (data (;0;) (i32.const 1024) "01"))
)");
    wasm_ctx.module.value()->compile(*wasm_engine);
    auto argv = wasmtime::get_val_vec();
    argv->push_i64(42);
    auto rets = wasmtime::get_val_vec();
    rets->push_i32(0);

    auto store = wasmtime::create_store(wasm_ctx.engine_ptr, wasm_ctx.total_fuel, wasm_ctx.yield_fuel);
    auto instance = wasmtime::create_instance(wasm_ctx.engine_ptr, **wasm_ctx.module, *store);
    auto func = wasmtime::create_func(*instance, *store, wasm_ctx.function_name);
    auto memory = wasmtime::get_memory(*instance, *store);

    auto fut = wasmtime::get_func_future(*store, *func, *argv, *rets);
    bool stop = false;

    using namespace std::chrono;
    auto max_duration = duration(nanoseconds(0));
    auto steps = 0;
    while (!stop) {
        auto start = system_clock::now();
        timespec cpu_start;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_start);
        stop = fut->resume();
        timespec cpu_end;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end);
        auto cpu_duration = duration_cast<nanoseconds>(seconds(cpu_end.tv_sec) + nanoseconds(cpu_end.tv_nsec) - seconds(cpu_start.tv_sec) - nanoseconds(cpu_start.tv_nsec));
        auto curr_duration = system_clock::now() - start;
        if (duration_cast<microseconds>(curr_duration).count() < duration_cast<microseconds>(cpu_duration).count() * 10) {
            // If the wall clock time is too much bigger than the cpu time, the machine is probably overloaded
            // and we don't want to count this as a valid measurement
            max_duration = std::max(max_duration, curr_duration);
        }
        ++steps;
    }
    BOOST_REQUIRE(max_duration < duration(milliseconds(100)));
    BOOST_REQUIRE(steps > 1);
    BOOST_CHECK_EQUAL(rets->pop_val()->i64(), 267914296);
    co_return;
}

// libgcc's __register_frame() and __deregister_frame() register and deregister
// unwind information of generated code, for unwinders to use. We define them
// here, to count their calls, and pass the calls on to libgcc's functions.
// Because this executable defines them, its definitions are the ones that
// wasmtime calls.
static std::atomic<int> register_frame_calls = 0;

extern "C" void __register_frame(void* begin) {
    ++register_frame_calls;
    static auto libgcc_register_frame = reinterpret_cast<void (*)(void*)>(dlsym(RTLD_NEXT, "__register_frame"));
    libgcc_register_frame(begin);
}

extern "C" void __deregister_frame(void* begin) {
    ++register_frame_calls;
    static auto libgcc_deregister_frame = reinterpret_cast<void (*)(void*)>(dlsym(RTLD_NEXT, "__deregister_frame"));
    libgcc_deregister_frame(begin);
}

// Test that loading and unloading the compiled code of a Wasm function doesn't
// call __register_frame() or __deregister_frame(). Scylla does both on the
// shard, and if the stall detector's signal interrupts one of these calls,
// the signal handler's backtrace waits forever for a lock in libgcc which the
// interrupted call holds, so the shard hangs.
// Reproduces SCYLLADB-6435.
SEASTAR_TEST_CASE(test_no_frame_registration) {
    auto wasm_engine = wasmtime::create_engine(1024 * 1024);
    wasm::alien_thread_runner alien_runner;
    auto wasm_cache = std::make_unique<wasm::instance_cache>(100 * 1024 * 1024, 1024 * 1024, std::chrono::seconds(1));
    auto wasm_ctx = wasm::context(*wasm_engine, "f", *wasm_cache, 100000, 100000000);
    register_frame_calls = 0;
    // precompile() also loads the compiled code, creates an instance to check
    // it, and unloads the code
    co_await wasm::precompile(alien_runner, wasm_ctx, {}, R"(
(module
  (func (param i32) (result i32) local.get 0)
  (memory 1)
  (global i32 (i32.const 0))
  (export "memory" (memory 0))
  (export "f" (func 0))
  (export "_scylla_abi" (global 0)))
)");
    BOOST_REQUIRE_EQUAL(register_frame_calls, 0);
}
