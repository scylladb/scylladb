/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lang/wasm.hh"
#include "lang/wasm_instance_cache.hh"
#include "rust/wasmtime_bindings.hh"
#include <seastar/core/reactor.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/core/coroutine.hh>

// This test file can contain only a single test case which uses the wasmtime
// runtime. This is because the wasmtime runtime registers a signal handler
// only once in the lifetime of the process, while each boost test case
// registers its own signal handler.
// We need the signal handler registered by the wasm runtime not to be
// overwritten by the signal handlers registered by the boost test cases
// because the wasm runtime uses the SIGILL signal to detect illegal
// instructions in the wasm code - in particular, an illegal instruction is
// executed when an memory allocation fails in the WASM program.

static const char* grow_return = R"(
  (module
  (type (;0;) (func (param i64) (result i64)))
  (func (;0;) (type 0) (param i64) (result i64)
    i32.const 1
    memory.grow
    i32.const -1
    i32.eq
    if  ;; label = @1
      unreachable
    end
    i64.const 10)
  (memory (;0;) 2)
  (global (;0;) i32 (i32.const 1024))
  (export "memory" (memory 0))
  (export "grow_return" (func 0))
  (export "_scylla_abi" (global 0))
  (data (;0;) (i32.const 1024) "\01"))
)";

// This test injects allocation failure at every wasm memory growth
// and checks that the wasm function returns an error and does not abort.
// This would normally be done using an alloc_failure_injector, but
// we can't do that here because this test case uses Rust code, which
// may abort on allocation failure in general.
// TODO: test all allocation points when Rust enables OOM handling.
SEASTAR_TEST_CASE(test_allocation_failures) {
    // First we register an empty signal handler to remove SIGILL from the set
    // of signals blocked by pthread_sigmask. This signal is excluded from the
    // mask in the app_template but not in the test_runner, which is why we need
    // to do this in the test case and not in the main Scylla code.
    // Other signals that may need to be unblocked in future test cases
    // are SIGFPE and SIGBUS.
    engine().handle_signal(SIGILL, [] {});
    int errors_during_compilation = 0;
    int errors_during_execution = 0;
    wasm::alien_thread_runner alien_runner;
    for (size_t fail_after = 0;;fail_after++) {
        auto wasm_engine = wasmtime::create_test_engine(1024 * 1024, fail_after);
        auto wasm_cache = std::make_unique<wasm::instance_cache>(100 * 1024 * 1024, 1024 * 1024, std::chrono::seconds(1));
        auto wasm_ctx = wasm::context(*wasm_engine, "grow_return", *wasm_cache, 1000, 1000000000);
        try {
            // Function that ignores the input, grows its memory by 1 page, and returns 10
            co_await wasm::precompile(alien_runner, wasm_ctx, {}, grow_return);
            wasm_ctx.module.value()->compile(*wasm_engine);
        } catch (const wasm::exception& e) {
            errors_during_compilation++;
            continue;
        }
        try {
            auto store = wasmtime::create_store(wasm_ctx.engine_ptr, wasm_ctx.total_fuel, wasm_ctx.yield_fuel);
            auto instance = wasmtime::create_instance(wasm_ctx.engine_ptr, **wasm_ctx.module, *store);
            auto func = wasmtime::create_func(*instance, *store, wasm_ctx.function_name);
            auto memory = wasmtime::get_memory(*instance, *store);

            size_t mem_size = memory->grow(*store, 1) * 64 * 1024;
            int32_t serialized_size = -1;
            data_value arg{(int64_t)10};
            auto arg_serialized = serialized(arg);

            int64_t arg_combined = ((int64_t)serialized_size << 32) | mem_size;
            auto argv = wasmtime::get_val_vec();
            argv->push_i64(arg_combined);
            auto rets = wasmtime::get_val_vec();
            rets->push_i32(0);
            auto fut = wasmtime::get_func_future(*store, *func, *argv, *rets);
            while (!fut->resume());

            BOOST_CHECK_EQUAL(rets->pop_val()->i64(), 10);
        } catch (const rust::Error& e) {
            errors_during_execution++;
            continue;
        }
        // Check that we can actually throw errors both during compilation and during execution
        BOOST_CHECK(errors_during_compilation > 0);
        BOOST_CHECK(errors_during_execution > 0);
        co_return;
    }
}
