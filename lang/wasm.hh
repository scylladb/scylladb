/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <span>
#include "types/types.hh"
#include <seastar/core/future.hh>
#include "db/functions/function_name.hh"

namespace wasm {

class instance_cache;
class alien_thread_runner;
struct context;

struct exception : public std::exception {
    std::string _msg;
public:
    explicit exception(std::string_view msg) : _msg(msg) {}
    const char* what() const noexcept {
        return _msg.c_str();
    }
};

struct instance_corrupting_exception : public exception {
    explicit instance_corrupting_exception(std::string_view msg) : exception(msg) {}
};

seastar::future<> precompile(alien_thread_runner& alien_runner, context& ctx, const std::vector<sstring>& arg_names, std::string script);

seastar::future<bytes_opt> run_script(const db::functions::function_name& name, context& ctx, const std::vector<data_type>& arg_types, std::span<const bytes_opt> params, data_type return_type, bool allow_null_input);

}

#ifdef SCYLLA_BUILD_WASMTIME
#include "rust/wasmtime_bindings.hh"
#include "lang/wasm_instance_cache.hh"
#include "lang/wasm_alien_thread_runner.hh"

namespace wasm {

struct context {
    wasmtime::Engine& engine_ptr;
    std::optional<rust::Box<wasmtime::Module>> module;
    std::string function_name;
    instance_cache& cache;
    uint64_t yield_fuel;
    uint64_t total_fuel;

    context(wasmtime::Engine& engine_ptr, std::string name, instance_cache& cache, uint64_t yield_fuel, uint64_t total_fuel);
};

// lang::manager's wasm members.
struct manager_state {
    std::shared_ptr<rust::Box<wasmtime::Engine>> engine;
    std::optional<instance_cache> cache;
    std::shared_ptr<alien_thread_runner> alien_runner;
};

} // namespace wasm
#else
namespace wasm {

// Without wasmtime no context is ever created; see lang/wasm_disabled.cc.
struct context {};
struct manager_state {};

} // namespace wasm
#endif
