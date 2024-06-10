/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "rust/wasmtime_bindings.hh"
#include "lang/wasm_instance_cache.hh"
#include "lang/wasm_alien_thread_runner.hh"
#include "cql3/functions/user_function.hh"

namespace wasm {
struct context;
}

namespace lang {

class manager : public seastar::peering_sharded_service<manager> {
    std::shared_ptr<rust::Box<wasmtime::Engine>> _engine;
    std::optional<wasm::instance_cache> _instance_cache;
    std::shared_ptr<wasm::alien_thread_runner> _alien_runner;

public:
    const uint64_t wasm_yield_fuel;
    const uint64_t wasm_total_fuel;

    const unsigned lua_max_bytes;
    const unsigned lua_max_contiguous;
    const std::chrono::milliseconds lua_timeout;

public:
    struct wasm_config {
        size_t udf_memory_limit;
        size_t cache_size;
        size_t cache_instance_size;
        std::chrono::milliseconds cache_timer_period;
        uint64_t yield_fuel;
        uint64_t total_fuel;
    };
    struct lua_config {
        unsigned max_bytes;
        unsigned max_contiguous;
        std::chrono::milliseconds timeout;
    };
    struct config {
        std::optional<wasm_config> wasm;
        lua_config lua;
    };
    manager(config);
    future<> start();
    future<> stop();
    void remove(const db::functions::function_name& name, const std::vector<data_type>& arg_types) noexcept {
        _instance_cache->remove(name, arg_types);
    }

    using context = std::optional<cql3::functions::user_function::context>;
    future<context> create(sstring language, sstring name, const std::vector<sstring>& arg_names, std::string script);
};

} // lang namespace
