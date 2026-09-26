/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "lang/wasm.hh"
#include "cql3/functions/user_function.hh"

namespace lang {

class manager : public seastar::peering_sharded_service<manager> {
    wasm::manager_state _wasm;

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
    // start(), stop() and remove() only manage wasm state; they and the
    // private *_wasm() helpers live in lang/wasm.cc or lang/wasm_disabled.cc.
    future<> start();
    future<> stop();
    void remove(const db::functions::function_name& name, const std::vector<data_type>& arg_types) noexcept;

    using context = std::optional<cql3::functions::user_function::context>;
    future<context> create(sstring language, sstring name, const std::vector<sstring>& arg_names, std::string script);

private:
    void init_wasm(const wasm_config& cfg);
    future<wasm::context> create_wasm(sstring name, const std::vector<sstring>& arg_names, std::string script);
};

} // lang namespace
