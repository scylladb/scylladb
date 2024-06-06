/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "rust/wasmtime_bindings.hh"
#include "lang/wasm_instance_cache.hh"
#include "lang/wasm_alien_thread_runner.hh"

namespace wasm {

struct startup_context;

class manager {
    std::shared_ptr<rust::Box<wasmtime::Engine>> _engine;
    std::optional<wasm::instance_cache> _instance_cache;
    std::shared_ptr<wasm::alien_thread_runner> _alien_runner;

public:
    manager(const std::optional<wasm::startup_context>&);
    friend context;
    future<> stop();
    seastar::future<> precompile(context& ctx, const std::vector<sstring>& arg_names, std::string script);
    void remove(const db::functions::function_name& name, const std::vector<data_type>& arg_types) noexcept {
        _instance_cache->remove(name, arg_types);
    }
};

}
