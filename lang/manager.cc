/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lang/wasm.hh"
#include "lang/manager.hh"
#include "exceptions/exceptions.hh"

namespace lang {

manager::manager(config cfg)
        : wasm_yield_fuel(cfg.wasm ? cfg.wasm->yield_fuel : 0)
        , wasm_total_fuel(cfg.wasm ? cfg.wasm->total_fuel : 0)
        , lua_max_bytes(cfg.lua.max_bytes)
        , lua_max_contiguous(cfg.lua.max_contiguous)
        , lua_timeout(cfg.lua.timeout)
{
    if (cfg.wasm) {
        if (this_shard_id() == 0) {
            // Other shards will get this pointer in .start()
            _engine = std::make_shared<rust::Box<wasmtime::Engine>>(wasmtime::create_engine(cfg.wasm->udf_memory_limit));
            _alien_runner = std::make_shared<wasm::alien_thread_runner>();
        }
        _instance_cache.emplace(cfg.wasm->cache_size, cfg.wasm->cache_instance_size, cfg.wasm->cache_timer_period);
    }
}

future<> manager::start() {
    if (this_shard_id() == 0) {
        co_await container().invoke_on_others([this] (auto& m) {
            m._engine = this->_engine;
            m._alien_runner = this->_alien_runner;
        });
    }
}

future<> manager::stop() {
    if (_instance_cache) {
        co_await _instance_cache->stop();
    }
}

future<manager::context> manager::create(sstring language, sstring name, const std::vector<sstring>& arg_names, std::string script) {
    manager::context ctx;
    if (language == "lua") {
        utils::updateable_value<unsigned> max_bytes(lua_max_bytes);
        utils::updateable_value<unsigned> max_contiguous(lua_max_contiguous);
        utils::updateable_value<unsigned> timeout_in_ms(lua_timeout.count());
        auto lua_cfg = lua::runtime_config{std::move(timeout_in_ms), std::move(max_bytes), std::move(max_contiguous)};
        auto lua_ctx = cql3::functions::user_function::lua_context {
            .bitcode = lua::compile(lua_cfg, arg_names, script),
            .cfg = lua_cfg,
        };

        ctx = std::move(lua_ctx);
    } else if (language == "wasm") {
       // FIXME: need better way to test wasm compilation without real_database()
       auto wasm_ctx = wasm::context(**_engine, std::move(name), *_instance_cache, wasm_yield_fuel, wasm_total_fuel);
       try {
            co_await ::wasm::precompile(*_alien_runner, wasm_ctx, arg_names, std::move(script));
       } catch (const wasm::exception& we) {
           throw exceptions::invalid_request_exception(we.what());
       }
       ctx.emplace(std::move(wasm_ctx));
    }
    co_return ctx;
}

} // lang namespace
