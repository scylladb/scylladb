/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
        init_wasm(*cfg.wasm);
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
        ctx.emplace(co_await create_wasm(std::move(name), arg_names, std::move(script)));
    }
    co_return ctx;
}

} // lang namespace
