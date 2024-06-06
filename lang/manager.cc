/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lang/wasm.hh"
#include "lang/manager.hh"

namespace wasm {

manager::manager(const std::optional<wasm::startup_context>& ctx)
        : _engine(ctx ? ctx->engine : nullptr)
        , _instance_cache(ctx ? std::make_optional<wasm::instance_cache>(ctx->cache_size, ctx->instance_size, ctx->timer_period) : std::nullopt)
        , _alien_runner(ctx ? ctx->alien_runner : nullptr)
{}

future<> manager::stop() {
    if (_instance_cache) {
        co_await _instance_cache->stop();
    }
}

}
