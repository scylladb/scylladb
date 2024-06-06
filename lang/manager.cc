/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lang/wasm.hh"
#include "lang/manager.hh"

namespace lang {

manager::manager(config cfg)
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

} // lang namespace
