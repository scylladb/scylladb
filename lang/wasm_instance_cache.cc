/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifdef SCYLLA_ENABLE_WASMTIME

#include "lang/wasm_instance_cache.hh"
#include "seastar/core/metrics.hh"
#include "seastar/core/scheduling.hh"
#include <seastar/core/units.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/util/defer.hh>

namespace wasm {

static constexpr size_t WASM_PAGE_SIZE = 64 * KB;

instance_cache::stats& instance_cache::shard_stats() {
    return _stats;
}

void instance_cache::setup_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("user_functions", {
        sm::make_counter("cache_hits", wasm::instance_cache::shard_stats().cache_hits,
                        sm::description("The number of user defined function cache hits")),
        sm::make_counter("cache_misses", wasm::instance_cache::shard_stats().cache_misses,
                        sm::description("The number of user defined functions loaded")),
        sm::make_counter("cache_blocks", wasm::instance_cache::shard_stats().cache_blocks,
                        sm::description("The number of times a user defined function waited for an instance")),
        sm::make_gauge("cache_instace_count_any", [this] { return _cache.size(); },
                        sm::description("The total number of cached wasm instances, instances in use and empty instances")),
        sm::make_gauge("cache_total_size", [this] { return _total_size; },
                        sm::description("The total size of instances stored in the user defined function cache")),
    });
}

instance_cache::instance_cache(size_t size, seastar::lowres_clock::duration timer_period)
    : _timer([this] { return on_timer(); })
    , _timer_period(timer_period)
    , _max_size(size)
{
    setup_metrics();
    _timer.arm_periodic(_timer_period);
}

wasm_instance instance_cache::load(wasm::context& ctx) {
    auto store = wasmtime::Store(ctx.engine_ptr->get());
    auto linker = wasmtime::Linker(ctx.engine_ptr->get());
    auto wasi_def = linker.define_wasi();
    if (!wasi_def) {
        throw wasm::exception(format("Setting up wasi failed: {}", wasi_def.err().message()));
    }
    auto cfg = wasmtime::WasiConfig();
    auto set_cfg = store.context().set_wasi(std::move(cfg));
    if (!set_cfg) {
        throw wasm::exception(format("Setting up wasi failed: {}", set_cfg.err().message()));
    }
    auto instance_res = linker.instantiate(store, *ctx.module);
    if (!instance_res) {
        throw wasm::exception(format("Creating a wasm runtime instance failed: {}", instance_res.err().message()));
    }
    auto instance = instance_res.unwrap();
    auto function_obj = instance.get(store, ctx.function_name);
    if (!function_obj) {
        throw wasm::exception(format("Function {} was not found in given wasm source code", ctx.function_name));
    }
    wasmtime::Func* func = std::get_if<wasmtime::Func>(&*function_obj);
    if (!func) {
        throw wasm::exception(format("Exported object {} is not a function", ctx.function_name));
    }
    auto memory_export = instance.get(store, "memory");
    if (!memory_export) {
        throw wasm::exception("memory export not found - please export `memory` in the wasm module");
    }
    auto memory = std::get<wasmtime::Memory>(*memory_export);

    return wasm::wasm_instance{.store=std::move(store), .instance=std::move(instance), .func=std::move(*func), .memory=std::move(memory)};
}

// lru must not be empty, and its elements must refer to entries in _cache
void instance_cache::evict_lru() noexcept {
    auto& entry = _lru.front();
    _total_size -= entry.instance_size;
    entry.cache_entry->instance = std::nullopt;
    entry.cache_entry->it = _lru.end();
    _lru.pop_front();
}

void instance_cache::on_timer() noexcept {
    auto now = seastar::lowres_clock::now();
    while (!_lru.empty() && _lru.front().timestamp + _timer_period < now) {
        evict_lru();
    }
}

static uint32_t get_instance_size(wasm_instance& instance) {
    // reserve 1 wasm page for instance data other than the wasm memory
    return WASM_PAGE_SIZE * (1 + instance.memory.size(instance.store));
}

seastar::future<instance_cache::value_type> instance_cache::get(const db::functions::function_name& name, const std::vector<data_type>& arg_types, wasm::context& ctx) {
    auto [it, end_it] = _cache.equal_range(name);
    while (it != end_it) {
        if (it->second->scheduling_group == seastar::current_scheduling_group() && it->second->arg_types == arg_types) {
            break;
        }
        ++it;
    }

    if (it == end_it) {
        it = _cache.emplace(name, make_lw_shared<cache_entry_type>(cache_entry_type{
            .scheduling_group = seastar::current_scheduling_group(),
            .arg_types = arg_types,
            .mutex = seastar::shared_mutex(),
            .instance = std::nullopt,
            .it = _lru.end(),
        }));
    }
    auto& entry = it->second;
    auto f = entry->mutex.lock();
    if (!f.available()) {
        ++shard_stats().cache_blocks;
    }
    return f.then([this, entry, &ctx] {
        if (!entry->instance) {
            ++shard_stats().cache_misses;
            entry->instance = load(ctx);
        } else {
            // because we don't want to remove an instance after it starts being used,
            // and also because we can't track its size efficiently, we remove it from
            // lru and subtract its size from the total size until it is no longer used
            ++shard_stats().cache_hits;
            _total_size -= entry->it->instance_size;
            _lru.erase(entry->it);
            entry->it = _lru.end();
        }
        return entry;
    });
}

void instance_cache::recycle(instance_cache::value_type val) noexcept {
    val->mutex.unlock();
    size_t size = 0;
    try {
        if (val->instance) {
            size = get_instance_size(val->instance.value());
        }
        if (size > 1 * MB) {
            val->instance = std::nullopt;
            return;
        }
    } catch (...) {
        // we can't get the instance size, so we can't recycle it
        val->instance = std::nullopt;
        return;
    }
    while (_total_size + size > _max_size) {
        // make space for the recycled instance if needed. we won't
        // remove the instance itself because it was not in the lru
        evict_lru();
    }
    try {
        // new instance_size is set here
        _lru.push_back({val, seastar::lowres_clock::now(), size});
        val->it = --_lru.end();
        _total_size += val->it->instance_size;
    } catch (...) {
        // we can't add the instance to the lru, so we can't recycle it
        val->instance = std::nullopt;
    }
}

void instance_cache::remove(const db::functions::function_name& name, const std::vector<data_type>& arg_types) noexcept {
    auto [it,end_it] = _cache.equal_range(name);
    while (it != end_it) {
        auto& entry_ptr = it->second;
        if (entry_ptr->arg_types == arg_types) {
            if (entry_ptr->it != _lru.end()) {
                _total_size -= entry_ptr->it->instance_size;
                _lru.erase(entry_ptr->it);
            }
            it = _cache.erase(it);
        } else {
            ++it;
        }
    }
}

size_t instance_cache::size() const {
    return _cache.size();
}

size_t instance_cache::max_size() const {
    return _max_size;
}

size_t instance_cache::memory_footprint() const {
    return _total_size;
}

future<> instance_cache::stop() {
    _timer.cancel();
    return make_ready_future<>();
}

}

namespace std {

inline std::ostream& operator<<(std::ostream& out, const seastar::scheduling_group& sg) {
    return out << sg.name();
}

template <>
struct equal_to<seastar::scheduling_group> {
    bool operator()(seastar::scheduling_group& sg1, seastar::scheduling_group& sg2) const noexcept {
        return sg1 == sg2;
    }
};

}

#endif
