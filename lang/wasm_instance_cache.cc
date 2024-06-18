/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lang/wasm_instance_cache.hh"
#include "lang/wasm.hh"
#include <seastar/core/metrics.hh>
#include <seastar/core/scheduling.hh>
#include <exception>
#include <seastar/core/units.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/util/defer.hh>
#include <unistd.h>

namespace wasm {

static size_t compiled_size(const wasmtime::Module& module) noexcept {
    // Round up the exact size to the nearest page size.
    auto page_size = getpagesize();
    return (module.raw_size() + (page_size - 1)) & (~(page_size - 1));
}

static size_t wasm_stack_size() noexcept {
    // Wasm stack contains 2 stacks - one for wasm functions and one for
    // host functions, both of which are 128KB - and a guard page.
    return 256 * KB + getpagesize();
}

module_handle::module_handle(wasmtime::Module& module, instance_cache& cache, wasmtime::Engine& engine)
    : _module(module)
    , _cache(cache)
{
    _cache.track_module_ref(_module, engine);
}

module_handle::module_handle(const module_handle& mh) noexcept
    : _module(mh._module)
    , _cache(mh._cache)
{
    _module.add_user();
}

module_handle::~module_handle() noexcept {
    _cache.remove_module_ref(_module);
}

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

instance_cache::instance_cache(size_t size, size_t instance_size, seastar::lowres_clock::duration timer_period)
    : _timer([this] { return on_timer(); })
    , _timer_period(timer_period)
    , _max_size(size)
    , _max_instance_size(instance_size)
{
    setup_metrics();
    _timer.arm_periodic(_timer_period);
}

wasm_instance instance_cache::load(wasm::context& ctx) {
    auto mh = module_handle(**ctx.module, *this, ctx.engine_ptr);
    auto store = wasmtime::create_store(ctx.engine_ptr, ctx.total_fuel, ctx.yield_fuel);
    auto instance = wasmtime::create_instance(ctx.engine_ptr, **ctx.module, *store);
    auto func = wasmtime::create_func(*instance, *store, ctx.function_name);
    auto memory = wasmtime::get_memory(*instance, *store);
    return wasm_instance{
        .store = std::move(store),
        .instance = std::move(instance),
        .func = std::move(func),
        .memory = std::move(memory),
        .mh = std::move(mh)
    };
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
    return WASM_PAGE_SIZE * (1 + instance.memory->size(*instance.store));
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
            .module = *ctx.module.value(),
        }));
    }
    auto& entry = it->second;
    auto f = entry->mutex.lock();
    if (!f.available()) {
        ++shard_stats().cache_blocks;
    }
    return f.then([this, entry, &ctx] {
        // When the instance leaves the cache, it should be ready to be used. For
        // that, we need to make sure that there is enough free memory for the
        // wasm runtime stack, that is allocated at the start of the UDF execution,
        // and which is not allocated using seastar allocator, but using mmap.
        reserve_wasm_stack();
        if (!entry->instance) {
            ++shard_stats().cache_misses;
            try {
                entry->instance.emplace(load(ctx));
            } catch (...) {
                // We couldn't actually use the compiled module, so we need to remove
                // the reference to it.
                std::exception_ptr ex = std::current_exception();
                return make_exception_future<instance_cache::value_type>(std::move(ex));
            }
        } else {
            // because we don't want to remove an instance after it starts being used,
            // and also because we can't track its size efficiently, we remove it from
            // lru and subtract its size from the total size until it is no longer used
            ++shard_stats().cache_hits;
            _total_size -= entry->it->instance_size;
            _lru.erase(entry->it);
            entry->it = _lru.end();
        }
        return make_ready_future<instance_cache::value_type>(entry);
    });
}

void instance_cache::recycle(instance_cache::value_type val) noexcept {
    // While the instance is in cache, it is not used and no stack is allocated for it.
    free_wasm_stack();
    val->mutex.unlock();
    size_t size;
    try {
        if (!val->instance) {
            return;
        }
        size = get_instance_size(val->instance.value());
        if (size > _max_instance_size) {
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

void instance_cache::track_module_ref(wasmtime::Module& module, wasmtime::Engine& engine) {
    if (!module.is_compiled()) {
        size_t module_size = compiled_size(module);
        while (_compiled_size + module_size > _max_compiled_size && !_lru.empty()) {
            evict_modules();
        }
        if (_compiled_size + module_size > _max_compiled_size) {
            throw wasm::exception("No memory left for the compiled WASM function");
        }
        module.compile(engine);
        _compiled_size += module_size;
    }
    module.add_user();
}

void instance_cache::remove_module_ref(wasmtime::Module& module) noexcept {
    module.remove_user();
    if (module.user_count() == 0) {
        module.release();
        _compiled_size -= compiled_size(module);
    }
}

void instance_cache::reserve_wasm_stack() {
    size_t stack_size = wasm_stack_size();
    while (!_lru.empty() && _compiled_size + stack_size > _max_compiled_size) {
        evict_modules();
    }
    if (_compiled_size + stack_size > _max_compiled_size) {
        throw wasm::exception("No memory left to execute the WASM function");
    } else {
        _compiled_size += stack_size;
    }
}

void instance_cache::free_wasm_stack() noexcept {
    _compiled_size -= wasm_stack_size();
}

void instance_cache::evict_modules() noexcept {
    size_t prev_size = _compiled_size;
    while (!_lru.empty() && _compiled_size == prev_size) {
        evict_lru();
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

template <>
struct equal_to<seastar::scheduling_group> {
    bool operator()(seastar::scheduling_group& sg1, seastar::scheduling_group& sg2) const noexcept {
        return sg1 == sg2;
    }
};

}
