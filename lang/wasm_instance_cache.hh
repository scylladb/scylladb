/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/functions/function_name.hh"
#include <list>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <unordered_map>
#include "lang/wasm.hh"
#include "rust/cxx.h"
#include "rust/wasmtime_bindings.hh"

namespace wasm {

struct wasm_instance {
    rust::Box<wasmtime::Store> store;
    rust::Box<wasmtime::Instance> instance;
    rust::Box<wasmtime::Func> func;
    rust::Box<wasmtime::Memory> memory;
};

// For each UDF full name and a scheduling group, we store a wasmtime instance
// that is usable after acquiring a corresponding mutex. This way, the instance
// can't be used in multiple continuations from the same scheduling group at the
// same time.
// The instance may be evicted only when it is not used, i.e. when the corresponding
// mutex is not held. When the instance is used, its size is not tracked, but
// it's limited by the size of its memory - it can't exceed a set value (1MB).
// After the instance stops being used, a timestamp of last use is recorded,
// and its size is added to the total size of all instances. Other, older instances
// may be evicted if the total size of all instances exceeds a set value (100MB).
// If the instance is not used for at least _timer_period, it is evicted after
// at most another _timer_period.
// Entries in the cache are created on the first use of a UDF in a given scheduling
// and they are stored in memory until the UDF is dropped. The number of such
// entries is limited by the number of stored UDFs multiplied by the number of
// scheduling groups.
class instance_cache {
public:
    struct stats {
        uint64_t cache_hits = 0;
        uint64_t cache_misses = 0;
        uint64_t cache_blocks = 0;
    };

private:
    stats _stats;
    seastar::metrics::metric_groups _metrics;

public:
    stats& shard_stats();

    void setup_metrics();

private:
    using cache_key_type = db::functions::function_name;

    struct lru_entry_type;

    struct cache_entry_type {
        seastar::scheduling_group scheduling_group;
        std::vector<data_type> arg_types;
        seastar::shared_mutex mutex;
        std::optional<wasm_instance> instance;
        // iterator points to _lru.end() when the entry is being used (at that point, it is not in lru)
        std::list<lru_entry_type>::iterator it;
    };

public:
    using value_type = lw_shared_ptr<cache_entry_type>;

private:
    struct lru_entry_type {
        value_type cache_entry;
        seastar::lowres_clock::time_point timestamp;
        size_t instance_size;
    };

private:
    std::unordered_multimap<cache_key_type, value_type> _cache;
    std::list<lru_entry_type> _lru;
    seastar::timer<seastar::lowres_clock> _timer;
    // The instance in cache time out after up to 2*_timer_period.
    seastar::lowres_clock::duration _timer_period;
    size_t _total_size = 0;
    size_t _max_size;
    size_t _max_instance_size;

public:
    explicit instance_cache(size_t size, size_t instance_size, seastar::lowres_clock::duration timer_period);

private:
    wasm_instance load(wasm::context& ctx);

    void evict_lru() noexcept;

    void on_timer() noexcept;

public:
    seastar::future<value_type> get(const db::functions::function_name& name, const std::vector<data_type>& arg_types, wasm::context& ctx);

    void recycle(value_type inst) noexcept;

    void remove(const db::functions::function_name& name, const std::vector<data_type>& arg_types) noexcept;

    size_t size() const;

    size_t max_size() const;

    size_t memory_footprint() const;

    future<> stop();
};

}
