/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/metrics.hh>

#include "key_cache.hh"

struct key_cache_stats {
    uint64_t key_cache_hits = 0;
    uint64_t key_cache_misses = 0;
    uint64_t key_cache_blocks = 0;
    uint64_t key_cache_evictions = 0;
    uint64_t key_cache_privileged_entries_evictions_on_size = 0;
    uint64_t key_cache_unprivileged_entries_evictions_on_size = 0;
};

template<encryption::key_cache_type Type> 
static key_cache_stats& shard_stats() {
    static thread_local key_cache_stats stats;
    return stats;
}

template<encryption::key_cache_type Type> 
void encryption::detail::key_cache_stats_updater<Type>::inc_hits() noexcept {
    ++shard_stats<Type>().key_cache_hits;
}
template<encryption::key_cache_type Type> 
void encryption::detail::key_cache_stats_updater<Type>::inc_misses() noexcept {
    ++shard_stats<Type>().key_cache_misses;
}
template<encryption::key_cache_type Type> 
void encryption::detail::key_cache_stats_updater<Type>::inc_blocks() noexcept {
    ++shard_stats<Type>().key_cache_blocks;
}
template<encryption::key_cache_type Type> 
void encryption::detail::key_cache_stats_updater<Type>::inc_evictions() noexcept {
    ++shard_stats<Type>().key_cache_evictions;
}
template<encryption::key_cache_type Type> 
void encryption::detail::key_cache_stats_updater<Type>::inc_privileged_on_cache_size_eviction() noexcept {
    ++shard_stats<Type>().key_cache_privileged_entries_evictions_on_size;
}
template<encryption::key_cache_type Type> 
void encryption::detail::key_cache_stats_updater<Type>::inc_unprivileged_on_cache_size_eviction() noexcept {
    ++shard_stats<Type>().key_cache_unprivileged_entries_evictions_on_size;
}

namespace sm = seastar::metrics;

using namespace std::string_literals;

void encryption::register_key_cache_metrics(sm::metric_groups& metrics) {
    for (auto [n, f] : { std::make_pair("id_cache"s, &shard_stats<key_cache_type::id_cache>), std::make_pair("attr_cache"s, &shard_stats<key_cache_type::attr_cache>) }) {
        auto& stats = f();
        metrics.add_group("encryption_key_"s + n, {
            sm::make_counter("hits", stats.key_cache_hits, sm::description("Number of key cache hits")),
            sm::make_counter("misses", stats.key_cache_misses, sm::description("Number of key cache misses")),
            sm::make_counter("blocked", stats.key_cache_blocks, sm::description("Number of key cache blocking load resolves")),
            sm::make_counter("evictions", stats.key_cache_evictions, sm::description("Number of key cache evictions")),
            sm::make_counter("privileged_evictions", stats.key_cache_privileged_entries_evictions_on_size, sm::description("Number of privileged key cache evictions")),
            sm::make_counter("unprivileged_evictions", stats.key_cache_unprivileged_entries_evictions_on_size, sm::description("Number of unprivileged key cache evictions")),
        });
    }
}

template class encryption::detail::key_cache_stats_updater<encryption::key_cache_type::id_cache>;
template class encryption::detail::key_cache_stats_updater<encryption::key_cache_type::attr_cache>;

using namespace std::chrono_literals;

encryption::cache_expiry encryption::adjust_expiry(std::chrono::milliseconds exp, std::chrono::milliseconds ref) {
    auto shard = this_shard_id();
    if (shard == 0) {
        return cache_expiry { exp, ref };
    }
    // Assume shard zero is the master cache. Adjust so we lower refresh than expiry 
    // on other shards -> until expired, keepalive shard 0. But also refreshes a replaced
    // key from shard 0 -> other shard (potentially) earlier.
    // This does not affect any attempts to reach an endpoint service, as only shard 0
    // does those ops.
    auto shard_ref = std::min(ref, exp / 2);
    if (shard_ref < 1ms) {
        shard_ref = exp;
    }
    return cache_expiry {
        exp, shard_ref
    };
}

utils::loading_cache_config encryption::make_key_cache_config(std::chrono::milliseconds exp, std::chrono::milliseconds ref) {
    auto expiry = adjust_expiry(exp, ref);
    return utils::loading_cache_config{ 
        .max_size = std::numeric_limits<size_t>::max(),
        .expiry = expiry.key_cache_expiry,
        .refresh = expiry.key_cache_refresh,
    };
}

