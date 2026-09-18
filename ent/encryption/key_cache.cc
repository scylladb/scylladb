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

static void register_one_key_cache_group(sm::metric_groups& metrics, const char* group_name, key_cache_stats& stats) {
    metrics.add_group(group_name, {
        sm::make_counter("hits", stats.key_cache_hits, sm::description("Number of key cache hits")),
        sm::make_counter("misses", stats.key_cache_misses, sm::description("Number of key cache misses")),
        sm::make_counter("blocked", stats.key_cache_blocks, sm::description("Number of key cache blocking load resolves")),
        sm::make_counter("evictions", stats.key_cache_evictions, sm::description("Number of key cache evictions")),
        sm::make_counter("priviledged_evictions", stats.key_cache_privileged_entries_evictions_on_size, sm::description("Number of priviledged key cache evictions")),
        sm::make_counter("unpriviledged_evictions", stats.key_cache_unprivileged_entries_evictions_on_size, sm::description("Number of unpriviledged key cache evictions")),
    });
}

void encryption::register_key_cache_metrics(sm::metric_groups& metrics) {
    // literal group names required: doc scraper can't parse a runtime-concatenated name
    register_one_key_cache_group(metrics, "encryption_key_id_cache", shard_stats<key_cache_type::id_cache>());
    register_one_key_cache_group(metrics, "encryption_key_attr_cache", shard_stats<key_cache_type::attr_cache>());
}

template class encryption::detail::key_cache_stats_updater<encryption::key_cache_type::id_cache>;
template class encryption::detail::key_cache_stats_updater<encryption::key_cache_type::attr_cache>;
