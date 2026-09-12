/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/loading_cache.hh"

namespace seastar::metrics {
class metric_groups;
}

namespace encryption {

enum class key_cache_type : uint8_t {
    id_cache, attr_cache
};

namespace detail {

template<key_cache_type Type> 
struct key_cache_stats_updater {
    static void inc_hits() noexcept;
    static void inc_misses() noexcept;
    static void inc_blocks() noexcept;
    static void inc_evictions() noexcept;
    static void inc_privileged_on_cache_size_eviction() noexcept;
    static void inc_unprivileged_on_cache_size_eviction() noexcept;
};

}

void register_key_cache_metrics(seastar::metrics::metric_groups&);

template<key_cache_type Type,
     typename Key,
     typename Tp,
     typename Hash = std::hash<Key>,
     typename EqualPred = std::equal_to<Key>>
using key_cache = utils::loading_cache<Key, Tp, 2
        , utils::loading_cache_reload_enabled::yes
        , utils::simple_entry_size<Tp>
        , Hash
        , EqualPred
        , detail::key_cache_stats_updater<Type>
        , detail::key_cache_stats_updater<Type>
        >;

template<typename Key,
     typename Tp,
     typename Hash = std::hash<Key>,
     typename EqualPred = std::equal_to<Key>>
using id_cache = key_cache<key_cache_type::id_cache, Key, Tp, Hash, EqualPred>;

template<typename Key,
     typename Tp,
     typename Hash = std::hash<Key>,
     typename EqualPred = std::equal_to<Key>>
using attr_cache = key_cache<key_cache_type::attr_cache, Key, Tp, Hash, EqualPred>;

}
