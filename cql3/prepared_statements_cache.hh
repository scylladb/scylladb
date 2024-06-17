/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/loading_cache.hh"
#include "utils/hash.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/column_specification.hh"

namespace cql3 {

using prepared_cache_entry = std::unique_ptr<statements::prepared_statement>;

struct prepared_cache_entry_size {
    size_t operator()(const prepared_cache_entry& val) {
        // TODO: improve the size approximation
        return 10000;
    }
};

typedef bytes cql_prepared_id_type;

/// \brief The key of the prepared statements cache
///
/// TODO: consolidate prepared_cache_key_type and the nested cache_key_type
///       the latter was introduced for unifying the CQL and Thrift prepared
///       statements so that they can be stored in the same cache.
class prepared_cache_key_type {
public:
    // derive from cql_prepared_id_type so we can customize the formatter of
    // cache_key_type
    struct cache_key_type : public cql_prepared_id_type {};

private:
    cache_key_type _key;

public:
    prepared_cache_key_type() = default;
    explicit prepared_cache_key_type(cql_prepared_id_type cql_id) : _key(std::move(cql_id)) {}

    cache_key_type& key() { return _key; }
    const cache_key_type& key() const { return _key; }

    static const cql_prepared_id_type& cql_id(const prepared_cache_key_type& key) {
        return key.key();
    }

    bool operator==(const prepared_cache_key_type& other) const = default;
};

class prepared_statements_cache {
public:
    struct stats {
        uint64_t prepared_cache_evictions = 0;
        uint64_t privileged_entries_evictions_on_size = 0;
        uint64_t unprivileged_entries_evictions_on_size = 0;
    };

    static stats& shard_stats() {
        static thread_local stats _stats;
        return _stats;
    }

    struct prepared_cache_stats_updater {
        static void inc_hits() noexcept {}
        static void inc_misses() noexcept {}
        static void inc_blocks() noexcept {}
        static void inc_evictions() noexcept {
            ++shard_stats().prepared_cache_evictions;
        }
        static void inc_privileged_on_cache_size_eviction() noexcept {
            ++shard_stats().privileged_entries_evictions_on_size;
        }
        static void inc_unprivileged_on_cache_size_eviction() noexcept {
            ++shard_stats().unprivileged_entries_evictions_on_size;
        }
    };

private:
    using cache_key_type = typename prepared_cache_key_type::cache_key_type;
    // Keep the entry in the "unprivileged" cache section till 2 hits because
    // every prepared statement is accessed at least twice in the cache:
    //  1) During PREPARE
    //  2) During EXECUTE
    //
    // Therefore a typical "pollution" (when a cache entry is used only once) would involve
    // 2 cache hits.
    using cache_type = utils::loading_cache<cache_key_type, prepared_cache_entry, 2, utils::loading_cache_reload_enabled::no, prepared_cache_entry_size, std::hash<cache_key_type>, std::equal_to<cache_key_type>, prepared_cache_stats_updater, prepared_cache_stats_updater>;
    using cache_value_ptr = typename cache_type::value_ptr;
    using checked_weak_ptr = typename statements::prepared_statement::checked_weak_ptr;

public:
    static const std::chrono::minutes entry_expiry;

    using key_type = prepared_cache_key_type;
    using value_type = checked_weak_ptr;
    using statement_is_too_big = typename cache_type::entry_is_too_big;

private:
    cache_type _cache;

public:
    prepared_statements_cache(logging::logger& logger, size_t size)
        : _cache(size, entry_expiry, logger)
    {}

    template <typename LoadFunc>
    future<value_type> get(const key_type& key, LoadFunc&& load) {
        return _cache.get_ptr(key.key(), [load = std::forward<LoadFunc>(load)] (const cache_key_type&) { return load(); }).then([] (cache_value_ptr v_ptr) {
            return make_ready_future<value_type>((*v_ptr)->checked_weak_from_this());
        });
    }

    // "Touch" the corresponding cache entry in order to bump up its reference count.
    void touch(const key_type& key) {
        // loading_cache::find() returns a value_ptr object which constructor does the "thouching".
        _cache.find(key.key());
    }

    value_type find(const key_type& key) {
        cache_value_ptr vp = _cache.find(key.key());
        if (vp) {
            return (*vp)->checked_weak_from_this();
        }
        return value_type();
    }

    template <typename Pred>
    requires std::is_invocable_r_v<bool, Pred, ::shared_ptr<cql_statement>>
    void remove_if(Pred&& pred) {
        _cache.remove_if([&pred] (const prepared_cache_entry& e) {
            return pred(e->statement);
        });
    }

    size_t size() const {
        return _cache.size();
    }

    size_t memory_footprint() const {
        return _cache.memory_footprint();
    }

    future<> stop() {
        return _cache.stop();
    }
};
}

namespace std {

template<>
struct hash<cql3::prepared_cache_key_type::cache_key_type> final {
    size_t operator()(const cql3::prepared_cache_key_type::cache_key_type& k) const {
        return std::hash<cql3::cql_prepared_id_type>()(k);
    }
};

template<>
struct hash<cql3::prepared_cache_key_type> final {
    size_t operator()(const cql3::prepared_cache_key_type& k) const {
        return std::hash<cql3::cql_prepared_id_type>()(k.key());
    }
};
}

// for prepared_statements_cache log printouts
template <> struct fmt::formatter<cql3::prepared_cache_key_type::cache_key_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql3::prepared_cache_key_type::cache_key_type& p, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{cql_id: {}}}", static_cast<const cql3::cql_prepared_id_type&>(p));
    }
};

template <> struct fmt::formatter<cql3::prepared_cache_key_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql3::prepared_cache_key_type& p, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", p.key());
    }
};
