/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "utils/loading_cache.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/column_specification.hh"
#include "cql3/cql_statement.hh"
#include "cql3/dialect.hh"

namespace cql3 {

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
    struct cache_key_type : public cql_prepared_id_type {
        cache_key_type(cql_prepared_id_type&& id, cql3::dialect d) : cql_prepared_id_type(std::move(id)), dialect(d) {}
        cql3::dialect dialect; // Not part of hash, but we don't expect collisions because of that
        bool operator==(const cache_key_type& other) const = default;
    };

private:
    cache_key_type _key;

public:
    explicit prepared_cache_key_type(cql_prepared_id_type cql_id, dialect d) : _key(std::move(cql_id), d) {}

    cache_key_type& key() { return _key; }
    const cache_key_type& key() const { return _key; }

    static const cql_prepared_id_type& cql_id(const prepared_cache_key_type& key) {
        return key.key();
    }

    bool operator==(const prepared_cache_key_type& other) const = default;
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
        return fmt::format_to(ctx.out(), "{{cql_id: {}, dialect: {}}}", static_cast<const cql3::cql_prepared_id_type&>(p), p.dialect);
    }
};

template <> struct fmt::formatter<cql3::prepared_cache_key_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql3::prepared_cache_key_type& p, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", p.key());
    }
};

namespace cql3 {

class prepared_statements_cache;

/// Owns the prepared_statement; its destructor unindexes the key from the cache's per-table index.
/// _generation tells a stale entry (pinned past removal) from a newer one loaded under the same key.
class prepared_cache_entry {
    std::unique_ptr<statements::prepared_statement> _stmt;
    prepared_cache_key_type::cache_key_type _key;
    prepared_statements_cache* _owner = nullptr;
    std::vector<dependent_table> _dependent_tables;
    uint64_t _generation;

public:
    prepared_cache_entry(std::unique_ptr<statements::prepared_statement> stmt, prepared_cache_key_type::cache_key_type key,
            prepared_statements_cache* owner, std::vector<dependent_table> dependent_tables, uint64_t generation)
        : _stmt(std::move(stmt)), _key(std::move(key)), _owner(owner), _dependent_tables(std::move(dependent_tables)), _generation(generation)
    {}
    prepared_cache_entry(prepared_cache_entry&&) = default;
    // Swap so the moved-from source unindexes our old statement when destroyed.
    prepared_cache_entry& operator=(prepared_cache_entry&& o) noexcept {
        std::swap(_stmt, o._stmt);
        std::swap(_key, o._key);
        std::swap(_owner, o._owner);
        std::swap(_dependent_tables, o._dependent_tables);
        std::swap(_generation, o._generation);
        return *this;
    }
    prepared_cache_entry(const prepared_cache_entry&) = delete;

    ~prepared_cache_entry();

    statements::prepared_statement* get() const { return _stmt.get(); }
    statements::prepared_statement* operator->() const { return _stmt.get(); }
    statements::prepared_statement& operator*() const { return *_stmt; }
    explicit operator bool() const { return bool(_stmt); }
};

struct prepared_cache_entry_size {
    size_t operator()(const prepared_cache_entry& val) {
        // TODO: improve the size approximation
        return 10000;
    }
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

    // table_id -> keys of statements depending on it; pruned by prepared_cache_entry's destructor on any eviction.
    std::unordered_map<table_id, std::unordered_set<cache_key_type>> _table_index;
    // keyspace -> table name -> table_ids, to map name-only schema notifications to ids.
    // A set: a drop+recreate under the same name can leave both ids indexed.
    std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<table_id>>> _keyspace_table_index;

    // Current owner generation per cache key, so unindex() ignores stale entries.
    uint64_t _next_generation = 0;
    std::unordered_map<cache_key_type, uint64_t> _key_generation;

    // In-flight loads (generation -> key); invalidation removes them too so a stale-schema load isn't cached.
    // Orphaned loads are never indexed, so they can't steal a newer entry's generation.
    std::unordered_map<uint64_t, cache_key_type> _loading;

    friend class prepared_cache_entry;

    // Returns the tables indexed; rolls back via unindex() if an insertion throws.
    std::vector<dependent_table> index(const cache_key_type& key, uint64_t generation, const cql_statement& stmt) {
        auto tables = stmt.dependent_tables();
        try {
            _key_generation[key] = generation;
            for (auto& t : tables) {
                _table_index[t.id].insert(key);
                _keyspace_table_index[t.ks_name][t.cf_name].insert(t.id);
            }
        } catch (...) {
            unindex(key, generation, tables);
            throw;
        }
        return tables;
    }

    // No-op if a newer entry owns `key` (generation mismatch).
    void unindex(const cache_key_type& key, uint64_t generation, const std::vector<dependent_table>& dependent_tables) noexcept {
        auto git = _key_generation.find(key);
        if (git == _key_generation.end() || git->second != generation) {
            return;
        }
        _key_generation.erase(git);
        for (auto& tk : dependent_tables) {
            unindex_one(key, tk);
        }
    }

    void unindex_one(const cache_key_type& key, const dependent_table& tk) noexcept {
        auto it = _table_index.find(tk.id);
        if (it != _table_index.end()) {
            it->second.erase(key);
            if (!it->second.empty()) {
                return;
            }
            _table_index.erase(it);
        }
        auto ks_it = _keyspace_table_index.find(tk.ks_name);
        if (ks_it != _keyspace_table_index.end()) {
            auto& table_map = ks_it->second;
            auto name_it = table_map.find(tk.cf_name);
            if (name_it != table_map.end()) {
                name_it->second.erase(tk.id);
                if (name_it->second.empty()) {
                    table_map.erase(name_it);
                }
            }
            if (table_map.empty()) {
                _keyspace_table_index.erase(ks_it);
            }
        }
    }

public:
    static const std::chrono::minutes entry_expiry;

    using key_type = prepared_cache_key_type;
    using pinned_value_type = cache_value_ptr;
    using value_type = checked_weak_ptr;
    using statement_is_too_big = typename cache_type::entry_is_too_big;

private:
    cache_type _cache;

public:
    prepared_statements_cache(logging::logger& logger, size_t size)
        : _cache(size, entry_expiry, logger)
    {}

    template <typename LoadFunc>
    future<pinned_value_type> get_pinned(const key_type& key, LoadFunc&& load) {
        return _cache.get_ptr(key.key(), [this, load = std::forward<LoadFunc>(load)] (const cache_key_type& k) mutable {
            auto generation = ++_next_generation;
            _loading.emplace(generation, k);
            return futurize_invoke(load).then_wrapped([this, k, generation] (future<std::unique_ptr<statements::prepared_statement>> f) mutable {
                bool live = _loading.erase(generation);
                if (f.failed()) {
                    return make_exception_future<prepared_cache_entry>(f.get_exception());
                }
                auto stmt = f.get();
                if (!live) {
                    return make_ready_future<prepared_cache_entry>(std::move(stmt), std::move(k), nullptr, std::vector<dependent_table>{}, generation);
                }
                auto tables = index(k, generation, *stmt->statement);
                return make_ready_future<prepared_cache_entry>(std::move(stmt), std::move(k), this, std::move(tables), generation);
            });
        });
    }

    template <typename LoadFunc>
    future<value_type> get(const key_type& key, LoadFunc&& load) {
        return get_pinned(key, std::forward<LoadFunc>(load)).then([] (cache_value_ptr v_ptr) {
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

    // Removes statements depending on the table, or on any table of the keyspace if cf_name is unset.
    void remove_for_table(const sstring& ks_name, const std::optional<sstring>& cf_name) {
        remove_loading();
        remove_indexed(ks_name, cf_name);
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

private:
    void remove_indexed(const sstring& ks_name, const std::optional<sstring>& cf_name) {
        auto ks_it = _keyspace_table_index.find(ks_name);
        if (ks_it == _keyspace_table_index.end()) {
            return;
        }
        // Detach from the name index before removing: entry destructors mutate it.
        if (cf_name) {
            auto& table_map = ks_it->second;
            auto name_it = table_map.find(*cf_name);
            if (name_it == table_map.end()) {
                return;
            }
            auto table_ids = std::move(name_it->second);
            table_map.erase(name_it);
            if (table_map.empty()) {
                _keyspace_table_index.erase(ks_it);
            }
            for (auto& tid : table_ids) {
                remove_for_table_id(tid);
            }
            return;
        }
        auto tables = std::move(ks_it->second);
        _keyspace_table_index.erase(ks_it);
        for (auto& [cf, ids] : tables) {
            for (auto& tid : ids) {
                remove_for_table_id(tid);
            }
        }
    }

    void remove_loading() {
        auto loading = std::move(_loading);
        _loading.clear();
        for (auto& [gen, k] : loading) {
            _cache.remove(k);
        }
    }

    void remove_for_table_id(const table_id& tid) {
        auto it = _table_index.find(tid);
        if (it == _table_index.end()) {
            return;
        }
        // Detach first: entry destructors call unindex(), mutating _table_index.
        auto keys = std::move(it->second);
        _table_index.erase(it);
        for (auto& k : keys) {
            _cache.remove(k);
        }
    }
};

inline prepared_cache_entry::~prepared_cache_entry() {
    if (_owner && _stmt) {
        _owner->unindex(_key, _generation, _dependent_tables);
    }
}

}
