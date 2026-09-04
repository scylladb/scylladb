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
#include <string_view>

#include "utils/loading_cache.hh"
#include "utils/hash.hh"
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

/// \brief Owns the prepared_statement and keeps prepared_statements_cache's
/// per-table index in sync: on destruction (LRU/size eviction, explicit
/// removal, or targeted invalidation) it removes its own cache key from
/// every table bucket it was indexed under.
///
/// Stores the dependent_table list computed at index time so the destructor
/// doesn't need to call dependent_tables() again, keeping unindex() noexcept.
///
/// A cache key can outlive the entry it was assigned to: an entry pinned by
/// a caller survives eviction/removal from the cache, and a fresh entry can
/// be loaded and indexed under the very same key before the old one is
/// destroyed. _generation identifies which of those entries currently owns
/// the key, so the old entry's destructor doesn't unindex the new entry.
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
    prepared_cache_entry& operator=(prepared_cache_entry&&) = default;
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

// Transparent equality for the (ks_name, cf_name) pair key, comparing via
// string_view so lookups don't need to construct owning sstrings.
struct name_pair_equal {
    using is_transparent = void;
    using spair = std::pair<std::string_view, std::string_view>;
    bool operator()(spair lhs, spair rhs) const {
        return lhs == rhs;
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

    // table_id -> cache keys of statements whose dependent_tables()
    // includes it. Lets a schema change on one table find exactly the
    // statements it may need to invalidate, instead of scanning the whole
    // cache. Entries for statements that were evicted through some other path
    // (LRU/size eviction) are pruned by prepared_cache_entry's destructor, so
    // this stays in sync without a dedicated eviction hook in loading_cache.
    std::unordered_map<table_id, std::unordered_set<cache_key_type>> _table_index;
    // (keyspace, table) name -> table_ids, and keyspace -> table_ids. Schema
    // change notifications (migration_listener) only carry names, and by the
    // time a drop notification fires the table's schema is no longer
    // registered anywhere to recover its id from, so these are maintained
    // purely to translate names back to the ids _table_index is keyed by.
    //
    // A set, not a single table_id: if a table is dropped and recreated
    // under the same name before the drop notification is processed, both
    // the old and new table_id can be indexed under the same name at once,
    // and the drop must invalidate the old one specifically.
    std::unordered_map<std::pair<sstring, sstring>, std::unordered_set<table_id>, utils::tuple_hash, name_pair_equal> _name_index;
    std::unordered_map<sstring, std::unordered_set<table_id>> _keyspace_index;

    // Which generation (see prepared_cache_entry) currently owns a cache key's
    // indexing. Lets unindex() tell an old, stale entry apart from a newer one
    // that has since been loaded and indexed under the same key.
    uint64_t _next_generation = 0;
    std::unordered_map<cache_key_type, uint64_t> _key_generation;

    friend class prepared_cache_entry;

    // Indexes a newly-loaded statement; returns what it inserted, plus the
    // generation it was indexed under, so the owning prepared_cache_entry can
    // pass them back to unindex() later without recomputing dependent_tables().
    //
    // Unwinds already-inserted tables if dependent_tables() throws partway.
    std::pair<uint64_t, std::vector<dependent_table>> index(const cache_key_type& key, const cql_statement& stmt) {
        auto tables = stmt.dependent_tables();
        uint64_t generation = ++_next_generation;
        _key_generation[key] = generation;
        auto it = tables.begin();
        try {
            for (; it != tables.end(); ++it) {
                _table_index[it->id].insert(key);
                _name_index[{it->ks_name, it->cf_name}].insert(it->id);
                _keyspace_index[it->ks_name].insert(it->id);
            }
        } catch (...) {
            // unindex_one is idempotent (no-op on missing entries), so it's safe to
            // include `it` itself: one of its three inserts may have already landed.
            for (auto undo_it = tables.begin(); undo_it != std::next(it); ++undo_it) {
                unindex_one(key, *undo_it);
            }
            throw;
        }
        return {generation, std::move(tables)};
    }

    // Uses only data the entry already owns, so this genuinely can't throw.
    //
    // Identity-safe: if a newer entry has since been loaded and indexed under
    // `key` (the calling entry was pinned past its removal from the cache),
    // that generation mismatch means this is a stale entry, and it must leave
    // the newer entry's indexing alone.
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
        // Heterogeneous lookup (name_pair_equal) avoids allocating owning
        // sstrings here, keeping this function genuinely noexcept.
        auto name_it = _name_index.find(std::pair<std::string_view, std::string_view>{tk.ks_name, tk.cf_name});
        if (name_it != _name_index.end()) {
            name_it->second.erase(tk.id);
            if (name_it->second.empty()) {
                _name_index.erase(name_it);
            }
        }
        auto ks_it = _keyspace_index.find(tk.ks_name);
        if (ks_it != _keyspace_index.end()) {
            ks_it->second.erase(tk.id);
            if (ks_it->second.empty()) {
                _keyspace_index.erase(ks_it);
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
        return _cache.get_ptr(key.key(), [this, load = std::forward<LoadFunc>(load)] (const cache_key_type& k) {
            return load().then([this, k] (std::unique_ptr<statements::prepared_statement> stmt) {
                auto [generation, tables] = index(k, *stmt->statement);
                // If wrapping into the entry below throws, undo index() so the
                // statement isn't left indexed without an owning entry to unindex it.
                // Pass tables by copy (not moved) so it's still valid for that rollback.
                try {
                    return prepared_cache_entry(std::move(stmt), k, this, tables, generation);
                } catch (...) {
                    unindex(k, generation, tables);
                    throw;
                }
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

    // Removes every cached statement that depends on the given table (or, if
    // cf_name is not set, every table in the given keyspace). O(matching
    // statements), via the per-table index, instead of a full cache scan.
    void remove_for_table(const sstring& ks_name, const std::optional<sstring>& cf_name) {
        if (cf_name) {
            auto it = _name_index.find({ks_name, *cf_name});
            if (it == _name_index.end()) {
                return;
            }
            // Copy out: a table can be dropped and recreated under the same
            // name before this drop notification is processed, so more than
            // one table_id may be indexed under it; invalidate all of them.
            auto table_ids = std::move(it->second);
            for (auto& tid : table_ids) {
                remove_for_table_id(tid);
            }
            return;
        }
        auto it = _keyspace_index.find(ks_name);
        if (it == _keyspace_index.end()) {
            return;
        }
        auto table_ids = std::move(it->second);
        _keyspace_index.erase(it);
        for (auto& tid : table_ids) {
            remove_for_table_id(tid);
        }
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
    void remove_for_table_id(const table_id& tid) {
        auto it = _table_index.find(tid);
        if (it == _table_index.end()) {
            return;
        }
        // Copy out: removing entries from the cache destroys prepared_cache_entry
        // objects, whose destructor calls back into unindex() and mutates _table_index.
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
