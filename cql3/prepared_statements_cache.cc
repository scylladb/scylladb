/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/prepared_statements_cache.hh"
#include "cql3/cql_statement.hh"

namespace cql3 {

prepared_cache_entry::~prepared_cache_entry() {
    if (_owner && _stmt) {
        _owner->unindex(_key, _generation, _dependent_tables);
    }
}

prepared_cache_entry& prepared_cache_entry::operator=(prepared_cache_entry&& o) noexcept {
    if (this != &o) {
        if (_owner && _stmt) {
            _owner->unindex(_key, _generation, _dependent_tables);
        }
        _stmt = std::move(o._stmt);
        _key = std::move(o._key);
        _owner = o._owner;
        _dependent_tables = std::move(o._dependent_tables);
        _generation = o._generation;
    }
    return *this;
}

std::vector<dependent_table> prepared_statements_cache::index(const cache_key_type& key, uint64_t generation, const statements::prepared_statement& stmt) {
    auto tables = stmt.statement->dependent_tables();
    try {
        for (auto& t : tables) {
            // Overwrites an older, still-pinned entry's generation: it is out of the cache already.
            _table_index[t.id].insert_or_assign(key, generation);
            _keyspace_table_index[t.ks_name][t.cf_name].insert(t.id);
        }
    } catch (...) {
        for (auto& t : tables) {
            unindex_one(key, generation, t);
        }
        throw;
    }
    return tables;
}

void prepared_statements_cache::unindex(const cache_key_type& key, uint64_t generation, const std::vector<dependent_table>& dependent_tables) noexcept {
    for (auto& tk : dependent_tables) {
        unindex_one(key, generation, tk);
    }
}

void prepared_statements_cache::unindex_one(const cache_key_type& key, uint64_t generation, const dependent_table& tk) noexcept {
    auto it = _table_index.find(tk.id);
    if (it != _table_index.end()) {
        auto kit = it->second.find(key);
        // A newer entry owns `key` for this table.
        if (kit == it->second.end() || kit->second != generation) {
            return;
        }
        it->second.erase(kit);
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

prepared_statements_cache::prepared_statements_cache(logging::logger& logger, size_t size)
    : _cache(size, entry_expiry, logger)
{}

void prepared_statements_cache::touch(const key_type& key) {
    // loading_cache::find() returns a value_ptr object which constructor does the "thouching".
    _cache.find(key.key());
}

prepared_statements_cache::value_type prepared_statements_cache::find(const key_type& key) {
    cache_value_ptr vp = _cache.find(key.key());
    if (vp) {
        return (*vp)->checked_weak_from_this();
    }
    return value_type();
}

size_t prepared_statements_cache::size() const {
    return _cache.size();
}

size_t prepared_statements_cache::memory_footprint() const {
    return _cache.memory_footprint();
}

future<> prepared_statements_cache::stop() {
    return _cache.stop();
}

void prepared_statements_cache::remove_for_table(const sstring& ks_name, const std::optional<sstring>& cf_name) {
    // In-flight loads aren't indexed yet, so their tables are unknown: drop them all.
    remove_loading();
    remove_indexed(ks_name, cf_name);
}

void prepared_statements_cache::remove_indexed(const sstring& ks_name, const std::optional<sstring>& cf_name) {
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

void prepared_statements_cache::remove_loading() {
    auto loading = std::move(_loading);
    _loading.clear();
    for (auto& [gen, k] : loading) {
        _cache.remove(k);
    }
}

void prepared_statements_cache::remove_for_table_id(const table_id& tid) {
    auto it = _table_index.find(tid);
    if (it == _table_index.end()) {
        return;
    }
    // Detach first: entry destructors call unindex(), mutating _table_index.
    auto keys = std::move(it->second);
    _table_index.erase(it);
    for (auto& k : keys) {
        _cache.remove(k.first);
    }
}

}
