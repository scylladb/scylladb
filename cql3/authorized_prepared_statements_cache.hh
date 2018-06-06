/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "cql3/prepared_statements_cache.hh"

namespace cql3 {

struct authorized_prepared_statements_cache_size {
    size_t operator()(const statements::prepared_statement::checked_weak_ptr& val) {
        // TODO: improve the size approximation - most of the entry is occupied by the key here.
        return 100;
    }
};

class authorized_prepared_statements_cache_key {
public:
    using cache_key_type = std::pair<auth::authenticated_user, typename cql3::prepared_cache_key_type::cache_key_type>;
private:
    cache_key_type _key;

public:
    authorized_prepared_statements_cache_key(auth::authenticated_user user, cql3::prepared_cache_key_type prepared_cache_key)
        : _key(std::move(user), std::move(prepared_cache_key.key())) {}

    cache_key_type& key() { return _key; }

    const cache_key_type& key() const { return _key; }

    bool operator==(const authorized_prepared_statements_cache_key& other) const {
        return _key == other._key;
    }

    bool operator!=(const authorized_prepared_statements_cache_key& other) const {
        return !(*this == other);
    }

    static size_t hash(const auth::authenticated_user& user, const cql3::prepared_cache_key_type::cache_key_type& prep_cache_key) {
        return utils::hash_combine(std::hash<auth::authenticated_user>()(user), utils::tuple_hash()(prep_cache_key));
    }
};

/// \class authorized_prepared_statements_cache
/// \brief A cache of previously authorized statements.
///
/// Entries are inserted every time a new statement is authorized.
/// Entries are evicted in any of the following cases:
///    - When the corresponding prepared statement is not valid anymore.
///    - Periodically, with the same period as the permission cache is refreshed.
///    - If the corresponding entry hasn't been used for \ref entry_expiry.
class authorized_prepared_statements_cache {
public:
    struct stats {
        uint64_t authorized_prepared_statements_cache_evictions = 0;
    };

    static stats& shard_stats() {
        static thread_local stats _stats;
        return _stats;
    }

    struct authorized_prepared_statements_cache_stats_updater {
        static void inc_hits() noexcept {}
        static void inc_misses() noexcept {}
        static void inc_blocks() noexcept {}
        static void inc_evictions() noexcept {
            ++shard_stats().authorized_prepared_statements_cache_evictions;
        }
    };

private:
    using cache_key_type = authorized_prepared_statements_cache_key;
    using checked_weak_ptr = typename statements::prepared_statement::checked_weak_ptr;
    using cache_type = utils::loading_cache<cache_key_type,
                                            checked_weak_ptr,
                                            utils::loading_cache_reload_enabled::yes,
                                            authorized_prepared_statements_cache_size,
                                            std::hash<cache_key_type>,
                                            std::equal_to<cache_key_type>,
                                            authorized_prepared_statements_cache_stats_updater>;

public:
    using key_type = cache_key_type;
    using value_type = checked_weak_ptr;
    using entry_is_too_big = typename cache_type::entry_is_too_big;
    using iterator = typename cache_type::iterator;

private:
    cache_type _cache;
    logging::logger& _logger;

public:
    // Choose the memory budget such that would allow us ~4K entries when a shard gets 1GB of RAM
    authorized_prepared_statements_cache(std::chrono::milliseconds entry_expiration, std::chrono::milliseconds entry_refresh, size_t cache_size, logging::logger& logger)
        : _cache(cache_size, entry_expiration, entry_refresh, logger, [this] (const key_type& k) {
            _cache.remove(k);
            return make_ready_future<value_type>();
        })
        , _logger(logger)
    {}

    future<> insert(auth::authenticated_user user, cql3::prepared_cache_key_type prep_cache_key, value_type v) noexcept {
        return _cache.get_ptr(key_type(std::move(user), std::move(prep_cache_key)), [v = std::move(v)] (const cache_key_type&) mutable {
            return make_ready_future<value_type>(std::move(v));
        }).discard_result();
    }

    iterator find(const auth::authenticated_user& user, const cql3::prepared_cache_key_type& prep_cache_key) {
        struct key_view {
            const auth::authenticated_user& user_ref;
            const cql3::prepared_cache_key_type& prep_cache_key_ref;
        };

        struct hasher {
            size_t operator()(const key_view& kv) {
                return cql3::authorized_prepared_statements_cache_key::hash(kv.user_ref, kv.prep_cache_key_ref.key());
            }
        };

        struct equal {
            bool operator()(const key_type& k1, const key_view& k2) {
                return k1.key().first == k2.user_ref && k1.key().second == k2.prep_cache_key_ref.key();
            }

            bool operator()(const key_view& k2, const key_type& k1) {
                return operator()(k1, k2);
            }
        };

        return _cache.find(key_view{user, prep_cache_key}, hasher(), equal());
    }

    iterator end() {
        return _cache.end();
    }

    void remove(const auth::authenticated_user& user, const cql3::prepared_cache_key_type& prep_cache_key) {
        iterator it = find(user, prep_cache_key);
        _cache.remove(it);
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
template <>
struct hash<cql3::authorized_prepared_statements_cache_key> final {
    size_t operator()(const cql3::authorized_prepared_statements_cache_key& k) const {
        return cql3::authorized_prepared_statements_cache_key::hash(k.key().first, k.key().second);
    }
};

inline std::ostream& operator<<(std::ostream& out, const cql3::authorized_prepared_statements_cache_key& k) {
    return out << "{ " << k.key().first << ", " << k.key().second << " }";
}
}
