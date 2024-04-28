/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/prepared_statements_cache.hh"
#include "auth/authenticated_user.hh"

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

    struct view {
        const auth::authenticated_user& user_ref;
        const cql3::prepared_cache_key_type& prep_cache_key_ref;
    };

    struct view_hasher {
        size_t operator()(const view& kv) {
            return cql3::authorized_prepared_statements_cache_key::hash(kv.user_ref, kv.prep_cache_key_ref.key());
        }
    };

    struct view_equal {
        bool operator()(const authorized_prepared_statements_cache_key& k1, const view& k2) {
            return k1.key().first == k2.user_ref && k1.key().second == k2.prep_cache_key_ref.key();
        }

        bool operator()(const view& k2, const authorized_prepared_statements_cache_key& k1) {
            return operator()(k1, k2);
        }
    };

private:
    cache_key_type _key;

public:
    authorized_prepared_statements_cache_key(auth::authenticated_user user, cql3::prepared_cache_key_type prepared_cache_key)
        : _key(std::move(user), std::move(prepared_cache_key.key())) {}

    cache_key_type& key() { return _key; }

    const cache_key_type& key() const { return _key; }

    bool operator==(const authorized_prepared_statements_cache_key&) const = default;

    static size_t hash(const auth::authenticated_user& user, const cql3::prepared_cache_key_type::cache_key_type& prep_cache_key) {
        return utils::hash_combine(std::hash<auth::authenticated_user>()(user),
                                   std::hash<cql3::prepared_cache_key_type::cache_key_type>()(prep_cache_key));
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
        uint64_t authorized_prepared_statements_privileged_entries_evictions_on_size = 0;
        uint64_t authorized_prepared_statements_unprivileged_entries_evictions_on_size = 0;
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

        static void inc_privileged_on_cache_size_eviction() noexcept {
            ++shard_stats().authorized_prepared_statements_privileged_entries_evictions_on_size;
        }

        static void inc_unprivileged_on_cache_size_eviction() noexcept {
            ++shard_stats().authorized_prepared_statements_unprivileged_entries_evictions_on_size;
        }
    };

private:
    using cache_key_type = authorized_prepared_statements_cache_key;
    using checked_weak_ptr = typename statements::prepared_statement::checked_weak_ptr;
    using cache_type = utils::loading_cache<cache_key_type,
                                            checked_weak_ptr,
                                            1,
                                            utils::loading_cache_reload_enabled::yes,
                                            authorized_prepared_statements_cache_size,
                                            std::hash<cache_key_type>,
                                            std::equal_to<cache_key_type>,
                                            authorized_prepared_statements_cache_stats_updater,
                                            authorized_prepared_statements_cache_stats_updater>;

public:
    using key_type = cache_key_type;
    using key_view_type = typename key_type::view;
    using key_view_hasher = typename key_type::view_hasher;
    using key_view_equal = typename key_type::view_equal;
    using value_type = checked_weak_ptr;
    using entry_is_too_big = typename cache_type::entry_is_too_big;
    using value_ptr = typename cache_type::value_ptr;
private:
    cache_type _cache;

public:
    // Choose the memory budget such that would allow us ~4K entries when a shard gets 1GB of RAM
    authorized_prepared_statements_cache(utils::loading_cache_config c, logging::logger& logger)
        : _cache(std::move(c), logger, [this] (const key_type& k) {
            _cache.remove(k);
            return make_ready_future<value_type>();
        })
    {}

    future<> insert(auth::authenticated_user user, cql3::prepared_cache_key_type prep_cache_key, value_type v) noexcept {
        return _cache.get_ptr(key_type(std::move(user), std::move(prep_cache_key)), [v = std::move(v)] (const cache_key_type&) mutable {
            return make_ready_future<value_type>(std::move(v));
        }).discard_result();
    }

    value_ptr find(const auth::authenticated_user& user, const cql3::prepared_cache_key_type& prep_cache_key) {
        return _cache.find(key_view_type{user, prep_cache_key}, key_view_hasher(), key_view_equal());
    }

    void remove(const auth::authenticated_user& user, const cql3::prepared_cache_key_type& prep_cache_key) {
        _cache.remove(key_view_type{user, prep_cache_key}, key_view_hasher(), key_view_equal());
    }

    size_t size() const {
        return _cache.size();
    }

    size_t memory_footprint() const {
        return _cache.memory_footprint();
    }

    bool update_config(utils::loading_cache_config c) {
        return _cache.update_config(std::move(c));
    }

    void reset() {
        _cache.reset();
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

}

template <>
struct fmt::formatter<cql3::authorized_prepared_statements_cache_key> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql3::authorized_prepared_statements_cache_key& k, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}, {}}}", k.key().first, k.key().second);
    }
};
