/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "utils/loading_cache.hh"
#include "cql3/statements/prepared_statement.hh"

namespace cql3 {

using prepared_cache_entry = std::unique_ptr<statements::prepared_statement>;

struct prepared_cache_entry_size {
    size_t operator()(const prepared_cache_entry& val) {
        // TODO: improve the size approximation
        return 10000;
    }
};

typedef bytes cql_prepared_id_type;
typedef int32_t thrift_prepared_id_type;

/// \brief The key of the prepared statements cache
///
/// We are going to store the CQL and Thrift prepared statements in the same cache therefore we need generate the key
/// that is going to be unique in both cases. Thrift use int32_t as a prepared statement ID, CQL - MD5 digest.
///
/// We are going to use an std::pair<CQL_PREP_ID_TYPE, int64_t> as a key. For CQL statements we will use {CQL_PREP_ID, std::numeric_limits<int64_t>::max()} as a key
/// and for Thrift - {CQL_PREP_ID_TYPE(0), THRIFT_PREP_ID}. This way CQL and Thrift keys' values will never collide.
class prepared_cache_key_type {
public:
    using cache_key_type = std::pair<cql_prepared_id_type, int64_t>;

private:
    cache_key_type _key;

public:
    prepared_cache_key_type() = default;
    explicit prepared_cache_key_type(cql_prepared_id_type cql_id) : _key(std::move(cql_id), std::numeric_limits<int64_t>::max()) {}
    explicit prepared_cache_key_type(thrift_prepared_id_type thrift_id) : _key(cql_prepared_id_type(), thrift_id) {}

    cache_key_type& key() { return _key; }
    const cache_key_type& key() const { return _key; }

    static const cql_prepared_id_type& cql_id(const prepared_cache_key_type& key) {
        return key.key().first;
    }
    static thrift_prepared_id_type thrift_id(const prepared_cache_key_type& key) {
        return key.key().second;
    }

    bool operator==(const prepared_cache_key_type& other) const {
        return _key == other._key;
    }

    bool operator!=(const prepared_cache_key_type& other) const {
        return !(*this == other);
    }
};

class prepared_statements_cache {
public:
    struct stats {
        uint64_t prepared_cache_evictions = 0;
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
    };

private:
    using cache_key_type = typename prepared_cache_key_type::cache_key_type;
    using cache_type = utils::loading_cache<cache_key_type, prepared_cache_entry, utils::loading_cache_reload_enabled::no, prepared_cache_entry_size, utils::tuple_hash, std::equal_to<cache_key_type>, prepared_cache_stats_updater>;
    using cache_value_ptr = typename cache_type::value_ptr;
    using cache_iterator = typename cache_type::iterator;
    using checked_weak_ptr = typename statements::prepared_statement::checked_weak_ptr;
    struct value_extractor_fn {
        checked_weak_ptr operator()(prepared_cache_entry& e) const {
            return e->checked_weak_from_this();
        }
    };

public:
    static const std::chrono::minutes entry_expiry;

    using key_type = prepared_cache_key_type;
    using value_type = checked_weak_ptr;
    using statement_is_too_big = typename cache_type::entry_is_too_big;
    /// \note both iterator::reference and iterator::value_type are checked_weak_ptr
    using iterator = boost::transform_iterator<value_extractor_fn, cache_iterator>;

private:
    cache_type _cache;
    value_extractor_fn _value_extractor_fn;

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

    iterator find(const key_type& key) {
        return boost::make_transform_iterator(_cache.find(key.key()), _value_extractor_fn);
    }

    iterator end() {
        return boost::make_transform_iterator(_cache.end(), _value_extractor_fn);
    }

    iterator begin() {
        return boost::make_transform_iterator(_cache.begin(), _value_extractor_fn);
    }

    template <typename Pred>
    void remove_if(Pred&& pred) {
        static_assert(std::is_same<bool, std::result_of_t<Pred(::shared_ptr<cql_statement>)>>::value, "Bad Pred signature");

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

namespace std { // for prepared_statements_cache log printouts
inline std::ostream& operator<<(std::ostream& os, const typename cql3::prepared_cache_key_type::cache_key_type& p) {
    os << "{cql_id: " << p.first << ", thrift_id: " << p.second << "}";
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const cql3::prepared_cache_key_type& p) {
    os << p.key();
    return os;
}

template<>
struct hash<cql3::prepared_cache_key_type> final {
    size_t operator()(const cql3::prepared_cache_key_type& k) const {
        return utils::tuple_hash()(k.key());
    }
};
}
