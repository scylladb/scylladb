/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "bytes.hh"
#include "utils/hash.hh"
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
