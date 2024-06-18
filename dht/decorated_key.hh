/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "keys.hh"
#include "schema/schema_fwd.hh"
#include "dht/token.hh"
#include "dht/i_partitioner_fwd.hh"

namespace dht {

//
// Origin uses a complex class hierarchy where Token is an abstract class,
// and various subclasses use different implementations (LongToken vs.
// BigIntegerToken vs. StringToken), plus other variants to to signify the
// the beginning of the token space etc.
//
// We'll fold all of that into the token class and push all of the variations
// into its users.

// Wraps partition_key with its corresponding token.
//
// Total ordering defined by comparators is compatible with Origin's ordering.
class decorated_key {
public:
    dht::token _token;
    partition_key _key;

    decorated_key(dht::token t, partition_key k)
        : _token(std::move(t))
        , _key(std::move(k)) {
    }

    struct less_comparator {
        schema_ptr s;
        less_comparator(schema_ptr s);
        bool operator()(const decorated_key& k1, const decorated_key& k2) const;
        bool operator()(const decorated_key& k1, const ring_position& k2) const;
        bool operator()(const ring_position& k1, const decorated_key& k2) const;
    };

    bool equal(const schema& s, const decorated_key& other) const;

    bool less_compare(const schema& s, const decorated_key& other) const;
    bool less_compare(const schema& s, const ring_position& other) const;

    // Trichotomic comparators defining total ordering on the union of
    // decorated_key and ring_position objects.
    std::strong_ordering tri_compare(const schema& s, const decorated_key& other) const;
    std::strong_ordering tri_compare(const schema& s, const ring_position& other) const;

    const dht::token& token() const noexcept {
        return _token;
    }

    const partition_key& key() const {
        return _key;
    }

    size_t external_memory_usage() const {
        return _key.external_memory_usage() + _token.external_memory_usage();
    }

    size_t memory_usage() const {
        return sizeof(decorated_key) + external_memory_usage();
    }
};

class decorated_key_equals_comparator {
    const schema& _schema;
public:
    explicit decorated_key_equals_comparator(const schema& schema) : _schema(schema) {}
    bool operator()(const dht::decorated_key& k1, const dht::decorated_key& k2) const {
        return k1.equal(_schema, k2);
    }
};

using decorated_key_opt = std::optional<decorated_key>;

} // namespace dht

namespace std {

template <>
struct hash<dht::decorated_key> {
    size_t operator()(const dht::decorated_key& k) const {
        auto h_token = hash<dht::token>();
        return h_token(k.token());
    }
};

} // namespace std

template <> struct fmt::formatter<dht::decorated_key> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const dht::decorated_key& dk, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{key: {}, token: {}}}", dk._key, dk._token);
    }
};
