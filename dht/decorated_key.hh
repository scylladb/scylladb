/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "keys/keys.hh"
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
    // The token is stored as a raw 8-byte value (without the token kind)
    // instead of a full dht::token (16 bytes), to save memory. Decorated keys
    // only ever carry key-kind tokens or the before_all_keys sentinel
    // (dht::minimum_token(), used as an initial/empty marker); both round-trip
    // losslessly through the raw value. after_all_keys is never stored here.
    raw_token _token;
    partition_key _key;

    decorated_key(dht::token t, partition_key k)
        // minimum_token() (before_all_keys) is stored as a disengaged raw_token; key tokens go through
        // raw_token(const token&), which asserts kind==key in DEBUG so that an after_all_keys token (which
        // would silently collapse onto token::last() via raw()==INT64_MAX) is caught instead of stored.
        : _token(t.is_minimum() ? raw_token() : raw_token(t))
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

    dht::token token() const noexcept {
        return dht::token(_token);
    }

    const partition_key& key() const {
        return _key;
    }

    size_t external_memory_usage() const {
        return _key.external_memory_usage();
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
        return fmt::format_to(ctx.out(), "{{key: {}, token: {}}}", dk._key, dk.token());
    }
};
