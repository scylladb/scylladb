/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "types.hh"
#include "keys.hh"
#include "utils/managed_bytes.hh"
#include <memory>
#include <random>
#include <utility>
#include <vector>
#include <compare>
#include "range.hh"
#include <byteswap.h>
#include "dht/token.hh"
#include "dht/token-sharding.hh"
#include "utils/maybe_yield.hh"

namespace sstables {

class key_view;
class decorated_key_view;

}

namespace dht {

//
// Origin uses a complex class hierarchy where Token is an abstract class,
// and various subclasses use different implementations (LongToken vs.
// BigIntegerToken vs. StringToken), plus other variants to to signify the
// the beginning of the token space etc.
//
// We'll fold all of that into the token class and push all of the variations
// into its users.

class decorated_key;
class ring_position;

using partition_range = nonwrapping_range<ring_position>;
using token_range = nonwrapping_range<token>;

using partition_range_vector = std::vector<partition_range>;
using token_range_vector = std::vector<token_range>;

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

class i_partitioner {
public:
    i_partitioner() = default;
    virtual ~i_partitioner() {}

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(const schema& s, const partition_key& key) const {
        return { get_token(s, key), key };
    }

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(const schema& s, partition_key&& key) const {
        auto token = get_token(s, key);
        return { std::move(token), std::move(key) };
    }

    /**
     * @return a token that can be used to route a given key
     * (This is NOT a method to create a token from its string representation;
     * for that, use tokenFactory.fromString.)
     */
    virtual token get_token(const schema& s, partition_key_view key) const = 0;
    virtual token get_token(const sstables::key_view& key) const = 0;

    // FIXME: token.tokenFactory
    //virtual token.tokenFactory gettokenFactory() = 0;

    /**
     * @return name of partitioner.
     */
    virtual const sstring name() const = 0;

    bool operator==(const i_partitioner& o) const {
        return name() == o.name();
    }
    bool operator!=(const i_partitioner& o) const {
        return !(*this == o);
    }
};

//
// Represents position in the ring of partitions, where partitions are ordered
// according to decorated_key ordering (first by token, then by key value).
// Intended to be used for defining partition ranges.
//
// The 'key' part is optional. When it's absent, this object represents a position
// which is either before or after all keys sharing given token. That's determined
// by relation_to_keys().
//
// For example for the following data:
//
//   tokens: |    t1   | t2 |
//           +----+----+----+
//   keys:   | k1 | k2 | k3 |
//
// The ordering is:
//
//   ring_position(t1, token_bound::start) < ring_position(k1)
//   ring_position(k1)                     < ring_position(k2)
//   ring_position(k1)                     == decorated_key(k1)
//   ring_position(k2)                     == decorated_key(k2)
//   ring_position(k2)                     < ring_position(t1, token_bound::end)
//   ring_position(k2)                     < ring_position(k3)
//   ring_position(t1, token_bound::end)   < ring_position(t2, token_bound::start)
//
// Maps to org.apache.cassandra.db.RowPosition and its derivatives in Origin.
//
class ring_position {
public:
    enum class token_bound : int8_t { start = -1, end = 1 };
private:
    friend class ring_position_comparator;
    friend class ring_position_ext;
    dht::token _token;
    token_bound _token_bound{}; // valid when !_key
    std::optional<partition_key> _key;
public:
    static ring_position min() noexcept {
        return { minimum_token(), token_bound::start };
    }

    static ring_position max() noexcept {
        return { maximum_token(), token_bound::end };
    }

    bool is_min() const noexcept {
        return _token.is_minimum();
    }

    bool is_max() const noexcept {
        return _token.is_maximum();
    }

    static ring_position starting_at(dht::token token) {
        return { std::move(token), token_bound::start };
    }

    static ring_position ending_at(dht::token token) {
        return { std::move(token), token_bound::end };
    }

    ring_position(dht::token token, token_bound bound)
        : _token(std::move(token))
        , _token_bound(bound)
    { }

    ring_position(dht::token token, partition_key key)
        : _token(std::move(token))
        , _key(std::make_optional(std::move(key)))
    { }

    ring_position(dht::token token, token_bound bound, std::optional<partition_key> key)
        : _token(std::move(token))
        , _token_bound(bound)
        , _key(std::move(key))
    { }

    ring_position(const dht::decorated_key& dk)
        : _token(dk._token)
        , _key(std::make_optional(dk._key))
    { }

    ring_position(dht::decorated_key&& dk)
        : _token(std::move(dk._token))
        , _key(std::make_optional(std::move(dk._key)))
    { }

    const dht::token& token() const noexcept {
        return _token;
    }

    // Valid when !has_key()
    token_bound bound() const {
        return _token_bound;
    }

    // Returns -1 if smaller than keys with the same token, +1 if greater.
    int relation_to_keys() const {
        return _key ? 0 : static_cast<int>(_token_bound);
    }

    const std::optional<partition_key>& key() const {
        return _key;
    }

    bool has_key() const {
        return bool(_key);
    }

    // Call only when has_key()
    dht::decorated_key as_decorated_key() const {
        return { _token, *_key };
    }

    bool equal(const schema&, const ring_position&) const;

    // Trichotomic comparator defining a total ordering on ring_position objects
    std::strong_ordering tri_compare(const schema&, const ring_position&) const;

    // "less" comparator corresponding to tri_compare()
    bool less_compare(const schema&, const ring_position&) const;

    friend std::ostream& operator<<(std::ostream&, const ring_position&);
};

// Non-owning version of ring_position and ring_position_ext.
//
// Unlike ring_position, it can express positions which are right after and right before the keys.
// ring_position still can not because it is sent between nodes and such a position
// would not be (yet) properly interpreted by old nodes. That's why any ring_position
// can be converted to ring_position_view, but not the other way.
//
// It is possible to express a partition_range using a pair of two ring_position_views v1 and v2,
// where v1 = ring_position_view::for_range_start(r) and v2 = ring_position_view::for_range_end(r).
// Such range includes all keys k such that v1 <= k < v2, with order defined by ring_position_comparator.
//
class ring_position_view {
    friend std::strong_ordering ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh);
    friend class ring_position_comparator;
    friend class ring_position_comparator_for_sstables;
    friend class ring_position_ext;

    // Order is lexicographical on (_token, _key) tuples, where _key part may be missing, and
    // _weight affecting order between tuples if one is a prefix of the other (including being equal).
    // A positive weight puts the position after all strictly prefixed by it, while a non-positive
    // weight puts it before them. If tuples are equal, the order is further determined by _weight.
    //
    // For example {_token=t1, _key=nullptr, _weight=1} is ordered after {_token=t1, _key=k1, _weight=0},
    // but {_token=t1, _key=nullptr, _weight=-1} is ordered before it.
    //
    const dht::token* _token; // always not nullptr
    const partition_key* _key; // Can be nullptr
    int8_t _weight;
public:
    using token_bound = ring_position::token_bound;
    struct after_key_tag {};
    using after_key = bool_class<after_key_tag>;

    static ring_position_view min() noexcept {
        return { minimum_token(), nullptr, -1 };
    }

    static ring_position_view max() noexcept {
        return { maximum_token(), nullptr, 1 };
    }

    bool is_min() const noexcept {
        return _token->is_minimum();
    }

    bool is_max() const noexcept {
        return _token->is_maximum();
    }

    static ring_position_view for_range_start(const partition_range& r) {
        return r.start() ? ring_position_view(r.start()->value(), after_key(!r.start()->is_inclusive())) : min();
    }

    static ring_position_view for_range_end(const partition_range& r) {
        return r.end() ? ring_position_view(r.end()->value(), after_key(r.end()->is_inclusive())) : max();
    }

    static ring_position_view for_after_key(const dht::decorated_key& dk) {
        return ring_position_view(dk, after_key::yes);
    }

    static ring_position_view for_after_key(dht::ring_position_view view) {
        return ring_position_view(after_key_tag(), view);
    }

    static ring_position_view starting_at(const dht::token& t) {
        return ring_position_view(t, token_bound::start);
    }

    static ring_position_view ending_at(const dht::token& t) {
        return ring_position_view(t, token_bound::end);
    }

    ring_position_view(const dht::ring_position& pos, after_key after = after_key::no)
        : _token(&pos.token())
        , _key(pos.has_key() ? &*pos.key() : nullptr)
        , _weight(pos.has_key() ? bool(after) : pos.relation_to_keys())
    { }

    ring_position_view(const ring_position_view& pos) = default;
    ring_position_view& operator=(const ring_position_view& other) = default;

    ring_position_view(after_key_tag, const ring_position_view& v)
        : _token(v._token)
        , _key(v._key)
        , _weight(v._key ? 1 : v._weight)
    { }

    ring_position_view(const dht::decorated_key& key, after_key after_key = after_key::no)
        : _token(&key.token())
        , _key(&key.key())
        , _weight(bool(after_key))
    { }

    ring_position_view(const dht::token& token, const partition_key* key, int8_t weight)
        : _token(&token)
        , _key(key)
        , _weight(weight)
    { }

    explicit ring_position_view(const dht::token& token, token_bound bound = token_bound::start)
        : _token(&token)
        , _key(nullptr)
        , _weight(static_cast<std::underlying_type_t<token_bound>>(bound))
    { }

    const dht::token& token() const noexcept { return *_token; }
    const partition_key* key() const { return _key; }

    // Only when key() == nullptr
    token_bound get_token_bound() const { return token_bound(_weight); }
    // Only when key() != nullptr
    after_key is_after_key() const { return after_key(_weight == 1); }

    friend std::ostream& operator<<(std::ostream&, ring_position_view);
};

using ring_position_ext_view = ring_position_view;

//
// Represents position in the ring of partitions, where partitions are ordered
// according to decorated_key ordering (first by token, then by key value).
// Intended to be used for defining partition ranges.
//
// Unlike ring_position, it can express positions which are right after and right before the keys.
// ring_position still can not because it is sent between nodes and such a position
// would not be (yet) properly interpreted by old nodes. That's why any ring_position
// can be converted to ring_position_ext, but not the other way.
//
// It is possible to express a partition_range using a pair of two ring_position_exts v1 and v2,
// where v1 = ring_position_ext::for_range_start(r) and v2 = ring_position_ext::for_range_end(r).
// Such range includes all keys k such that v1 <= k < v2, with order defined by ring_position_comparator.
//
class ring_position_ext {
    // Order is lexicographical on (_token, _key) tuples, where _key part may be missing, and
    // _weight affecting order between tuples if one is a prefix of the other (including being equal).
    // A positive weight puts the position after all strictly prefixed by it, while a non-positive
    // weight puts it before them. If tuples are equal, the order is further determined by _weight.
    //
    // For example {_token=t1, _key=nullptr, _weight=1} is ordered after {_token=t1, _key=k1, _weight=0},
    // but {_token=t1, _key=nullptr, _weight=-1} is ordered before it.
    //
    dht::token _token;
    std::optional<partition_key> _key;
    int8_t _weight;
public:
    using token_bound = ring_position::token_bound;
    struct after_key_tag {};
    using after_key = bool_class<after_key_tag>;

    static ring_position_ext min() noexcept {
        return { minimum_token(), std::nullopt, -1 };
    }

    static ring_position_ext max() noexcept {
        return { maximum_token(), std::nullopt, 1 };
    }

    bool is_min() const noexcept {
        return _token.is_minimum();
    }

    bool is_max() const noexcept {
        return _token.is_maximum();
    }

    static ring_position_ext for_range_start(const partition_range& r) {
        return r.start() ? ring_position_ext(r.start()->value(), after_key(!r.start()->is_inclusive())) : min();
    }

    static ring_position_ext for_range_end(const partition_range& r) {
        return r.end() ? ring_position_ext(r.end()->value(), after_key(r.end()->is_inclusive())) : max();
    }

    static ring_position_ext for_after_key(const dht::decorated_key& dk) {
        return ring_position_ext(dk, after_key::yes);
    }

    static ring_position_ext for_after_key(dht::ring_position_ext view) {
        return ring_position_ext(after_key_tag(), view);
    }

    static ring_position_ext starting_at(const dht::token& t) {
        return ring_position_ext(t, token_bound::start);
    }

    static ring_position_ext ending_at(const dht::token& t) {
        return ring_position_ext(t, token_bound::end);
    }

    ring_position_ext(const dht::ring_position& pos, after_key after = after_key::no)
        : _token(pos.token())
        , _key(pos.key())
        , _weight(pos.has_key() ? bool(after) : pos.relation_to_keys())
    { }

    ring_position_ext(const ring_position_ext& pos) = default;
    ring_position_ext& operator=(const ring_position_ext& other) = default;

    ring_position_ext(ring_position_view v)
        : _token(*v._token)
        , _key(v._key ? std::make_optional(*v._key) : std::nullopt)
        , _weight(v._weight)
    { }

    ring_position_ext(after_key_tag, const ring_position_ext& v)
        : _token(v._token)
        , _key(v._key)
        , _weight(v._key ? 1 : v._weight)
    { }

    ring_position_ext(const dht::decorated_key& key, after_key after_key = after_key::no)
        : _token(key.token())
        , _key(key.key())
        , _weight(bool(after_key))
    { }

    ring_position_ext(dht::token token, std::optional<partition_key> key, int8_t weight) noexcept
        : _token(std::move(token))
        , _key(std::move(key))
        , _weight(weight)
    { }

    ring_position_ext(ring_position&& pos) noexcept
        : _token(std::move(pos._token))
        , _key(std::move(pos._key))
        , _weight(pos.relation_to_keys())
    { }

    explicit ring_position_ext(const dht::token& token, token_bound bound = token_bound::start)
        : _token(token)
        , _key(std::nullopt)
        , _weight(static_cast<std::underlying_type_t<token_bound>>(bound))
    { }

    const dht::token& token() const noexcept { return _token; }
    const std::optional<partition_key>& key() const { return _key; }

    // Only when key() == std::nullopt
    token_bound get_token_bound() const { return token_bound(_weight); }

    // Only when key() != std::nullopt
    after_key is_after_key() const { return after_key(_weight == 1); }

    operator ring_position_view() const { return { _token, _key ? &*_key : nullptr, _weight }; }

    friend std::ostream& operator<<(std::ostream&, const ring_position_ext&);
};

std::strong_ordering ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh);

template <typename T>
requires std::is_convertible<T, ring_position_view>::value
ring_position_view ring_position_view_to_compare(const T& val) {
    return val;
}

// Trichotomic comparator for ring order
struct ring_position_comparator {
    const schema& s;
    ring_position_comparator(const schema& s_) : s(s_) {}

    std::strong_ordering operator()(ring_position_view lh, ring_position_view rh) const {
        return ring_position_tri_compare(s, lh, rh);
    }

    template <typename T>
    std::strong_ordering operator()(const T& lh, ring_position_view rh) const {
        return ring_position_tri_compare(s, ring_position_view_to_compare(lh), rh);
    }

    template <typename T>
    std::strong_ordering operator()(ring_position_view lh, const T& rh) const {
        return ring_position_tri_compare(s, lh, ring_position_view_to_compare(rh));
    }

    template <typename T1, typename T2>
    std::strong_ordering operator()(const T1& lh, const T2& rh) const {
        return ring_position_tri_compare(s, ring_position_view_to_compare(lh), ring_position_view_to_compare(rh));
    }
};

struct ring_position_comparator_for_sstables {
    const schema& s;
    ring_position_comparator_for_sstables(const schema& s_) : s(s_) {}
    std::strong_ordering operator()(ring_position_view, sstables::decorated_key_view) const;
    std::strong_ordering operator()(sstables::decorated_key_view, ring_position_view) const;
};

// "less" comparator giving the same order as ring_position_comparator
struct ring_position_less_comparator {
    ring_position_comparator tri;

    ring_position_less_comparator(const schema& s) : tri(s) {}

    template<typename T, typename U>
    bool operator()(const T& lh, const U& rh) const {
        return tri(lh, rh) < 0;
    }
};

struct token_comparator {
    // Return values are those of a trichotomic comparison.
    std::strong_ordering operator()(const token& t1, const token& t2) const;
};

std::ostream& operator<<(std::ostream& out, const token& t);

std::ostream& operator<<(std::ostream& out, const decorated_key& t);

std::ostream& operator<<(std::ostream& out, const i_partitioner& p);

class partition_ranges_view {
    const dht::partition_range* _data = nullptr;
    size_t _size = 0;

public:
    partition_ranges_view() = default;
    partition_ranges_view(const dht::partition_range& range) : _data(&range), _size(1) {}
    partition_ranges_view(const dht::partition_range_vector& ranges) : _data(ranges.data()), _size(ranges.size()) {}
    bool empty() const { return _size == 0; }
    size_t size() const { return _size; }
    const dht::partition_range& front() const { return *_data; }
    const dht::partition_range& back() const { return *(_data + _size - 1); }
    const dht::partition_range* begin() const { return _data; }
    const dht::partition_range* end() const { return _data + _size; }
};
std::ostream& operator<<(std::ostream& out, partition_ranges_view v);

unsigned shard_of(const schema&, const token&);
inline decorated_key decorate_key(const schema& s, const partition_key& key) {
    return s.get_partitioner().decorate_key(s, key);
}
inline decorated_key decorate_key(const schema& s, partition_key&& key) {
    return s.get_partitioner().decorate_key(s, std::move(key));
}

inline token get_token(const schema& s, partition_key_view key) {
    return s.get_partitioner().get_token(s, key);
}

dht::partition_range to_partition_range(dht::token_range);
dht::partition_range_vector to_partition_ranges(const dht::token_range_vector& ranges, utils::can_yield can_yield = utils::can_yield::no);

// Each shard gets a sorted, disjoint vector of ranges
std::map<unsigned, dht::partition_range_vector>
split_range_to_shards(dht::partition_range pr, const schema& s);

// Intersect a partition_range with a shard and return the the resulting sub-ranges, in sorted order
future<utils::chunked_vector<partition_range>> split_range_to_single_shard(const schema& s, const dht::partition_range& pr, shard_id shard);

std::unique_ptr<dht::i_partitioner> make_partitioner(sstring name);

} // dht

namespace std {
template<>
struct hash<dht::token> {
    size_t operator()(const dht::token& t) const {
        // We have to reverse the bytes here to keep compatibility with
        // the behaviour that was here when tokens were represented as
        // sequence of bytes.
        return bswap_64(t._data);
    }
};

template <>
struct hash<dht::decorated_key> {
    size_t operator()(const dht::decorated_key& k) const {
        auto h_token = hash<dht::token>();
        return h_token(k.token());
    }
};


}
