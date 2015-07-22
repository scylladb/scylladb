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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include "types.hh"
#include "keys.hh"
#include <memory>
#include <random>
#include <utility>

namespace sstables {

class key_view;

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
class token;
class ring_position;

class token {
public:
    enum class kind {
        before_all_keys,
        key,
        after_all_keys,
    };
    kind _kind;
    // _data can be interpreted as a big endian binary fraction
    // in the range [0.0, 1.0).
    //
    // So, [] == 0.0
    //     [0x00] == 0.0
    //     [0x80] == 0.5
    //     [0x00, 0x80] == 1/512
    //     [0xff, 0x80] == 1 - 1/512
    bytes _data;
    token(kind k, bytes d) : _kind(std::move(k)), _data(std::move(d)) {
    }

    bool is_minimum() const {
        return _kind == kind::before_all_keys;
    }

    bool is_maximum() const {
        return _kind == kind::after_all_keys;
    }
};

token midpoint_unsigned(const token& t1, const token& t2);
token minimum_token();
token maximum_token();
bool operator==(const token& t1, const token& t2);
bool operator<(const token& t1, const token& t2);
inline bool operator!=(const token& t1, const token& t2) { return std::rel_ops::operator!=(t1, t2); }
inline bool operator>(const token& t1, const token& t2) { return std::rel_ops::operator>(t1, t2); }
inline bool operator<=(const token& t1, const token& t2) { return std::rel_ops::operator<=(t1, t2); }
inline bool operator>=(const token& t1, const token& t2) { return std::rel_ops::operator>=(t1, t2); }
std::ostream& operator<<(std::ostream& out, const token& t);

template <typename T>
inline auto get_random_number() {
    static thread_local std::random_device rd;
    static thread_local std::default_random_engine re(rd());
    static thread_local std::uniform_int_distribution<T> dist{};
    return dist(re);
}

// Wraps partition_key with its corresponding token.
//
// Total ordering defined by comparators is compatible with Origin's ordering.
class decorated_key {
public:
    dht::token _token;
    partition_key _key;

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

    int tri_compare(const schema& s, const decorated_key& other) const;
    int tri_compare(const schema& s, const ring_position& other) const;

    const dht::token& token() const {
        return _token;
    }

    const partition_key& key() const {
        return _key;
    }
};

class i_partitioner {
public:
    virtual ~i_partitioner() {}

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(const schema& s, const partition_key& key) {
        return { get_token(s, key), key };
    }

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(const schema& s, partition_key&& key) {
        auto token = get_token(s, key);
        return { std::move(token), std::move(key) };
    }

    /**
     * Calculate a token representing the approximate "middle" of the given
     * range.
     *
     * @return The approximate midpoint between left and right.
     */
    virtual token midpoint(const token& left, const token& right) const = 0;

    /**
     * @return A token smaller than all others in the range that is being partitioned.
     * Not legal to assign to a node or key.  (But legal to use in range scans.)
     */
    token get_minimum_token() {
        return dht::minimum_token();
    }

    /**
     * @return a token that can be used to route a given key
     * (This is NOT a method to create a token from its string representation;
     * for that, use tokenFactory.fromString.)
     */
    virtual token get_token(const schema& s, partition_key_view key) = 0;
    virtual token get_token(const sstables::key_view& key) = 0;


    /**
     * @return a partitioner-specific string representation of this token
     */
    virtual sstring to_sstring(const dht::token& t) const = 0;

    /**
     * @return a randomly generated token
     */
    virtual token get_random_token() = 0;

    // FIXME: token.tokenFactory
    //virtual token.tokenFactory gettokenFactory() = 0;

    /**
     * @return True if the implementing class preserves key order in the tokens
     * it generates.
     */
    virtual bool preserves_order() = 0;

    /**
     * Calculate the deltas between tokens in the ring in order to compare
     *  relative sizes.
     *
     * @param sortedtokens a sorted List of tokens
     * @return the mapping from 'token' to 'percentage of the ring owned by that token'.
     */
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) = 0;

    virtual data_type get_token_validator() = 0;

    /**
     * @return name of partitioner.
     */
    virtual const sstring name() = 0;
protected:
    /**
     * @return true if t1's _data array is equal t2's. _kind comparison should be done separately.
     */
    virtual bool is_equal(const token& t1, const token& t2);
    /**
     * @return true if t1's _data array is less then t2's. _kind comparison should be done separately.
     */
    virtual bool is_less(const token& t1, const token& t2);

    friend bool operator==(const token& t1, const token& t2);
    friend bool operator<(const token& t1, const token& t2);
};

//
// Represents position in the ring of partitons, where partitions are ordered
// according to decorated_key ordering (first by token, then by key value).
//
// The 'key' part is optional. When it's absent, this object selects all
// partitions which share given token. When 'key' is present, position is
// further narrowed down to a partition with given key value among all
// partitions which share given token.
//
// Maps to org.apache.cassandra.db.RowPosition and its derivatives in Origin.
// Range bound intricacies are handled separately, by wrapping range<>. This
// allows us to devirtualize this.
//
class ring_position {
    dht::token _token;
    std::experimental::optional<partition_key> _key;
public:
    ring_position(dht::token token) : _token(std::move(token)) {}

    ring_position(dht::token token, partition_key key)
        : _token(std::move(token))
        , _key(std::experimental::make_optional(std::move(key)))
    { }

    ring_position(const dht::decorated_key& dk)
        : _token(dk._token)
        , _key(std::experimental::make_optional(dk._key))
    { }

    const dht::token& token() const {
        return _token;
    }

    const std::experimental::optional<partition_key>& key() const {
        return _key;
    }

    bool has_key() const {
        return bool(_key);
    }

    // Call only when has_key()
    dht::decorated_key as_decorated_key() const {
        return { _token, *_key };
    }

    bool equal(const schema& s, const ring_position& lhr) const {
        if (_token != lhr._token) {
            return false;
        } else if (!_key || !lhr._key){
            return true; // empty key "matches" any other key
        } else {
            return _key->legacy_equal(s, *lhr._key);
        };
    }

    bool less_compare(const schema& s, const ring_position& lhr) const {
        if (_token != lhr._token) {
            return _token < lhr._token;
        } else if (!_key || !lhr._key) {
            return false;
        } else {
            return _key->legacy_tri_compare(s, *lhr._key) < 0;
        }
    }

    size_t serialized_size() const;
    void serialize(bytes::iterator& out) const;
    static ring_position deserialize(bytes_view& in);

    friend std::ostream& operator<<(std::ostream&, const ring_position&);
};

struct ring_position_comparator {
    const schema& s;
    ring_position_comparator(const schema& s_) : s(s_) {}
    int operator()(const ring_position& lh, const ring_position& rh) const;
};

std::ostream& operator<<(std::ostream& out, const token& t);

std::ostream& operator<<(std::ostream& out, const decorated_key& t);

void set_global_partitioner(const sstring& class_name);
i_partitioner& global_partitioner();

unsigned shard_of(const token&);

} // dht

namespace std {
template<>
struct hash<dht::token> {
    size_t operator()(const dht::token& t) const {
        return (t._kind == dht::token::kind::key) ? std::hash<bytes>()(t._data) : 0;
    }
};
}
