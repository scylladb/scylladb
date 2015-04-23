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
#include "types.hh"
#include "keys.hh"
#include <memory>

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
};

token midpoint(const token& t1, const token& t2);
token minimum_token();
bool operator==(const token& t1, const token& t2);
bool operator<(const token& t1, const token& t2);
std::ostream& operator<<(std::ostream& out, const token& t);


class decorated_key {
public:
    token _token;
    partition_key _key;
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
    decorated_key decorate_key(const partition_key& key) {
        return { get_token(key), key };
    }

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(partition_key&& key) {
        auto token = get_token(key);
        return { std::move(token), std::move(key) };
    }

    /**
     * Calculate a token representing the approximate "middle" of the given
     * range.
     *
     * @return The approximate midpoint between left and right.
     */
    token midpoint(const token& left, const token& right) {
        return dht::midpoint(left, right);
    }

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
    virtual token get_token(const partition_key& key) = 0;

    /**
     * @return a randomly generated token
     */
    token get_random_token();

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

bool operator<(const decorated_key& lht, const decorated_key& rht);

bool operator==(const decorated_key& lht, const decorated_key& rht);

bool operator!=(const decorated_key& lht, const decorated_key& rht);

std::ostream& operator<<(std::ostream& out, const token& t);

std::ostream& operator<<(std::ostream& out, const decorated_key& t);

i_partitioner& global_partitioner();

} // dht

namespace std {
template<>
struct hash<dht::token> {
    size_t operator()(const dht::token& t) const {
        return (t._kind == dht::token::kind::key) ? std::hash<bytes>()(t._data) : 0;
    }
};
}
