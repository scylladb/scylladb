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
#include <memory>

namespace dht {

class decorated_key;
class token;

class i_partitioner {
public:
    virtual ~i_partitioner() {}
    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    virtual shared_ptr<decorated_key> decorate_key(bytes key) = 0;

    /**
     * Calculate a token representing the approximate "middle" of the given
     * range.
     *
     * @return The approximate midpoint between left and right.
     */
    virtual shared_ptr<token> midpoint(token& left, token& right) = 0;

    /**
     * @return A token smaller than all others in the range that is being partitioned.
     * Not legal to assign to a node or key.  (But legal to use in range scans.)
     */
    virtual shared_ptr<token> get_minimum_token() = 0;

    /**
     * @return a token that can be used to route a given key
     * (This is NOT a method to create a token from its string representation;
     * for that, use tokenFactory.fromString.)
     */
    virtual shared_ptr<token> get_token(bytes key) = 0;

    /**
     * @return a randomly generated token
     */
    virtual shared_ptr<token> get_random_token() = 0;

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
    virtual std::map<shared_ptr<token>, float> describe_ownership(std::vector<shared_ptr<token>> sorted_tokens) = 0;

    virtual data_type get_token_validator() = 0;
};

} // dht
