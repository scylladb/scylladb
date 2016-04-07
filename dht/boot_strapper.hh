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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
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
#include "gms/inet_address.hh"
#include "locator/token_metadata.hh"
#include "dht/i_partitioner.hh"
#include <unordered_set>
#include "database.hh"
#include "db/config.hh"

namespace dht {

class boot_strapper {
    using inet_address = gms::inet_address;
    using token_metadata = locator::token_metadata;
    using token = dht::token;
    distributed<database>& _db;
    /* endpoint that needs to be bootstrapped */
    inet_address _address;
    /* token of the node being bootstrapped. */
    std::unordered_set<token> _tokens;
    token_metadata _token_metadata;
public:
    boot_strapper(distributed<database>& db, inet_address addr, std::unordered_set<token> tokens, token_metadata tmd)
        : _db(db)
        , _address(addr)
        , _tokens(tokens)
        , _token_metadata(tmd) {
    }

    future<> bootstrap();

    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if num_tokens == 1, pick a token to assume half the load of the most-loaded node.
     * else choose num_tokens tokens at random
     */
    static std::unordered_set<token> get_bootstrap_tokens(token_metadata metadata, database& db);

    static std::unordered_set<token> get_random_tokens(token_metadata metadata, size_t num_tokens);
#if 0
    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.NATIVE.sizeof(s);
        }
    }
#endif
};

} // namespace dht
