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

#include "UUID_gen.hh"

#include <stdlib.h>
#include <atomic>
#include "hashers.hh"

namespace utils {

static int64_t make_node()
{
    // FIXME: Mix-in node's address. See the above commented-out code
    // which is what Cassandra's UUIDGen.java did. We can also get the MAC address.

    // We should take current core number under consideration
    // because create_time_safe() doesn't synchronize across cores and
    // it's easy to get duplicates.
    static std::atomic<unsigned> core_counter;
    return core_counter.fetch_add(1);
}


int64_t make_clock_seq_and_node()
{
    thread_local std::mt19937_64 engine(std::random_device().operator()());
    thread_local std::uniform_int_distribution<int> dist;
    // The original Java code did this, shuffling the number of millis
    // since the epoch, and taking 14 bits of it. We don't do exactly
    // the same, but the idea is the same.
    //long clock = new Random(System.currentTimeMillis()).nextLong();
    int clock = dist(engine);

    long lsb = 0;
    lsb |= 0x8000000000000000L;                 // variant (2 bits)
    lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
    lsb |= make_node();                          // 6 bytes
    return lsb;
}

UUID UUID_gen::get_name_UUID(bytes_view b) {
    return get_name_UUID(reinterpret_cast<const unsigned char*>(b.begin()), b.size());
}

UUID UUID_gen::get_name_UUID(sstring_view s) {
    static_assert(sizeof(char) == sizeof(sstring_view::value_type), "Assumed that str.size() counts in chars");
    return get_name_UUID(reinterpret_cast<const unsigned char*>(s.begin()), s.size());
}

UUID UUID_gen::get_name_UUID(const unsigned char *s, size_t len) {
    bytes digest = md5_hasher::calculate(std::string_view(reinterpret_cast<const char*>(s), len));

    // set version to 3
    digest[6] &= 0x0f;
    digest[6] |= 0x30;

    // set variant to IETF variant
    digest[8] &= 0x3f;
    digest[8] |= 0x80;

    return get_UUID(digest);
}

thread_local const std::unique_ptr<UUID_gen> UUID_gen::instance (new UUID_gen());


} // namespace utils
