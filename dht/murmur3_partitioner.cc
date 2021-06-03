/*
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

#include "murmur3_partitioner.hh"
#include "utils/murmur_hash.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include <boost/lexical_cast.hpp>
#include <boost/range/irange.hpp>

namespace dht {

token
murmur3_partitioner::get_token(bytes_view key) const {
    if (key.empty()) {
        return minimum_token();
    }
    std::array<uint64_t, 2> hash;
    utils::murmur_hash::hash3_x64_128(key, 0, hash);
    return get_token(hash[0]);
}

token
murmur3_partitioner::get_token(uint64_t value) const {
    return token(token::kind::key, value);
}

token
murmur3_partitioner::get_token(const sstables::key_view& key) const {
    return get_token(bytes_view(key));
}

token
murmur3_partitioner::get_token(const schema& s, partition_key_view key) const {
    std::array<uint64_t, 2> hash;
    auto&& legacy = key.legacy_form(s);
    utils::murmur_hash::hash3_x64_128(legacy.begin(), legacy.size(), 0, hash);
    return get_token(hash[0]);
}

using registry = class_registrator<i_partitioner, murmur3_partitioner>;
static registry registrator("org.apache.cassandra.dht.Murmur3Partitioner");
static registry registrator_short_name("Murmur3Partitioner");

}


