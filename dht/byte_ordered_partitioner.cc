/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "byte_ordered_partitioner.hh"
#include "utils/class_registrator.hh"

namespace dht {

token byte_ordered_partitioner::get_random_token()
{
    bytes b(bytes::initialized_later(), 16);
    *unaligned_cast<uint64_t>(b.begin()) = dht::get_random_number<uint64_t>();
    *unaligned_cast<uint64_t>(b.begin() + 8) = dht::get_random_number<uint64_t>();
    return token(token::kind::key, std::move(b));
}

std::map<token, float> byte_ordered_partitioner::describe_ownership(const std::vector<token>& sorted_tokens)
{
    throw std::runtime_error("not implemented");
}

token byte_ordered_partitioner::midpoint(const token& t1, const token& t2) const
{
    throw std::runtime_error("not implemented");
}

unsigned
byte_ordered_partitioner::shard_of(const token& t) const {
    switch (t._kind) {
        case token::kind::before_all_keys:
            return 0;
        case token::kind::after_all_keys:
            return smp::count - 1;
        case token::kind::key:
            if (t._data.empty()) {
                return 0;
            }
            // treat first byte as a fraction in the range [0, 1) and divide it evenly:
            return (uint8_t(t._data[0]) * smp::count) >> 8;
    }
    assert(0);
}

using registry = class_registrator<i_partitioner, byte_ordered_partitioner>;
static registry registrator("org.apache.cassandra.dht.ByteOrderedPartitioner");
static registry registrator_short_name("ByteOrderedPartitioner");

}
