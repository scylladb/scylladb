/*
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

#include "murmur3_partitioner.hh"
#include "utils/murmur_hash.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include <boost/lexical_cast.hpp>
#include <boost/range/irange.hpp>

namespace dht {

inline
unsigned
murmur3_partitioner::zero_based_shard_of(uint64_t token, unsigned shards, unsigned sharding_ignore_msb_bits) {
    // This is the master function, the inverses have to match it wrt. rounding errors.
    token <<= sharding_ignore_msb_bits;
    // Treat "token" as a fraction in the interval [0, 1); compute:
    //    shard = floor((0.token) * shards)
    return (uint128_t(token) * shards) >> 64;
}

std::vector<uint64_t>
murmur3_partitioner::init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits) {
    // computes the inverse of zero_based_shard_of(). ret[s] will return the smallest token that belongs to s
    if (shards == 1) {
        // Avoid the while loops below getting confused finding the "edge" between two nonexistent shards
        return std::vector<uint64_t>(1, uint64_t(0));
    }
    auto ret = std::vector<uint64_t>(shards);
    for (auto s : boost::irange<unsigned>(0, shards)) {
        uint64_t token = (uint128_t(s) << 64) / shards;
        token >>= sharding_ignore_msb_bits;   // leftmost bits are ignored by zero_based_shard_of
        // token is the start of the next shard, and can be slightly before due to rounding errors; adjust
        while (zero_based_shard_of(token, shards, sharding_ignore_msb_bits) != s) {
            ++token;
        }
        ret[s] = token;
    }
    return ret;
}

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

inline int64_t long_token(const token& t) {
    if (t.is_minimum() || t.is_maximum()) {
        return std::numeric_limits<long>::min();
    }

    return t._data;
}

uint64_t
murmur3_partitioner::unbias(const token& t) const {
    return uint64_t(long_token(t)) + uint64_t(std::numeric_limits<int64_t>::min());
}

token
murmur3_partitioner::bias(uint64_t n) const {
    return get_token(n - uint64_t(std::numeric_limits<int64_t>::min()));
}

unsigned
murmur3_partitioner::shard_of(const token& t) const {
    return shard_of(t, _shard_count, _sharding_ignore_msb_bits);
}

unsigned
murmur3_partitioner::shard_of(const token& t, unsigned shard_count, unsigned sharding_ignore_msb) const {
    switch (t._kind) {
        case dht::token::kind::before_all_keys:
            return 0;
        case dht::token::kind::after_all_keys:
            return shard_count - 1;
        case dht::token::kind::key:
            uint64_t adjusted = unbias(t);
            return zero_based_shard_of(adjusted, shard_count, sharding_ignore_msb);
    }
    abort();
}

token
murmur3_partitioner::token_for_next_shard(const token& t, shard_id shard, unsigned spans) const {
    uint64_t n = 0;
    switch (t._kind) {
        case token::kind::before_all_keys:
            break;
        case token::kind::after_all_keys:
            return maximum_token();
        case token::kind::key:
            n = unbias(t);
            break;
    }
    auto s = zero_based_shard_of(n, _shard_count, _sharding_ignore_msb_bits);

    if (!_sharding_ignore_msb_bits) {
        // This ought to be the same as the else branch, but avoids shifts by 64
        n = _shard_start[shard];
        if (spans > 1 || shard <= s) {
            return maximum_token();
        }
    } else {
        auto left_part = n >> (64 - _sharding_ignore_msb_bits);
        left_part += spans - unsigned(shard > s);
        if (left_part >= (1u << _sharding_ignore_msb_bits)) {
            return maximum_token();
        }
        left_part <<= (64 - _sharding_ignore_msb_bits);
        auto right_part = _shard_start[shard];
        n = left_part | right_part;
    }
    return bias(n);
}

unsigned
murmur3_partitioner::sharding_ignore_msb() const {
    return _sharding_ignore_msb_bits;
}


using registry = class_registrator<i_partitioner, murmur3_partitioner, const unsigned&, const unsigned&>;
static registry registrator("org.apache.cassandra.dht.Murmur3Partitioner");
static registry registrator_short_name("Murmur3Partitioner");

}


