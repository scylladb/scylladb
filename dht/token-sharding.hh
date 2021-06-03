/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "dht/token.hh"

namespace dht {

inline sstring cpu_sharding_algorithm_name() {
    return "biased-token-round-robin";
}

std::vector<uint64_t> init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits);

unsigned shard_of(unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t);

token token_for_next_shard(const std::vector<uint64_t>& shard_start, unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t, shard_id shard, unsigned spans);

class sharder {
protected:
    unsigned _shard_count;
    unsigned _sharding_ignore_msb_bits;
    std::vector<uint64_t> _shard_start;
public:
    sharder(unsigned shard_count = smp::count, unsigned sharding_ignore_msb_bits = 0);
    virtual ~sharder() = default;
    /**
     * Calculates the shard that handles a particular token.
     */
    virtual unsigned shard_of(const token& t) const;

    /**
     * Gets the first token greater than `t` that is in shard `shard`, and is a shard boundary (its first token).
     *
     * If the `spans` parameter is greater than zero, the result is the same as if the function
     * is called `spans` times, each time applied to its return value, but efficiently. This allows
     * selecting ranges that include multiple round trips around the 0..smp::count-1 shard span:
     *
     *     token_for_next_shard(t, shard, spans) == token_for_next_shard(token_for_shard(t, shard, 1), spans - 1)
     *
     * On overflow, maximum_token() is returned.
     */
    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans = 1) const;

    /**
     * @return number of shards configured for this partitioner
     */
    unsigned shard_count() const {
        return _shard_count;
    }

    unsigned sharding_ignore_msb() const {
        return _sharding_ignore_msb_bits;
    }

    bool operator==(const sharder& o) const {
        return _shard_count == o._shard_count && _sharding_ignore_msb_bits == o._sharding_ignore_msb_bits;
    }

    bool operator!=(const sharder& o) const {
        return !(*this == o);
    }

};

inline std::ostream& operator<<(std::ostream& os, const sharder& sharder) {
    os << "sharder[shard_count=" << sharder.shard_count()
       << ", ignore_msb_bits="<< sharder.sharding_ignore_msb() << "]";
    return os;
}

/*
 * Finds the first token in token range (`start`, `end`] that belongs to shard shard_idx.
 *
 * If there is no token that belongs to shard shard_idx in this range,
 * `end` is returned.
 *
 * The first token means the one that appears first on the ring when going
 * from `start` to `end`.
 * 'first token' is not always the smallest.
 * For example, if in vnode (100, 10] only tokens 110 and 1 belong to
 * shard shard_idx then token 110 is the first because it appears first
 * when going from 100 to 10 on the ring.
 */
dht::token find_first_token_for_shard(
        const dht::sharder& sharder, dht::token start, dht::token end, size_t shard_idx);

} //namespace dht
