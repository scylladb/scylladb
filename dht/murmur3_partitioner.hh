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

#pragma once

#include "i_partitioner.hh"
#include "bytes.hh"
#include <vector>

namespace dht {

class murmur3_partitioner final : public i_partitioner {
    unsigned _sharding_ignore_msb_bits;
    std::vector<uint64_t> _shard_start = init_zero_based_shard_start(_shard_count, _sharding_ignore_msb_bits);
public:
    murmur3_partitioner(unsigned shard_count = smp::count, unsigned sharding_ignore_msb_bits = 0)
            : i_partitioner(shard_count)
            // if one shard, ignore sharding_ignore_msb_bits as they will just cause needless
            // range breaks
            , _sharding_ignore_msb_bits(shard_count > 1 ? sharding_ignore_msb_bits : 0) {
    }
    virtual const sstring name() const { return "org.apache.cassandra.dht.Murmur3Partitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) const override;
    virtual token get_token(const sstables::key_view& key) const override;
    virtual bool preserves_order() override { return false; }
    virtual data_type get_token_validator() override;

    virtual unsigned shard_of(const token& t) const override;
    virtual unsigned shard_of(const token& t, unsigned shard_count, unsigned sharding_ignore_msb) const override;
    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans) const override;
    virtual unsigned sharding_ignore_msb() const override;
private:
    using uint128_t = unsigned __int128;
    token get_token(bytes_view key) const;
    token get_token(uint64_t value) const;
    token bias(uint64_t value) const;      // translate from a zero-baed range
    uint64_t unbias(const token& t) const; // translate to a zero-baed range
    static unsigned zero_based_shard_of(uint64_t zero_based_token, unsigned shards, unsigned sharding_ignore_msb_bits);
    static std::vector<uint64_t> init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits);
};


}

