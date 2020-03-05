/*
 * Copyright (C) 2020 ScyllaDB
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

#include <boost/range/adaptor/map.hpp>
#include "dht/token.hh"
#include "dht/i_partitioner.hh"

// Shards tokens such that tokens are owned by shards in a round-robin manner.
class dummy_partitioner : public dht::i_partitioner {
    const dht::i_partitioner& _partitioner;
    std::vector<dht::token> _tokens;

public:
    // We need a container input that enforces token order by design.
    // In addition client code will often map tokens to something, e.g. mutation
    // they originate from or shards, etc. So, for convenience we allow any
    // ordered associative container (std::map) that has dht::token as keys.
    // Values will be ignored.
    template <typename T>
    dummy_partitioner(const dht::i_partitioner& partitioner, const std::map<dht::token, T>& something_by_token)
        : i_partitioner(smp::count)
        , _partitioner(partitioner)
        , _tokens(boost::copy_range<std::vector<dht::token>>(something_by_token | boost::adaptors::map_keys)) {
    }

    virtual dht::token get_token(const schema& s, partition_key_view key) const override { return _partitioner.get_token(s, key); }
    virtual dht::token get_token(const sstables::key_view& key) const override { return _partitioner.get_token(key); }
    virtual bool preserves_order() const override { return _partitioner.preserves_order(); }
    virtual const sstring name() const override { return _partitioner.name(); }
    virtual unsigned shard_of(const dht::token& t) const override;
    virtual dht::token token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans = 1) const override;
};

