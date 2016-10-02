/*
 * Copyright (C) 2016 ScyllaDB
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

#include "sstables/key.hh"

namespace dht {

class random_partitioner final : public i_partitioner {
public:
    virtual const sstring name() { return "org.apache.cassandra.dht.RandomPartitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) override;
    virtual token get_token(const sstables::key_view& key) override;
    virtual token get_random_token() override;
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) override;
    virtual data_type get_token_validator() override { return varint_type; }
    virtual bytes token_to_bytes(const token& t) const override;
    virtual int tri_compare(const token& t1, const token& t2) override;
    virtual token midpoint(const token& t1, const token& t2) const;
    virtual sstring to_sstring(const dht::token& t) const override;
    virtual dht::token from_sstring(const sstring& t) const override;
    virtual dht::token from_bytes(bytes_view bytes) const override;
    virtual unsigned shard_of(const token& t) const override;
private:
    token get_token(bytes data);
};

}
