/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "i_partitioner.hh"
#include "bytes.hh"

namespace dht {

class murmur3_partitioner final : public i_partitioner {
public:
    virtual const sstring name() { return "org.apache.cassandra.dht.Murmur3Partitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) override;
    virtual token get_token(const sstables::key_view& key) override;
    virtual token get_random_token() override;
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) override;
    virtual data_type get_token_validator() override;
    virtual int tri_compare(const token& t1, const token& t2) override;
    virtual token midpoint(const token& t1, const token& t2) const override;
    virtual sstring to_sstring(const dht::token& t) const override;
    virtual dht::token from_sstring(const sstring& t) const override;
    virtual unsigned shard_of(const token& t) const override;
private:
    static int64_t normalize(int64_t in);
    token get_token(bytes_view key);
    token get_token(uint64_t value) const;
};


}

