/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "i_partitioner.hh"
#include "bytes.hh"
#include "sstables/key.hh"

namespace dht {

class murmur3_partitioner final : public i_partitioner {
public:
    virtual const bytes name() { return "org.apache.cassandra.dht.Murmur3Partitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) override;
    virtual token get_token(const sstables::key_view& key) override;
    virtual token get_random_token() override;
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) override;
    virtual data_type get_token_validator() override;
    virtual bool is_equal(const token& t1, const token& t2) override;
    virtual bool is_less(const token& t1, const token& t2) override;
private:
    static int64_t normalize(int64_t in);
    token get_token(bytes_view key);
    token get_token(uint64_t value) const;
};


}

