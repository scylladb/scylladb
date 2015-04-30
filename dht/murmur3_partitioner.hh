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
    virtual token get_token(const schema& s, const partition_key& key);
    virtual token get_token(const sstables::key_view& key);
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens);
    virtual data_type get_token_validator();
    virtual bool is_equal(const token& t1, const token& t2);
    virtual bool is_less(const token& t1, const token& t2);
private:
    static int64_t normalize(int64_t in);
    token get_token(bytes_view key);
    token get_token(uint64_t value) const;
};


}

