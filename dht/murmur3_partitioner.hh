/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "i_partitioner.hh"

namespace dht {

class murmur3_partitioner final : public i_partitioner {
public:
    virtual token get_token(const bytes& key) override;
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens);
    virtual data_type get_token_validator();
private:
    static int64_t normalize(int64_t in);
};


}

