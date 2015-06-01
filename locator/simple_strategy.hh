/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "abstract_replication_strategy.hh"

namespace locator {

class simple_strategy : public abstract_replication_strategy {
protected:
    virtual std::vector<inet_address> calculate_natural_endpoints(const token& search_token) override;
public:
    simple_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr&& snitch, std::unordered_map<sstring, sstring>& config_options);
    virtual ~simple_strategy() {};
    size_t get_replication_factor() const;
};

}
