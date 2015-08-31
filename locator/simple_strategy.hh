/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "abstract_replication_strategy.hh"

#include <experimental/optional>
#include <set>

namespace locator {

class simple_strategy : public abstract_replication_strategy {
protected:
    virtual std::vector<inet_address> calculate_natural_endpoints(const token& search_token) const override;
public:
    simple_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options);
    virtual ~simple_strategy() {};
    virtual size_t get_replication_factor() const override;
    virtual void validate_options() const override;
    virtual std::experimental::optional<std::set<sstring>> recognized_options() const override;
private:
    size_t _replication_factor = 1;
};

}
