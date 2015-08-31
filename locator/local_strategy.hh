/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "abstract_replication_strategy.hh"

#include <experimental/optional>
#include <set>

// forward declaration since database.hh includes this file
class keyspace;

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

class local_strategy : public abstract_replication_strategy {
protected:
    virtual std::vector<inet_address> calculate_natural_endpoints(const token& search_token) const override;
public:
    local_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options);
    virtual ~local_strategy() {};
    virtual size_t get_replication_factor() const;
    /**
     * We need to override this even if we override calculateNaturalEndpoints,
     * because the default implementation depends on token calculations but
     * LocalStrategy may be used before tokens are set up.
     */
    std::vector<inet_address> get_natural_endpoints(const token& search_token) override;

    virtual void validate_options() const override;

    virtual std::experimental::optional<std::set<sstring>> recognized_options() const override;
};

}
