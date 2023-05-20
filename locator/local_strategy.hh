/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "abstract_replication_strategy.hh"

#include <optional>
#include <set>

// forward declaration since replica/database.hh includes this file
class keyspace;

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

class local_strategy : public abstract_replication_strategy {
public:
    local_strategy(const replication_strategy_config_options& config_options);
    virtual ~local_strategy() {};
    virtual size_t get_replication_factor(const token_metadata&) const override;

    virtual future<endpoint_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options(const gms::feature_service&) const override;

    virtual std::optional<std::unordered_set<sstring>> recognized_options(const topology&) const override;

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return false;
    }

    /**
     * We need to override this because the default implementation depends
     * on token calculations but LocalStrategy may be used before tokens are set up.
     */
    inet_address_vector_replica_set get_natural_endpoints(const token&, const vnode_effective_replication_map&) const override;
    virtual stop_iteration for_each_natural_endpoint_until(const token&, const vnode_effective_replication_map&, const noncopyable_function<stop_iteration(const inet_address&)>& func) const override;
};

}
