/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "abstract_replication_strategy.hh"

#include <optional>

// forward declaration since replica/database.hh includes this file
class keyspace;

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

class local_strategy : public abstract_replication_strategy {
public:
    local_strategy(replication_strategy_params params);
    virtual ~local_strategy() {};
    virtual size_t get_replication_factor(const token_metadata&) const override;

    virtual future<host_id_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options(const gms::feature_service&, const locator::topology&) const override;

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return false;
    }

    [[nodiscard]] sstring sanity_check_read_replicas(const effective_replication_map& erm, const host_id_vector_replica_set& read_replicas) const override;
};

}
