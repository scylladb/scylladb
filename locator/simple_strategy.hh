/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "abstract_replication_strategy.hh"

#include <optional>

namespace locator {

class simple_strategy : public abstract_replication_strategy {
public:
    simple_strategy(replication_strategy_params params);
    virtual ~simple_strategy() {};
    virtual size_t get_replication_factor(const token_metadata& tm) const override;
    virtual void validate_options(const gms::feature_service&, const locator::topology& topology) const override;
    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

    virtual future<host_id_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;

    [[nodiscard]] sstring sanity_check_read_replicas(const effective_replication_map& erm, const host_id_vector_replica_set& read_replicas) const override;
private:
    size_t _replication_factor = 1;
};

}
