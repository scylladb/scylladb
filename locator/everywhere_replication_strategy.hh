/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include <optional>

namespace locator {
class everywhere_replication_strategy : public abstract_replication_strategy {
public:
    everywhere_replication_strategy(replication_strategy_params params, const locator::topology*);

    virtual future<host_id_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options(const gms::feature_service&, const locator::topology&) const override;

    virtual size_t get_replication_factor(const token_metadata& tm) const override;

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }
};
}
