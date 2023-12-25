/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include <optional>

namespace locator {
class everywhere_replication_strategy : public abstract_replication_strategy {
public:
    everywhere_replication_strategy(replication_strategy_params params);

    virtual future<host_id_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options(const gms::feature_service&) const override { /* noop */ }

    std::optional<std::unordered_set<sstring>> recognized_options(const topology&) const override {
        // We explicitly allow all options
        return std::nullopt;
    }

    virtual size_t get_replication_factor(const token_metadata& tm) const override;

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }
};
}
