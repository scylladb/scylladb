/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    virtual void validate_options(const gms::feature_service&) const override;
    virtual std::optional<std::unordered_set<sstring>> recognized_options(const topology&) const override;
    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

    virtual future<host_id_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;
private:
    size_t _replication_factor = 1;
};

}
