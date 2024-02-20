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
#include "locator/tablet_replication_strategy.hh"

#include <optional>
#include <unordered_set>

namespace locator {

struct network_topology_strategy_traits : public abstract_replication_strategy_traits {
    network_topology_strategy_traits(const replication_strategy_params&);
};

class network_topology_strategy : public abstract_replication_strategy
                                , public tablet_aware_replication_strategy {
public:
    using dc_rep_factor = std::unordered_map<const datacenter*, size_t>;

    network_topology_strategy(const topology&, replication_strategy_params params);

    virtual size_t get_replication_factor(const token_metadata&) const override {
        return _rep_factor;
    }

    const dc_rep_factor& get_dc_rep_factor() const noexcept {
        return _dc_rep_factor;
    }

    size_t get_replication_factor(const datacenter* dc) const noexcept {
        auto dc_factor = _dc_rep_factor.find(dc);
        return (dc_factor == _dc_rep_factor.end()) ? 0 : dc_factor->second;
    }

    size_t get_replication_factor(const sstring& dc_name) const;

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

public: // tablet_aware_replication_strategy
    virtual effective_replication_map_ptr make_replication_map(table_id, token_metadata_ptr) const override;
    virtual future<tablet_map> allocate_tablets_for_new_table(schema_ptr, token_metadata_ptr, unsigned initial_scale) const override;
protected:
    /**
     * calculate endpoints in one pass through the tokens by tracking our
     * progress in each DC, rack etc.
     */
    virtual future<host_id_set> calculate_natural_endpoints(
        const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options(const gms::feature_service&) const override;

    virtual std::optional<std::unordered_set<sstring>> recognized_options(const topology&) const override;

private:
    const topology_registry& _topology_registry;
    // map: data centers -> replication factor
    dc_rep_factor _dc_rep_factor;

    size_t _rep_factor;
};
} // namespace locator
