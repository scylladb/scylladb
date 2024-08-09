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

class load_sketch;

class network_topology_strategy : public abstract_replication_strategy
                                , public tablet_aware_replication_strategy {
public:
    network_topology_strategy(replication_strategy_params params);

    virtual size_t get_replication_factor(const token_metadata&) const override {
        return _rep_factor;
    }

    size_t get_replication_factor(const sstring& dc) const {
        auto dc_factor = _dc_rep_factor.find(dc);
        return (dc_factor == _dc_rep_factor.end()) ? 0 : dc_factor->second;
    }

    const std::vector<sstring>& get_datacenters() const {
        return _datacenteres;
    }

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

public: // tablet_aware_replication_strategy
    virtual effective_replication_map_ptr make_replication_map(table_id, token_metadata_ptr) const override;
    virtual future<tablet_map> allocate_tablets_for_new_table(schema_ptr, token_metadata_ptr, unsigned initial_scale) const override;
    virtual future<tablet_map> reallocate_tablets(schema_ptr, token_metadata_ptr, tablet_map cur_tablets) const override;
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
    future<tablet_replica_set> reallocate_tablets(schema_ptr, token_metadata_ptr, load_sketch&, const tablet_map& cur_tablets, tablet_id tb) const;
    future<tablet_replica_set> add_tablets_in_dc(schema_ptr, token_metadata_ptr, load_sketch&, tablet_id,
            std::map<sstring, std::unordered_set<locator::host_id>>& replicas_per_rack,
            const tablet_replica_set& cur_replicas,
            sstring dc, size_t dc_node_count, size_t dc_rf) const;
    tablet_replica_set drop_tablets_in_dc(schema_ptr, const locator::topology&, load_sketch&, tablet_id,
            const tablet_replica_set& cur_replicas,
            sstring dc, size_t dc_node_count, size_t dc_rf) const;

    // map: data centers -> replication factor
    std::unordered_map<sstring, size_t> _dc_rep_factor;

    std::vector<sstring> _datacenteres;
    size_t _rep_factor;
};
} // namespace locator
