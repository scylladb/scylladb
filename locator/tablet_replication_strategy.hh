/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"

#include <seastar/core/sstring.hh>

namespace locator {

/// Trait class which allows replication strategies to work in a mode which
/// uses tablet-based replication.
///
/// Contains common logic, like parsing tablet options,
/// and creating effective_replication_map for a given table which works with
/// system's tablet_metadata.
class tablet_aware_replication_strategy : public per_table_replication_strategy {
private:
    size_t _initial_tablets = 0;
    db::tablet_options _tablet_options;
protected:
    void validate_tablet_options(const abstract_replication_strategy&, const gms::feature_service&, const replication_strategy_config_options&) const;
    void process_tablet_options(abstract_replication_strategy&, replication_strategy_config_options&, replication_strategy_params);
    effective_replication_map_ptr do_make_replication_map(table_id,
                                                          replication_strategy_ptr,
                                                          token_metadata_ptr,
                                                          size_t replication_factor) const;

public:
    size_t get_initial_tablets() const { return _initial_tablets; }

    /// Generates tablet_map for a new table.
    /// Runs under group0 guard.
    virtual future<tablet_map> allocate_tablets_for_new_table(schema_ptr, token_metadata_ptr, size_t tablet_count) const = 0;

    /// Generates tablet_map for a new table or when increasing replication factor.
    /// For a new table, cur_tablets is initialized with the tablet_count,
    /// otherwise, cur_tablets is a copy of the current tablet_map.
    /// Runs under group0 guard.
    virtual future<tablet_map> reallocate_tablets(schema_ptr, token_metadata_ptr, tablet_map cur_tablets) const = 0;

    /// Returns replication factor in a given DC.
    /// Note that individual tablets may lag behind desired replication factor in their
    /// current replica list, as replication factor changes involve table rebuilding transitions
    /// which are not instantaneous.
    virtual size_t get_replication_factor(const sstring& dc) const = 0;
};

} // namespace locator
