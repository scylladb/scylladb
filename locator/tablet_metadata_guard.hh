/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include "locator/tablets.hh"
#include "locator/abstract_replication_strategy.hh"

namespace locator {

/// A holder for table's effective_replication_map which
/// automatically switches to the latest one as long as it
/// does not affect the tablet associated with this guard.
///
/// This is useful for tracking long-running operations in
/// the context of a single tablet which we don't want to
/// block topology barriers for other tablets, only barriers for
/// this tablet.
class tablet_metadata_guard {
    lw_shared_ptr<replica::table> _table;
    global_tablet_id _tablet;
    effective_replication_map_ptr _erm;
    std::optional<tablet_transition_stage> _stage;
    seastar::abort_source _abort_source;
    optimized_optional<seastar::abort_source::subscription> _callback;
private:
    void subscribe();
    void check() noexcept;
public:
    tablet_metadata_guard(replica::table& table, global_tablet_id tablet);

    /// Returns an abort_source which is signaled when effective_replication_map changes
    /// in a way which is relevant for the tablet associated with this guard.
    /// When this happens, the guard stops refreshing the effective_replication_map,
    /// which will block topology coordinator barriers until the guard is destroyed.
    ///
    /// The abort_source is valid as long as this instance is alive.
    seastar::abort_source& get_abort_source() {
        return _abort_source;
    }

    locator::token_metadata_ptr get_token_metadata() {
        return _erm->get_token_metadata_ptr();
    }

    /// Returns tablet_map for the table of the tablet associated with this guard.
    /// The result is valid until the next deferring point.
    const locator::tablet_map& get_tablet_map() {
        return get_token_metadata()->tablets().get_tablet_map(_tablet.table);
    }
};

} // namespace locator
