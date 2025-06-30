/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "replica/database_fwd.hh"
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
    ~tablet_metadata_guard();

    tablet_metadata_guard(tablet_metadata_guard&&) = delete;
    tablet_metadata_guard(const tablet_metadata_guard&) = delete;

    tablet_metadata_guard& operator=(tablet_metadata_guard&&) = delete;
    tablet_metadata_guard& operator=(const tablet_metadata_guard&) = delete;

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

    const effective_replication_map_ptr& get_erm() const {
        return _erm;
    }

    /// Returns tablet_map for the table of the tablet associated with this guard.
    /// The result is valid until the next deferring point.
    const locator::tablet_map& get_tablet_map() {
        return get_token_metadata()->tablets().get_tablet_map(_tablet.table);
    }
};

// A topology guard for a single token.
//
// Data-plane requests typically hold a strong pointer to the
// effective_replication_map (ERM) to guard against tablet migrations and
// other topology changes. This works because topology coordinator steps
// use global barriers that install a new token_metadata version on each shard
// and wait for all references to the old one to be dropped. Since ERM holds
// a strong pointer to token_metadata, it blocks these operations until released.
//
// When operating on a single token, it is sufficient to block topology changes
// only for the relevant tablet. The tablet_metadata_guard class implements
// this behavior for tablet-aware tables by updating the ERM pointer automatically,
// unless the change affects the guarded tablet.
//
// The token_metadata_guard wraps tablet_metadata_guard for tablet-aware tables,
// and falls back to a plain strong ERM pointer for vnode-based (non-tablet-aware) tables.
//
// Unlike tablet_metadata_guard, token_metadata_guard is copyable and movable, since
// it contains only pointers.
class token_metadata_guard {
    // Holds tablet_metadata_guard for tablet-aware tables and erm otherwise.
    using guard_type = std::variant<lw_shared_ptr<tablet_metadata_guard>, effective_replication_map_ptr>;
    guard_type _guard;
public:
    token_metadata_guard(replica::table& table, dht::token token);

    const effective_replication_map_ptr& get_erm() const;
};

} // namespace locator
