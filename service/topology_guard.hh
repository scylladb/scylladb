/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "service/session.hh"
#include "replica/database_fwd.hh"

namespace service {

/// Represents topology_guard in transit.
/// Can be passed across nodes and copied across shards.
/// Guard in transit doesn't block barriers, but can be materialized into topology_guard
/// which does. If the guard was invalidated while in transit, materialization will fail.
/// Has an null state which represents no guard.
/// topology_guard created out of null frozen_topology_guard is valid and never aborted.
using frozen_topology_guard = session_id;

/// Represents a guard which is always valid and never aborted.
constexpr frozen_topology_guard null_topology_guard = default_session_id;

session_manager& get_topology_session_manager();

/// A guard used by operations started by the topology change coordinator whose scope
/// of operation is supposed to be limited to a single transition stage.
/// The coordinator invalidates guards, typically when it advances to the next stage.
/// Invalidated guards are still alive and block barriers, but their check() will throw, which
/// gives the holder a chance to interrupt the operation and release the guard early.
/// The token metadata barrier executed by the coordinator waits for guards to die, even the
/// invalidated ones.
///
/// Guards allow coordinators to make sure there are no side-effects from operations started
/// in earlier stages of topology change operation. If all side-effects are performed under
/// the guard's scope, then executing a token metadata barrier after invalidating the guard
/// guarantees that there can be no side-effects from earlier stages.
/// The token metadata barrier must contact all nodes which may have alive guards.
template <typename T> concept TopologyGuard = requires(T guard, frozen_topology_guard frozen_guard) {
    // Acquiring the guard.
    { T(frozen_guard) } -> std::same_as<T>;

    // Moving the guard.
    { T(std::move(guard)) } -> std::same_as<T>;

    // Checking if the guard was invalidated.
    { guard.check() } -> std::same_as<void>;
};

/// TopologyGuard implementation where the scope of the guard's validity
/// is limited to the scope of a session validity.
/// The topology change coordinator invalidates the guards by closing the session.
class session_topology_guard {
    session::guard _guard;
public:
    using frozen = session_id;

    session_topology_guard(frozen_topology_guard g)
        : _guard(get_topology_session_manager().enter_session(g))
    { }

    void check() {
        return _guard.check();
    }
};

static_assert(TopologyGuard<session_topology_guard>);

using topology_guard = session_topology_guard;

} // namespace service
