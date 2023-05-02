/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "gms/versioned_value.hh"
#include "gms/endpoint_id.hh"

namespace gms {

/**
 * This is called by the gossiper to notify
 * interested parties about changes in the the state associated with any endpoint.
 * For instance if node A figures there is a changes in state for an endpoint B
 * it notifies all interested parties of this change. It is upto to the registered
 * instance to decide what he does with this change. Not all modules maybe interested
 * in all state changes.
 *
 * All notificaions that accept a permit_id are guaranteed to be called
 * under the respective endpoint lock.  The permit_id must be provided
 * by the subscriber if it calls back gossiper functions that modify the same endpoint's
 * state, and therefore may acquire the same endpoint_lock - to prevent deadlock on the nested
 * call path.  Calls from other code paths or for other endpoints should pass a
 * null_permit_id to indicate that no endpoint lock is held for them.
 */
class i_endpoint_state_change_subscriber {
public:
    virtual ~i_endpoint_state_change_subscriber() {}
    /**
     * Use to inform interested parties about the change in the state
     * for specified endpoint
     *
     * @param node endpoint for which the state change occurred.
     * @param epState  state that actually changed for the above endpoint.
     */
    virtual future<> on_join(endpoint_id node, endpoint_state_ptr ep_state, permit_id) = 0;

    virtual future<> before_change(endpoint_id node, endpoint_state_ptr current_state, application_state new_statekey, const versioned_value& newvalue) = 0;

    virtual future<> on_change(endpoint_id node, application_state state, const versioned_value& value, permit_id) = 0;

    virtual future<> on_alive(endpoint_id node, endpoint_state_ptr state, permit_id) = 0;

    virtual future<> on_dead(endpoint_id node, endpoint_state_ptr state, permit_id) = 0;

    virtual future<> on_remove(endpoint_id node, permit_id) = 0;

    /**
     * Called whenever a node is restarted.
     * Note that there is no guarantee when that happens that the node was
     * previously marked down. It will have only if {@code state.isAlive() == false}
     * as {@code state} is from before the restarted node is marked up.
     */
    virtual future<> on_restart(endpoint_id node, endpoint_state_ptr state, permit_id) = 0;
};

} // namespace gms
