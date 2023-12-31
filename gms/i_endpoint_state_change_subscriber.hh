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

namespace gms {

/**
 * This is called by the gossiper to notify
 * interested parties about changes in the the state associated with any endpoint.
 * For instance if node A figures there is a changes in state for an endpoint B
 * it notifies all interested parties of this change. It is upto to the registered
 * instance to decide what he does with this change. Not all modules maybe interested
 * in all state changes.
 *
 * All notifications that accept a permit_id are guaranteed to be called
 * under the respective endpoint lock.  The permit_id must be provided
 * by the subscriber if it calls back gossiper functions that modify the same endpoint's
 * state, and therefore may acquire the same endpoint_lock - to prevent deadlock on the nested
 * call path.  Calls from other code paths or for other endpoints should pass a
 * null_permit_id to indicate that no endpoint lock is held for them.
 */
class i_endpoint_state_change_subscriber {
protected:
    future<> on_application_state_change(inet_address endpoint, const application_state_map& states, application_state app_state, permit_id,
        std::function<future<>(inet_address, const gms::versioned_value&, gms::permit_id)> func);

public:
    virtual ~i_endpoint_state_change_subscriber() {}
    /**
     * Use to inform interested parties about the change in the state
     * for specified endpoint
     *
     * @param endpoint endpoint for which the state change occurred.
     * @param epState  state that actually changed for the above endpoint.
     */
    virtual future<> on_join(inet_address endpoint, endpoint_state_ptr ep_state, permit_id) = 0;

    virtual future<> on_change(inet_address endpoint, const application_state_map& states, permit_id) = 0;

    virtual future<> on_alive(inet_address endpoint, endpoint_state_ptr state, permit_id) = 0;

    virtual future<> on_dead(inet_address endpoint, endpoint_state_ptr state, permit_id) = 0;

    virtual future<> on_remove(inet_address endpoint, permit_id) = 0;

    /**
     * Called whenever a node is restarted.
     * Note that there is no guarantee when that happens that the node was
     * previously marked down. It will have only if {@code state.isAlive() == false}
     * as {@code state} is from before the restarted node is marked up.
     */
    virtual future<> on_restart(inet_address endpoint, endpoint_state_ptr state, permit_id) = 0;
};

} // namespace gms
