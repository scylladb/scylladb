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
 * This is called by an instance of the IEndpointStateChangePublisher to notify
 * interested parties about changes in the the state associated with any endpoint.
 * For instance if node A figures there is a changes in state for an endpoint B
 * it notifies all interested parties of this change. It is upto to the registered
 * instance to decide what he does with this change. Not all modules maybe interested
 * in all state changes.
 */
class i_endpoint_state_change_subscriber {
public:
    virtual ~i_endpoint_state_change_subscriber() {}
    /**
     * Use to inform interested parties about the change in the state
     * for specified endpoint
     *
     * @param endpoint endpoint for which the state change occurred.
     * @param epState  state that actually changed for the above endpoint.
     */
    virtual future<> on_join(inet_address endpoint, endpoint_state ep_state) = 0;

    virtual future<> before_change(inet_address endpoint, endpoint_state current_state, application_state new_statekey, const versioned_value& newvalue) = 0;

    virtual future<> on_change(inet_address endpoint, application_state state, const versioned_value& value) = 0;

    virtual future<> on_alive(inet_address endpoint, endpoint_state state) = 0;

    virtual future<> on_dead(inet_address endpoint, endpoint_state state) = 0;

    virtual future<> on_remove(inet_address endpoint) = 0;

    /**
     * Called whenever a node is restarted.
     * Note that there is no guarantee when that happens that the node was
     * previously marked down. It will have only if {@code state.isAlive() == false}
     * as {@code state} is from before the restarted node is marked up.
     */
    virtual future<> on_restart(inet_address endpoint, endpoint_state state) = 0;
};

} // namespace gms
