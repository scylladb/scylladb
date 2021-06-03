/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
    virtual void on_join(inet_address endpoint, endpoint_state ep_state) = 0;

    virtual void before_change(inet_address endpoint, endpoint_state current_state, application_state new_statekey, const versioned_value& newvalue) = 0;

    virtual void on_change(inet_address endpoint, application_state state, const versioned_value& value) = 0;

    virtual void on_alive(inet_address endpoint, endpoint_state state) = 0;

    virtual void on_dead(inet_address endpoint, endpoint_state state) = 0;

    virtual void on_remove(inet_address endpoint) = 0;

    /**
     * Called whenever a node is restarted.
     * Note that there is no guarantee when that happens that the node was
     * previously marked down. It will have only if {@code state.isAlive() == false}
     * as {@code state} is from before the restarted node is marked up.
     */
    virtual void on_restart(inet_address endpoint, endpoint_state state) = 0;
};

} // namespace gms
