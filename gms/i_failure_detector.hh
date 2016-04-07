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
 * Copyright (C) 2015 ScyllaDB
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
#include "gms/i_failure_detection_event_listener.hh"

namespace gms {

/**
 * An interface that provides an application with the ability
 * to query liveness information of a node in the cluster. It
 * also exposes methods which help an application register callbacks
 * for notifications of liveness information of nodes.
 */

class i_failure_detector {
public:
    virtual ~i_failure_detector() {}
    /**
     * Failure Detector's knowledge of whether a node is up or
     * down.
     *
     * @param ep endpoint in question.
     * @return true if UP and false if DOWN.
     */
    virtual bool is_alive(inet_address ep) = 0;

    /**
     * This method is invoked by any entity wanting to interrogate the status of an endpoint.
     * In our case it would be the Gossiper. The Failure Detector will then calculate Phi and
     * deem an endpoint as suspicious or alive as explained in the Hayashibara paper.
     * <p/>
     * param ep endpoint for which we interpret the inter arrival times.
     */
    virtual void interpret(inet_address ep) = 0;

    /**
     * This method is invoked by the receiver of the heartbeat. In our case it would be
     * the Gossiper. Gossiper inform the Failure Detector on receipt of a heartbeat. The
     * FailureDetector will then sample the arrival time as explained in the paper.
     * <p/>
     * param ep endpoint being reported.
     */
    virtual void report(inet_address ep) = 0;

    /**
     * remove endpoint from failure detector
     */
    virtual void remove(inet_address ep) = 0;

    /**
     * force conviction of endpoint in the failure detector
     */
    virtual void force_conviction(inet_address ep) = 0;

    /**
     * Register interest for Failure Detector events.
     *
     * @param listener implementation of an application provided IFailureDetectionEventListener
     */
    virtual void register_failure_detection_event_listener(i_failure_detection_event_listener* listener) = 0;

    /**
     * Un-register interest for Failure Detector events.
     *
     * @param listener implementation of an application provided IFailureDetectionEventListener
     */
    virtual void unregister_failure_detection_event_listener(i_failure_detection_event_listener* listener) = 0;
};

} // namespace gms
