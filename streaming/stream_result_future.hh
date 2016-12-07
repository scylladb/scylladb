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

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "streaming/stream_coordinator.hh"
#include "streaming/stream_event_handler.hh"
#include "streaming/stream_state.hh"
#include "streaming/progress_info.hh"
#include <vector>

namespace streaming {
    using UUID = utils::UUID;
    using inet_address = gms::inet_address;
/**
 * A future on the result ({@link StreamState}) of a streaming plan.
 *
 * In practice, this object also groups all the {@link StreamSession} for the streaming job
 * involved. One StreamSession will be created for every peer involved and said session will
 * handle every streaming (outgoing and incoming) to that peer for this job.
 * <p>
 * The future will return a result once every session is completed (successfully or not). If
 * any session ended up with an error, the future will throw a StreamException.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to
 * track progress of the streaming.
 */
class stream_result_future {
public:
    using UUID = utils::UUID;
    UUID plan_id;
    sstring description;
private:
    shared_ptr<stream_coordinator> _coordinator;
    std::vector<stream_event_handler*> _event_listeners;
    promise<stream_state> _done;
    lowres_clock::time_point _start_time;
public:
    stream_result_future(UUID plan_id_, sstring description_, bool is_receiving)
        : stream_result_future(plan_id_, description_, make_shared<stream_coordinator>(is_receiving)) {
        // Note: Origin sets connections_per_host = 0 on receiving side, We set 1 to
        // refelct the fact that we actaully create one conncetion to the initiator.
    }

    /**
     * Create new StreamResult of given {@code planId} and type.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param description Stream description
     */
    stream_result_future(UUID plan_id_, sstring description_, shared_ptr<stream_coordinator> coordinator_)
        : plan_id(std::move(plan_id_))
        , description(std::move(description_))
        , _coordinator(coordinator_)
        , _start_time(lowres_clock::now()) {
        // if there is no session to listen to, we immediately set result for returning
        if (!_coordinator->is_receiving() && !_coordinator->has_active_sessions()) {
            _done.set_value(get_current_state());
        }
    }

public:
    shared_ptr<stream_coordinator> get_coordinator() { return _coordinator; };

public:
    static future<stream_state> init_sending_side(UUID plan_id_, sstring description_, std::vector<stream_event_handler*> listeners_, shared_ptr<stream_coordinator> coordinator_);
    static shared_ptr<stream_result_future> init_receiving_side(UUID plan_id, sstring description, inet_address from);

public:
    void add_event_listener(stream_event_handler* listener) {
        // FIXME: Futures.addCallback(this, listener);
        _event_listeners.push_back(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    stream_state get_current_state();

    void handle_session_prepared(shared_ptr<stream_session> session);


    void handle_session_complete(shared_ptr<stream_session> session);

    void handle_progress(progress_info progress);

    template <typename Event>
    void fire_stream_event(Event event);

private:
    void maybe_complete();
};

} // namespace streaming
