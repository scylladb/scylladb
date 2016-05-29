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
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <vector>
#include <atomic>
#include <seastar/core/sstring.hh>
#include "gc_clock.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"

namespace tracing {
struct i_tracing_backend_helper {
    virtual ~i_tracing_backend_helper() {}
    virtual future<> start() = 0;
    virtual future<> stop() = 0;

    /**
     * Store a new tracing session record
     *
     * @param session_id tracing session ID
     * @param client client IP
     * @param parameters optional parameters
     * @param request request we are tracing
     * @param started_at amount of milliseconds passed since Epoch before this
     *                   session is started (on a Coordinator Node)
     * @param command a type of this trace
     * @param elapsed number of microseconds this tracing session took
     * @param ttl TTL of a session record
     */
    virtual void store_session_record(const utils::UUID& session_id,
                                      gms::inet_address client,
                                      std::unordered_map<sstring, sstring> parameters,
                                      sstring request,
                                      long started_at,
                                      trace_type command,
                                      int elapsed,
                                      gc_clock::duration ttl) = 0;

    /**
     * Store a new tracing event record
     * @param session_id tracing session ID
     * @param message tracing message
     * @param elapsed number of microseconds passed since a beginning of a
     *                corresponding tracing session till this event
     * @param ttl TTL of the event record
     */
    virtual void store_event_record(const utils::UUID& session_id,
                                    sstring message,
                                    int elapsed,
                                    gc_clock::duration ttl) = 0;

    virtual void flush() = 0;
};
}
