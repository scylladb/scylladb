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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#include "streaming/stream_coordinator.hh"

namespace streaming {

bool stream_coordinator::has_active_sessions() {
    for (auto& x : _peer_sessions) {
        if (x.second.has_active_sessions()) {
            return true;
        }
    }
    return false;
}

bool stream_coordinator::host_streaming_data::has_active_sessions() {
    for (auto& x : _stream_sessions) {
        auto state = x.second.get_state();
        if (state != stream_session::state::COMPLETE && state != stream_session::state::FAILED) {
            return true;
        }
    }
    return false;
}

} // namespace streaming
