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

#pragma once

#include "utils/UUID.hh"
#include "streaming/session_info.hh"
#include <vector>

namespace streaming {

/**
 * Current snapshot of streaming progress.
 */
class stream_state {
public:
    using UUID = utils::UUID;
    UUID plan_id;
    sstring description;
    std::vector<session_info> sessions;

    stream_state(UUID plan_id_, sstring description_, std::vector<session_info> sessions_)
        : plan_id(std::move(plan_id_))
        , description(std::move(description_))
        , sessions(std::move(sessions_)) {
    }

    bool has_failed_session() {
        for (auto& x : sessions) {
            if (x.is_failed()) {
                return true;
            }
        }
        return false;
    }
};

} // namespace streaming
