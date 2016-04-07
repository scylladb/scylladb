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
