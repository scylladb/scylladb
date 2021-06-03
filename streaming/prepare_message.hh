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

#include "streaming/stream_request.hh"
#include "streaming/stream_summary.hh"

namespace streaming {

class prepare_message {
public:
    /**
     * Streaming requests
     */
    std::vector<stream_request> requests;

    /**
     * Summaries of streaming out
     */
    std::vector<stream_summary> summaries;

    uint32_t dst_cpu_id;

    prepare_message() = default;
    prepare_message(std::vector<stream_request> reqs, std::vector<stream_summary> sums, uint32_t dst_cpu_id_ = -1)
        : requests(std::move(reqs))
        , summaries(std::move(sums))
        , dst_cpu_id(dst_cpu_id_) {
    }
};

} // namespace streaming
