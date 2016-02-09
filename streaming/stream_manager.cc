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

#include "core/distributed.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_result_future.hh"
#include "log.hh"

namespace streaming {

extern logging::logger sslog;

distributed<stream_manager> _the_stream_manager;

void stream_manager::register_sending(shared_ptr<stream_result_future> result) {
#if 0
    result.addEventListener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            initiatedStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _initiated_streams[result->plan_id] = std::move(result);
}

void stream_manager::register_receiving(shared_ptr<stream_result_future> result) {
#if 0
    result->add_event_listener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            receivingStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _receiving_streams[result->plan_id] = std::move(result);
}

shared_ptr<stream_result_future> stream_manager::get_sending_stream(UUID plan_id) {
    auto it = _initiated_streams.find(plan_id);
    if (it != _initiated_streams.end()) {
        return it->second;
    }
    return {};
}

shared_ptr<stream_result_future> stream_manager::get_receiving_stream(UUID plan_id) {
    auto it = _receiving_streams.find(plan_id);
    if (it != _receiving_streams.end()) {
        return it->second;
    }
    return {};
}

void stream_manager::remove_stream(UUID plan_id) {
    sslog.debug("stream_manager: removing plan_id={}", plan_id);
    _initiated_streams.erase(plan_id);
    _receiving_streams.erase(plan_id);
}

void stream_manager::show_streams() {
    for (auto& x : _initiated_streams) {
        sslog.debug("stream_manager:initiated_stream: plan_id={}", x.first);
    }
    for (auto& x : _receiving_streams) {
        sslog.debug("stream_manager:receiving_stream: plan_id={}", x.first);
    }
}

std::vector<shared_ptr<stream_result_future>> stream_manager::get_all_streams() const {
    std::vector<shared_ptr<stream_result_future>> result;
    for (auto& x : _initiated_streams) {
        result.push_back(x.second);
    }
    for (auto& x : _receiving_streams) {
        result.push_back(x.second);
    }
    return result;
}

} // namespace streaming
