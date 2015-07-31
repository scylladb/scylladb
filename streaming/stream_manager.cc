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

#include "core/distributed.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_result_future.hh"

namespace streaming {

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

shared_ptr<stream_result_future> stream_manager::get_receiving_stream(UUID plan_id) {
    auto it = _receiving_streams.find(plan_id);
    if (it != _receiving_streams.end()) {
        return it->second;
    }
    return {};
}

} // namespace streaming
