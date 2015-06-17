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

#include "message/messaging_service.hh"
#include "streaming/stream_session.hh"
#include "streaming/messages/stream_init_message.hh"
#include "streaming/messages/prepare_message.hh"

namespace streaming {

void stream_session::init_messaging_service_handler() {
    ms().register_handler(messaging_verb::STREAM_INIT_MESSAGE, [] (messages::stream_init_message msg) {
        auto cpu_id = 0;
        return smp::submit_to(cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
            return make_ready_future<>();
        });
    });
    ms().register_handler(messaging_verb::PREPARE_MESSAGE, [] (messages::prepare_message msg) {
        auto cpu_id = 0;
        return smp::submit_to(cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
            messages::prepare_message msg_ret;
            return make_ready_future<messages::prepare_message>(std::move(msg_ret));
        });
    });
}

future<> stream_session::start() {
    return _handlers.start().then([this] {
        return _handlers.invoke_on_all([this] (handler& h) {
            this->init_messaging_service_handler();
        });
    });
}

} // namespace streaming
