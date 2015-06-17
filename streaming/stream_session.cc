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
#include "streaming/messages/outgoing_file_message.hh"
#include "streaming/messages/received_message.hh"
#include "streaming/messages/retry_message.hh"
#include "streaming/messages/complete_message.hh"
#include "streaming/messages/session_failed_message.hh"

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
    ms().register_handler(messaging_verb::OUTGOING_FILE_MESSAGE, [] (messages::outgoing_file_message msg) {
        auto cpu_id = 0;
        return smp::submit_to(cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
            messages::received_message msg_ret;
            return make_ready_future<messages::received_message>(std::move(msg_ret));
        });
    });
    ms().register_handler(messaging_verb::RETRY_MESSAGE, [] (messages::retry_message msg) {
        auto cpu_id = 0;
        return smp::submit_to(cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
            messages::outgoing_file_message msg_ret;
            return make_ready_future<messages::outgoing_file_message>(std::move(msg_ret));
        });
    });
    ms().register_handler(messaging_verb::COMPLETE_MESSAGE, [] (messages::complete_message msg) {
        auto cpu_id = 0;
        return smp::submit_to(cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
            messages::complete_message msg_ret;
            return make_ready_future<messages::complete_message>(std::move(msg_ret));
        });
    });
    ms().register_handler(messaging_verb::SESSION_FAILED_MESSAGE, [] (messages::session_failed_message msg) {
        auto cpu_id = 0;
        smp::submit_to(cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
        }).then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                print("stream_session: SESSION_FAILED_MESSAGE error\n");
            }
        });
        return messaging_service::no_wait();
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
