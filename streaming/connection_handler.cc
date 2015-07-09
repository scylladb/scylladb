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

#include "streaming/messages/stream_init_message.hh"
#include "streaming/connection_handler.hh"
#include "streaming/stream_session.hh"
#include "message/messaging_service.hh"
#include "utils/fb_utilities.hh"

namespace streaming {


connection_handler::connection_handler(std::shared_ptr<stream_session> session)
    : _session(session) {
}

connection_handler::~connection_handler() = default;

future<> connection_handler::initiate() {
    using shard_id = net::messaging_service::shard_id;
    using net::messaging_verb;
#if 0
    logger.debug("[Stream #{}] Sending stream init for incoming stream", session.planId());
    Socket incomingSocket = session.createConnection();
    incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION);
    incoming.sendInitMessage(incomingSocket, true);

    logger.debug("[Stream #{}] Sending stream init for outgoing stream", session.planId());
    Socket outgoingSocket = session.createConnection();
    outgoing.start(outgoingSocket, StreamMessage.CURRENT_VERSION);
    outgoing.sendInitMessage(outgoingSocket, false);
#endif
    auto from = utils::fb_utilities::get_broadcast_address();
    bool is_for_outgoing = true;
    messages::stream_init_message msg(from, _session->session_index(),
            _session->plan_id(), _session->description(),
            is_for_outgoing, _session->keep_ss_table_level());
    auto id = shard_id{_session->peer, 0};
    _session->src_cpu_id = engine().cpu_id();
    return _session->ms().send_message<unsigned>(net::messaging_verb::STREAM_INIT_MESSAGE,
            std::move(id), std::move(msg), _session->src_cpu_id).then([this] (unsigned dst_cpu_id) {
        _session->dst_cpu_id = dst_cpu_id;
    });
}

} // namespace streaming
