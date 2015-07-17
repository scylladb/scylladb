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

#include "log.hh"
#include "streaming/stream_detail.hh"
#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_session.hh"
#include "streaming/messages/outgoing_file_message.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"
#include "mutation.hh"
#include "message/messaging_service.hh"

namespace streaming {

extern logging::logger sslog;

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session, UUID cf_id)
    : stream_task(session, cf_id) {
}

stream_transfer_task::~stream_transfer_task() = default;

void stream_transfer_task::add_transfer_file(stream_detail detail) {
    assert(cf_id == detail.cf_id);
    auto message = messages::outgoing_file_message(sequence_number++, std::move(detail), session->keep_ss_table_level());
    auto size = message.header.size();
    auto seq = message.header.sequence_number;
    files.emplace(seq, std::move(message));
    total_size += size;
}

void stream_transfer_task::start() {
    using shard_id = net::messaging_service::shard_id;
    using net::messaging_verb;
    sslog.debug("stream_transfer_task: {} outgoing_file_message to send", files.size());
    for (auto& x : files) {
        auto& seq = x.first;
        auto& msg = x.second;
        auto id = shard_id{session->peer, session->dst_cpu_id};
        sslog.debug("stream_transfer_task: Sending outgoing_file_message seq={} msg.detail.cf_id={}", seq, msg.detail.cf_id);
        do_with(std::move(id), [this, seq, &msg] (shard_id& id) {
            return consume(msg.detail.mr, [this, seq, &id, &msg] (mutation&& m) {
                auto fm = make_lw_shared<const frozen_mutation>(m);
                sslog.debug("SEND STREAM_MUTATION to {}, cf_id={}", id, fm->column_family_id());
                return session->ms().send_stream_mutation(id, *fm, session->dst_cpu_id).then([this, fm] {
                    sslog.debug("GOT STREAM_MUTATION Reply");
                    return stop_iteration::no;
                });
            });
        }).then([this, seq] {
           this->complete(seq);
        });
    }
}

} // namespace streaming
