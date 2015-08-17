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
#include "streaming/stream_manager.hh"
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
    for (auto it = files.begin(); it != files.end();) {
        auto seq = it->first;
        auto& msg = it->second;
        auto id = shard_id{session->peer, session->dst_cpu_id};
        sslog.debug("stream_transfer_task: Sending outgoing_file_message seq={} msg.detail.cf_id={}", seq, msg.detail.cf_id);
        it++;
        consume(msg.detail.mr, [&msg, this, seq, id] (mutation&& m) {
            msg.mutations_nr++;
            auto fm = make_lw_shared<const frozen_mutation>(m);
            return get_local_stream_manager().mutation_send_limiter().wait().then([&msg, this, fm, seq, id] {
                sslog.debug("SEND STREAM_MUTATION to {}, cf_id={}", id, fm->column_family_id());
                session->ms().send_stream_mutation(id, session->plan_id(), *fm, session->dst_cpu_id).then_wrapped([&msg, this, id, fm] (auto&& f) {
                    try {
                        f.get();
                        sslog.debug("GOT STREAM_MUTATION Reply");
                        msg.mutations_done.signal();
                    } catch (...) {
                        sslog.error("stream_transfer_task: Fail to send STREAM_MUTATION to {}: {}", id, std::current_exception());
                        msg.mutations_done.broken();
                    }
                }).finally([] {
                    get_local_stream_manager().mutation_send_limiter().signal();
                });
                return stop_iteration::no;
            });
        }).then([&msg] {
            return msg.mutations_done.wait(msg.mutations_nr);
        }).then_wrapped([this, seq, id] (auto&& f){
            // TODO: Add retry and timeout logic
            try {
                f.get();
                this->complete(seq);
            } catch (...) {
                sslog.error("stream_transfer_task: Fail to send outgoing_file_message to {}: {}", id, std::current_exception());
                this->session->on_error();
            }
        });
    }
}

void stream_transfer_task::complete(int sequence_number) {
    // ScheduledFuture timeout = timeoutTasks.remove(sequenceNumber);
    // if (timeout != null)
    //     timeout.cancel(false);

    files.erase(sequence_number);

    sslog.debug("stream_transfer_task: complete cf_id = {}, seq = {}", this->cf_id, sequence_number);

    auto signal_complete = files.empty();
    // all file sent, notify session this task is complete.
    if (signal_complete) {
        using shard_id = net::messaging_service::shard_id;
        auto from = utils::fb_utilities::get_broadcast_address();
        auto id = shard_id{session->peer, session->dst_cpu_id};
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION_DONE to {}, seq={}", session->plan_id(), id, sequence_number);
        session->ms().send_stream_mutation_done(id, session->plan_id(), this->cf_id, from, session->connecting, session->dst_cpu_id).then_wrapped([this, id] (auto&& f) {
            try {
                f.get();
                sslog.debug("GOT STREAM_MUTATION_DONE Reply");
                session->transfer_task_completed(this->cf_id);
            } catch (...) {
                sslog.error("stream_transfer_task: Fail to send STREAM_MUTATION_DONE to {}: {}", id, std::current_exception());
                session->on_error();
            }
        });
    }
}

} // namespace streaming
