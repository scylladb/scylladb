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

#include "log.hh"
#include "streaming/stream_detail.hh"
#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_session.hh"
#include "streaming/stream_manager.hh"
#include "streaming/outgoing_file_message.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"
#include "mutation.hh"
#include "message/messaging_service.hh"
#include "range.hh"
#include "dht/i_partitioner.hh"

namespace streaming {

extern logging::logger sslog;

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session, UUID cf_id, std::vector<range<dht::token>> ranges)
    : stream_task(session, cf_id)
    , _ranges(std::move(ranges)) {
}

stream_transfer_task::~stream_transfer_task() = default;

void stream_transfer_task::add_transfer_file(stream_detail detail) {
    assert(cf_id == detail.cf_id);
    auto message = outgoing_file_message(sequence_number++, std::move(detail));
    auto seq = message.sequence_number;
    files.emplace(seq, std::move(message));
}

void stream_transfer_task::start() {
    using msg_addr = net::messaging_service::msg_addr;
    using net::messaging_verb;
    auto plan_id = session->plan_id();
    sslog.debug("[Stream #{}] stream_transfer_task: {} outgoing_file_message to send", plan_id, files.size());
    for (auto it = files.begin(); it != files.end();) {
        auto seq = it->first;
        auto& msg = it->second;
        auto id = msg_addr{session->peer, session->dst_cpu_id};
        sslog.debug("[Stream #{}] stream_transfer_task: Sending outgoing_file_message seq={} msg.detail.cf_id={}", plan_id, seq, msg.detail.cf_id);
        it++;
        consume(*msg.detail.mr, [&msg, this, seq, id, plan_id] (mutation&& m) {
            msg.mutations_nr++;
            auto fm = make_lw_shared<const frozen_mutation>(m);
            return get_local_stream_manager().mutation_send_limiter().wait().then([&msg, this, fm, seq, plan_id, id] {
                auto cf_id = fm->column_family_id();
                // Skip sending the mutation if the cf is dropped after we
                // call to make_local_reader in stream_session::add_transfer_ranges()
                try {
                    session->get_local_db().find_column_family(cf_id);
                } catch (no_such_column_family&) {
                    sslog.info("[Stream #{}] SEND STREAM_MUTATION to {} skipped, cf_id={}", plan_id, id, cf_id);
                    msg.mutations_done.signal();
                    get_local_stream_manager().mutation_send_limiter().signal();
                    return stop_iteration::yes;
                }
                sslog.debug("[Stream #{}] SEND STREAM_MUTATION to {}, cf_id={}", plan_id, id, cf_id);
                session->ms().send_stream_mutation(id, session->plan_id(), *fm, session->dst_cpu_id).then_wrapped([&msg, this, cf_id, plan_id, id, fm] (auto&& f) {
                    try {
                        f.get();
                        session->start_keep_alive_timer();
                        session->add_bytes_sent(fm->representation().size());
                        sslog.debug("[Stream #{}] GOT STREAM_MUTATION Reply from {}", plan_id, id.addr);
                        msg.mutations_done.signal();
                    } catch (std::exception& e) {
                        auto err = std::string(e.what());
                        // Seastar RPC does not provide exception type info, so we can not catch no_such_column_family here
                        // Need to compare the exception error msg
                        if (err.find("Can't find a column family with UUID") != std::string::npos) {
                            sslog.info("[Stream #{}] remote node {} does not have the cf_id = {}", plan_id, id, cf_id);
                            msg.mutations_done.signal();
                        } else {
                            sslog.error("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION to {}: {}", plan_id, id, err);
                            msg.mutations_done.broken();
                        }
                    }
                }).finally([] {
                    get_local_stream_manager().mutation_send_limiter().signal();
                });
                return stop_iteration::no;
            });
        }).then([&msg] {
            return msg.mutations_done.wait(msg.mutations_nr);
        }).then_wrapped([this, seq, plan_id, id] (auto&& f){
            // TODO: Add retry and timeout logic
            try {
                f.get();
                this->complete(seq);
            } catch (...) {
                sslog.error("[Stream #{}] stream_transfer_task: Fail to send outgoing_file_message to {}: {}", plan_id, id, std::current_exception());
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

    auto plan_id = session->plan_id();

    sslog.debug("[Stream #{}] stream_transfer_task: complete cf_id = {}, seq = {}", plan_id, this->cf_id, sequence_number);

    auto signal_complete = files.empty();
    // all file sent, notify session this task is complete.
    if (signal_complete) {
        using msg_addr = net::messaging_service::msg_addr;
        auto id = msg_addr{session->peer, session->dst_cpu_id};
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION_DONE to {}, seq={}", plan_id, id, sequence_number);
        session->ms().send_stream_mutation_done(id, plan_id, std::move(_ranges), this->cf_id, session->dst_cpu_id).then_wrapped([this, id, plan_id] (auto&& f) {
            try {
                f.get();
                session->start_keep_alive_timer();
                sslog.debug("[Stream #{}] GOT STREAM_MUTATION_DONE Reply from {}", plan_id, id.addr);
                session->transfer_task_completed(this->cf_id);
            } catch (...) {
                sslog.error("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION_DONE to {}: {}", plan_id, id, std::current_exception());
                session->on_error();
            }
        });
    }
}

} // namespace streaming
