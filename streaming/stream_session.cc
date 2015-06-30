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
#include "streaming/stream_result_future.hh"

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

distributed<stream_session::handler> stream_session::_handlers;

future<> stream_session::init_streaming_service() {
    return _handlers.start().then([this] {
        return _handlers.invoke_on_all([this] (handler& h) {
            this->init_messaging_service_handler();
        });
    });
}

void stream_session::on_initialization_complete() {
    // send prepare message
    set_state(stream_session_state::PREPARING);
    auto prepare = messages::prepare_message();
    std::copy(_requests.begin(), _requests.end(), std::back_inserter(prepare.requests));
    for (auto& x : _transfers) {
        prepare.summaries.emplace_back(x.second.get_summary());
    }
    // handler.sendMessage(prepare);

    // if we don't need to prepare for receiving stream, start sending files immediately
    if (_requests.empty()) {
        start_streaming_files();
    }
}

void stream_session::on_error() {
#if 0
    //logger.error("[Stream #{}] Streaming error occurred", planId(), e);
    // send session failure message
    if (handler.is_outgoing_connected()) {
       handler.sendMessage(session_failed_message());
    }
#endif
    // fail session
    close_session(stream_session_state::FAILED);
}

void stream_session::prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries) {
    // prepare tasks
    set_state(stream_session_state::PREPARING);
    for (auto& request : requests) {
        // always flush on stream request
        add_transfer_ranges(request.keyspace, request.ranges, request.column_families, true, request.repaired_at);
    }
    for (auto& summary : summaries) {
        prepare_receiving(summary);
    }

    // send back prepare message if prepare message contains stream request
    if (!requests.empty()) {
        messages::prepare_message prepare;
        for (auto& x: _transfers) {
            auto& task = x.second;
            prepare.summaries.emplace_back(task.get_summary());
        }
        //handler.send_message(std::move(prepare));
    }

    // if there are files to stream
    if (!maybe_completed()) {
        start_streaming_files();
    }
}

void stream_session::file_sent(const messages::file_message_header& header) {
#if 0
    auto header_size = header.size();
    StreamingMetrics.totalOutgoingBytes.inc(headerSize);
    metrics.outgoingBytes.inc(headerSize);
#endif
    // schedule timeout for receiving ACK
    auto it = _transfers.find(header.cf_id);
    if (it != _transfers.end()) {
        //task.scheduleTimeout(header.sequenceNumber, 12, TimeUnit.HOURS);
    }
}

void stream_session::receive(messages::incoming_file_message message) {
#if 0
    auto header_size = message.header.size();
    StreamingMetrics.totalIncomingBytes.inc(headerSize);
    metrics.incomingBytes.inc(headerSize);
#endif
    // send back file received message
    // handler.sendMessage(new ReceivedMessage(message.header.cfId, message.header.sequenceNumber));
    auto cf_id = message.header.cf_id;
    auto it = _receivers.find(cf_id);
    assert(it != _receivers.end());
    it->second.received(message.sstable);
}

void stream_session::progress(/* Descriptor desc */ progress_info::direction dir, long bytes, long total) {
    // auto progress = progress_info(peer, _index, /* desc.filenameFor(Component.DATA),*/ dir, bytes, total);
    // streamResult.handleProgress(progress);
}

void stream_session::received(UUID cf_id, int sequence_number) {
    auto it = _transfers.find(cf_id);
    if (it != _transfers.end()) {
        it->second.complete(sequence_number);
    }
}

void stream_session::retry(UUID cf_id, int sequence_number) {
    auto it = _transfers.find(cf_id);
    if (it != _transfers.end()) {
        //outgoing_file_message message = it->second.create_message_for_retry(sequence_number);
        //handler.sendMessage(message);
    }
}

void stream_session::complete() {
    if (_state == stream_session_state::WAIT_COMPLETE) {
        if (!_complete_sent) {
            //handler.sendMessage(new CompleteMessage());
            _complete_sent = true;
        }
       close_session(stream_session_state::COMPLETE);
    } else {
        set_state(stream_session_state::WAIT_COMPLETE);
    }
}

void stream_session::session_failed() {
    close_session(stream_session_state::FAILED);
}

session_info stream_session::get_session_info() {
    std::vector<stream_summary> receiving_summaries;
    for (auto& receiver : _receivers) {
        receiving_summaries.emplace_back(receiver.second.get_summary());
    }
    std::vector<stream_summary> transfer_summaries;
    for (auto& transfer : _transfers) {
        transfer_summaries.emplace_back(transfer.second.get_summary());
    }
    return session_info(peer, _index, connecting, std::move(receiving_summaries), std::move(transfer_summaries), _state);
}

void stream_session::task_completed(stream_receive_task& completed_task) {
    _receivers.erase(completed_task.cf_id);
    maybe_completed();
}

void stream_session::task_completed(stream_transfer_task& completed_task) {
    _transfers.erase(completed_task.cf_id);
    maybe_completed();
}

bool stream_session::maybe_completed() {
    bool completed = _receivers.empty() && _transfers.empty();
    if (completed) {
        if (_state == stream_session_state::WAIT_COMPLETE) {
            if (!_complete_sent) {
                //handler.sendMessage(new CompleteMessage());
                _complete_sent = true;
            }
            close_session(stream_session_state::COMPLETE);
        } else {
            // notify peer that this session is completed
            //handler.sendMessage(new CompleteMessage());
            _complete_sent = true;
            set_state(stream_session_state::WAIT_COMPLETE);
        }
    }
    return completed;
}

void stream_session::prepare_receiving(stream_summary& summary) {
    if (summary.files > 0) {
        // FIXME: handle when cf_id already exists
        _receivers.emplace(summary.cf_id, stream_receive_task(*this, summary.cf_id, summary.files, summary.total_size));
    }
}

void stream_session::start_streaming_files() {
#if 0
    streamResult.handleSessionPrepared(this);

    state(State.STREAMING);
    for (StreamTransferTask task : transfers.values())
    {
        Collection<OutgoingFileMessage> messages = task.getFileMessages();
        if (messages.size() > 0)
            handler.sendMessages(messages);
        else
            taskCompleted(task); // there is no file to send
    }
#endif
}

void stream_session::add_transfer_files(std::vector<ss_table_streaming_sections> sstable_details) {
    for (auto& details : sstable_details) {
        if (details.sections.empty()) {
            // A reference was acquired on the sstable and we won't stream it
            // FIXME
            // details.sstable.releaseReference();
            continue;
        }
        // FIXME
        UUID cf_id; // details.sstable.metadata.cfId;
        auto it = _transfers.find(cf_id);
        if (it == _transfers.end()) {
            it = _transfers.emplace(cf_id, stream_transfer_task(*this, cf_id)).first;
        }
        it->second.add_transfer_file(details.sstable, details.estimated_keys, std::move(details.sections), details.repaired_at);
    }
}

void stream_session::close_session(stream_session_state final_state) {
    if (!_is_aborted) {
        _is_aborted = true;
        set_state(final_state);

        if (final_state == stream_session_state::FAILED) {
            for (auto& x : _transfers) {
                x.second.abort();
            }
            for (auto& x : _receivers) {
                x.second.abort();
            }
        }

        // Note that we shouldn't block on this close because this method is called on the handler
        // incoming thread (so we would deadlock).
        //handler.close();

        //streamResult.handleSessionComplete(this);
    }
}

void stream_session::start() {
    if (_requests.empty() && _transfers.empty()) {
        //logger.info("[Stream #{}] Session does not have any tasks.", planId());
        close_session(stream_session_state::COMPLETE);
        return;
    }

    try {
        // logger.info("[Stream #{}] Starting streaming to {}{}", plan_id(),
        //                                                        peer, peer == connecting ? "" : " through " + connecting);
        conn_handler.initiate();
        on_initialization_complete();
    } catch (...) {
        on_error();
    }
}

void stream_session::init(shared_ptr<stream_result_future> stream_result_) {
    _stream_result = stream_result_;
}

utils::UUID stream_session::plan_id() {
    return _stream_result ? _stream_result->plan_id : UUID();
}

sstring stream_session::description() {
    return _stream_result  ? _stream_result->description : "";
}

} // namespace streaming
