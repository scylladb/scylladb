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
#include "streaming/stream_manager.hh"
#include "mutation_reader.hh"
#include "dht/i_partitioner.hh"
#include "database.hh"
#include "utils/fb_utilities.hh"
#include "streaming/stream_plan.hh"
#include "core/sleep.hh"
#include "service/storage_service.hh"
#include "core/thread.hh"
#include "cql3/query_processor.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_exception.hh"
#include "service/storage_proxy.hh"

namespace streaming {

logging::logger sslog("stream_session");

static auto get_stream_result_future(utils::UUID plan_id) {
    auto& sm = get_local_stream_manager();
    auto f = sm.get_sending_stream(plan_id);
    if (!f) {
        f = sm.get_receiving_stream(plan_id);
    }
    return f;
}

void stream_session::init_messaging_service_handler() {
    ms().register_stream_init_message([] (messages::stream_init_message msg, unsigned src_cpu_id) {
        auto dst_cpu_id = engine().cpu_id();
        sslog.debug("GOT STREAM_INIT_MESSAGE");
        return smp::submit_to(dst_cpu_id, [msg = std::move(msg), src_cpu_id, dst_cpu_id] () mutable {
            stream_result_future::init_receiving_side(msg.session_index, msg.plan_id,
                msg.description, msg.from, msg.keep_ss_table_level);
            return make_ready_future<unsigned>(dst_cpu_id);
        });
    });
    ms().register_prepare_message([] (messages::prepare_message msg, UUID plan_id, inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id) {
        return smp::submit_to(dst_cpu_id, [msg = std::move(msg), plan_id, from, connecting, src_cpu_id] () mutable {
            auto f = get_stream_result_future(plan_id);
            sslog.debug("GOT PREPARE_MESSAGE: plan_id={}, from={}, connecting={}", plan_id, from, connecting);
            if (f) {
                auto coordinator = f->get_coordinator();
                assert(coordinator);
                auto session = coordinator->get_or_create_next_session(from, from);
                assert(session);
                session->init(f);
                session->dst_cpu_id = src_cpu_id;
                sslog.debug("PREPARE_MESSAGE: get session peer={} connecting={} plan_id={} src_cpu_id={}, dst_cpu_id={}",
                    session->peer, session->connecting, session->plan_id(), session->src_cpu_id, session->dst_cpu_id);
                return session->prepare(std::move(msg.requests), std::move(msg.summaries));
            } else {
                auto err = sprint("PREPARE_MESSAGE: Can not find stream_manager for plan_id=%s", plan_id);
                sslog.warn(err.c_str());
                throw std::runtime_error(err);
            }
        });
    });
    ms().register_stream_mutation([] (UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id) {
        return smp::submit_to(dst_cpu_id, [plan_id, fm = std::move(fm)] () mutable {
            if (sslog.is_enabled(logging::log_level::debug)) {
                auto cf_id = fm.column_family_id();
                sslog.debug("GOT STREAM_MUTATION: plan_id={}, cf_id={}", plan_id, cf_id);
            }
            return service::get_storage_proxy().local().mutate_locally(fm);
        });
    });
    ms().register_stream_mutation_done([] (UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
        return smp::submit_to(dst_cpu_id, [plan_id, cf_id, from, connecting] () mutable {
            sslog.debug("GOT STREAM_MUTATION_DONE: plan_id={}, cf_id={}, from={}, connecting={}", plan_id, cf_id, from, connecting);
            auto f = get_stream_result_future(plan_id);
            if (f) {
                auto coordinator = f->get_coordinator();
                assert(coordinator);
                auto session = coordinator->get_or_create_next_session(from, from);
                assert(session);
                session->receive_task_completed(cf_id);
                return make_ready_future<>();
            } else {
                auto err = sprint("STREAM_MUTATION_DONE: Can not find stream_manager for plan_id=%s", plan_id);
                sslog.warn(err.c_str());
                throw std::runtime_error(err);
            }
        });
    });
#if 0
    ms().register_handler(messaging_verb::RETRY_MESSAGE, [] (messages::retry_message msg, unsigned dst_cpu_id) {
        return smp::submit_to(dst_cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
            return make_ready_future<>();
        });
    });
#endif
    ms().register_complete_message([] (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
        smp::submit_to(dst_cpu_id, [plan_id, from, connecting, dst_cpu_id] () mutable {
            sslog.debug("GOT COMPLETE_MESSAGE, plan_id={}, from={}, connecting={}, dst_cpu_id={}", plan_id, from, connecting, dst_cpu_id);
            auto f = get_stream_result_future(plan_id);
            if (f) {
                auto coordinator = f->get_coordinator();
                assert(coordinator);
                auto session = coordinator->get_or_create_next_session(from, from);
                assert(session);
                session->complete();
            } else {
                auto err = sprint("COMPLETE_MESSAGE: Can not find stream_manager for plan_id=%s", plan_id);
                sslog.warn(err.c_str());
                throw std::runtime_error(err);
            }
        });
        return messaging_service::no_wait();
    });
#if 0
    ms().register_handler(messaging_verb::SESSION_FAILED_MESSAGE, [] (messages::session_failed_message msg, unsigned dst_cpu_id) {
        smp::submit_to(dst_cpu_id, [msg = std::move(msg)] () mutable {
            // TODO
        }).then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                sslog.debug("stream_session: SESSION_FAILED_MESSAGE error");
            }
        });
        return messaging_service::no_wait();
    });
#endif
}

distributed<stream_session::handler> stream_session::_handlers;
distributed<database>* stream_session::_db;

stream_session::stream_session() = default;

stream_session::stream_session(inet_address peer_, inet_address connecting_, int index_, bool keep_ss_table_level_)
    : peer(peer_)
    , connecting(connecting_)
    , _index(index_)
    , _keep_ss_table_level(keep_ss_table_level_) {
    //this.metrics = StreamingMetrics.get(connecting);
}

stream_session::~stream_session() = default;

future<> stream_session::init_streaming_service(distributed<database>& db) {
    _db = &db;
    engine().at_exit([] {
        return _handlers.stop().then([]{
            return get_stream_manager().stop();
        });
    });
    return get_stream_manager().start().then([] {
        return _handlers.start().then([] {
            return _handlers.invoke_on_all([] (handler& h) {
                init_messaging_service_handler();
            });
        });
    });
}

future<> stream_session::test(distributed<cql3::query_processor>& qp) {
    if (utils::fb_utilities::get_broadcast_address() == inet_address("127.0.0.1")) {
        auto tester = make_shared<timer<lowres_clock>>();
        tester->set_callback ([tester, &qp] {
            seastar::async([&qp] {
                sslog.debug("================ STREAM_PLAN TEST ==============");
                auto cs = service::client_state::for_external_calls();
                service::query_state qs(cs);
                auto opts = make_shared<cql3::query_options>(cql3::query_options::DEFAULT);
                qp.local().process("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", qs, *opts).get();
                sslog.debug("CREATE KEYSPACE = KS DONE");
                qp.local().process("CREATE TABLE ks.tb ( key text PRIMARY KEY, C0 text, C1 text, C2 text, C3 blob, C4 text);", qs, *opts).get();
                sslog.debug("CREATE TABLE = TB DONE");
                qp.local().process("insert into ks.tb (key,c0) values ('1','1');", qs, *opts).get();
                sslog.debug("INSERT VALUE DONE: 1");
                qp.local().process("insert into ks.tb (key,c0) values ('2','2');", qs, *opts).get();
                sslog.debug("INSERT VALUE DONE: 2");
                qp.local().process("insert into ks.tb (key,c0) values ('3','3');", qs, *opts).get();
                sslog.debug("INSERT VALUE DONE: 3");
                qp.local().process("insert into ks.tb (key,c0) values ('4','4');", qs, *opts).get();
                sslog.debug("INSERT VALUE DONE: 4");
                qp.local().process("insert into ks.tb (key,c0) values ('5','5');", qs, *opts).get();
                sslog.debug("INSERT VALUE DONE: 5");
                qp.local().process("insert into ks.tb (key,c0) values ('6','6');", qs, *opts).get();
                sslog.debug("INSERT VALUE DONE: 6");
            }).then([] {
                sleep(std::chrono::seconds(10)).then([] {
                    sslog.debug("================ START STREAM  ==============");
                    auto sp = stream_plan("MYPLAN");
                    auto to = inet_address("127.0.0.2");
                    auto tb = sstring("tb");
                    auto ks = sstring("ks");
                    std::vector<query::range<token>> ranges = {query::range<token>::make_open_ended_both_sides()};
                    std::vector<sstring> cfs{tb};
                    sp.transfer_ranges(to, to, ks, ranges, cfs).request_ranges(to, to, ks, ranges, cfs).execute().then_wrapped([] (auto&& f) {
                        try {
                            auto state = f.get0();
                            sslog.debug("plan_id={} description={} DONE", state.plan_id, state.description);
                            sslog.debug("================ FINISH STREAM  ==============");
                        } catch (const stream_exception& e) {
                            auto& state = e.state;
                            sslog.debug("plan_id={} description={} FAIL: {}", state.plan_id, state.description, e.what());
                            sslog.error("================ FAIL   STREAM  ==============");
                        }
                    });
                });
            });
        });
        tester->arm(std::chrono::seconds(10));
    }
    return make_ready_future<>();
}

future<> stream_session::initiate() {
    sslog.debug("[Stream #{}] Sending stream init for incoming stream", plan_id());
    auto from = utils::fb_utilities::get_broadcast_address();
    bool is_for_outgoing = true;
    messages::stream_init_message msg(from, session_index(), plan_id(), description(),
            is_for_outgoing, keep_ss_table_level());
    auto id = shard_id{this->peer, 0};
    this->src_cpu_id = engine().cpu_id();
    sslog.debug("SEND SENDSTREAM_INIT_MESSAGE to {}", id);
    return ms().send_stream_init_message(std::move(id), std::move(msg), this->src_cpu_id).then_wrapped([this, id] (auto&& f) {
        try {
            unsigned dst_cpu_id = f.get0();
            sslog.debug("GOT STREAM_INIT_MESSAGE Reply: dst_cpu_id={}", dst_cpu_id);
            this->dst_cpu_id = dst_cpu_id;
        } catch (...) {
            sslog.error("Fail to send STREAM_INIT_MESSAGE to {}", id);
            throw;
        }
        return make_ready_future<>();
    });
}


future<> stream_session::on_initialization_complete() {
    // send prepare message
    set_state(stream_session_state::PREPARING);
    auto prepare = messages::prepare_message();
    std::copy(_requests.begin(), _requests.end(), std::back_inserter(prepare.requests));
    for (auto& x : _transfers) {
        prepare.summaries.emplace_back(x.second.get_summary());
    }
    auto id = shard_id{this->peer, this->dst_cpu_id};
    auto from = utils::fb_utilities::get_broadcast_address();
    sslog.debug("SEND PREPARE_MESSAGE to {}", id);
    return ms().send_prepare_message(id, std::move(prepare), plan_id(), from,
        this->connecting, this->src_cpu_id, this->dst_cpu_id).then_wrapped([this, id] (auto&& f) {
        try {
            auto msg = f.get0();
            sslog.debug("GOT PREPARE_MESSAGE Reply");
            for (auto& summary : msg.summaries) {
                this->prepare_receiving(summary);
            }
            this->start_streaming_files();
        } catch (...) {
            sslog.error("Fail to send PREPARE_MESSAGE to {}", id);
            throw;
        }
        return make_ready_future<>();
    });
}

void stream_session::on_error() {
    sslog.error("[Stream #{}] Streaming error occurred", plan_id());
#if 0
    // send session failure message
    if (handler.is_outgoing_connected()) {
       handler.sendMessage(session_failed_message());
    }
#endif
    // fail session
    close_session(stream_session_state::FAILED);
}

// Only follower calls this function upon receiving of prepare_message from initiator
future<messages::prepare_message> stream_session::prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries) {
    sslog.debug("stream_session::prepare requests nr={}, summaries nr={}", requests.size(), summaries.size());
    // prepare tasks
    set_state(stream_session_state::PREPARING);
    auto& db = get_local_db();
    for (auto& request : requests) {
        // always flush on stream request
        sslog.debug("stream_session::prepare stream_request={}", request);
        auto ks = request.keyspace;
        // Make sure cf requested by peer node exists
        for (auto& cf : request.column_families) {
            try {
                db.find_column_family(ks, cf);
            } catch (no_such_column_family) {
                auto err = sprint("prepare: requested ks={} cf={} does not exist", ks, cf);
                sslog.error(err.c_str());
                throw std::runtime_error(err);
            }
        }
        add_transfer_ranges(request.keyspace, request.ranges, request.column_families, true, request.repaired_at);
    }
    for (auto& summary : summaries) {
        sslog.debug("stream_session::prepare stream_summary={}", summary);
        auto cf_id = summary.cf_id;
        // Make sure cf the peer node will sent to us exists
        try {
            db.find_column_family(cf_id);
        } catch (no_such_column_family) {
            auto err = sprint("prepare: cf_id=%s does not exist", cf_id);
            sslog.error(err.c_str());
            throw std::runtime_error(err);
        }
        prepare_receiving(summary);
    }

    // Always send a prepare_message back to follower
    messages::prepare_message prepare;
    if (!requests.empty()) {
        for (auto& x: _transfers) {
            auto& task = x.second;
            prepare.summaries.emplace_back(task.get_summary());
        }
    }

    // if there are files to stream
    if (!maybe_completed()) {
        start_streaming_files();
    }

    return make_ready_future<messages::prepare_message>(std::move(prepare));
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
    it->second.received(std::move(message));
}

void stream_session::progress(/* Descriptor desc */ progress_info::direction dir, long bytes, long total) {
    auto progress = progress_info(peer, _index, "", dir, bytes, total);
    _stream_result->handle_progress(std::move(progress));
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
            send_complete_message().then([this] {
                _complete_sent = true;
            });
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

void stream_session::receive_task_completed(UUID cf_id) {
    _receivers.erase(cf_id);
    sslog.debug("receive_task_completed: cf_id={} done, {} {}", cf_id, _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::task_completed(stream_receive_task& completed_task) {
    _receivers.erase(completed_task.cf_id);
    maybe_completed();
}

void stream_session::task_completed(stream_transfer_task& completed_task) {
    _transfers.erase(completed_task.cf_id);
    maybe_completed();
}

future<> stream_session::send_complete_message() {
    auto from = utils::fb_utilities::get_broadcast_address();
    auto id = shard_id{this->peer, this->dst_cpu_id};
    sslog.debug("SEND COMPLETE_MESSAGE to {}, plan_id={}", id, this->plan_id());
    return this->ms().send_complete_message(id, this->plan_id(), from, this->connecting, this->dst_cpu_id).then_wrapped([] (auto&& f) {
        try {
            f.get();
            sslog.debug("GOT COMPLETE_MESSAGE Reply");
            return make_ready_future<>();
        } catch (...) {
            sslog.warn("ERROR COMPLETE_MESSAGE Reply");
            return make_ready_future<>();
        }
    });
}

bool stream_session::maybe_completed() {
    bool completed = _receivers.empty() && _transfers.empty();
    if (completed) {
        if (_state == stream_session_state::WAIT_COMPLETE) {
            if (!_complete_sent) {
                send_complete_message().then([this] {
                    _complete_sent = true;
                });
            }
            close_session(stream_session_state::COMPLETE);
            sslog.debug("session complete");
        } else {
            // notify peer that this session is completed
            send_complete_message().then([this] {
                _complete_sent = true;
                set_state(stream_session_state::WAIT_COMPLETE);
                sslog.debug("session wait complete");
            });
        }
    }
    return completed;
}

void stream_session::prepare_receiving(stream_summary& summary) {
    if (summary.files > 0) {
        // FIXME: handle when cf_id already exists
        _receivers.emplace(summary.cf_id, stream_receive_task(shared_from_this(), summary.cf_id, summary.files, summary.total_size));
    }
}

void stream_session::start_streaming_files() {
    _stream_result->handle_session_prepared(shared_from_this());
#if 0
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
    set_state(stream_session_state::STREAMING);
    sslog.debug("{}: {} transfers to send", __func__, _transfers.size());
    for (auto& x : _transfers) {
        stream_transfer_task& task = x.second;
        task.start();
    }
}

std::vector<column_family*> stream_session::get_column_family_stores(const sstring& keyspace, const std::vector<sstring>& column_families) {
    // if columnfamilies are not specified, we add all cf under the keyspace
    std::vector<column_family*> stores;
    auto& db = get_local_db();
    if (column_families.empty()) {
        abort();
        // FIXME: stores.addAll(Keyspace.open(keyspace).getColumnFamilyStores());
    } else {
        // TODO: We can move this to database class and use shared_ptr<column_family> instead
        for (auto& cf_name : column_families) {
            try {
                auto& x = db.find_column_family(keyspace, cf_name);
                stores.push_back(&x);
            } catch (no_such_column_family) {
                sslog.warn("stream_session: {}.{} does not exist\n", keyspace, cf_name);
                continue;
            }
        }
    }
    return stores;
}

void stream_session::add_transfer_ranges(sstring keyspace, std::vector<query::range<token>> ranges, std::vector<sstring> column_families, bool flush_tables, long repaired_at) {
    std::vector<stream_detail> stream_details;
    auto cfs = get_column_family_stores(keyspace, column_families);
    if (flush_tables) {
        // FIXME: flushSSTables(stores);
    }
    for (auto& cf : cfs) {
        std::vector<mutation_reader> readers;
        auto cf_id = cf->schema()->id();
        for (auto& range : ranges) {
            auto pr = query::to_partition_range(range);
            auto mr = service::get_storage_proxy().local().make_local_reader(cf_id, pr);
            readers.push_back(std::move(mr));
        }
        // Store this mutation_reader so we can send mutaions later
        mutation_reader mr = make_combined_reader(std::move(readers));
        // FIXME: sstable.estimatedKeysForRanges(ranges)
        long estimated_keys = 0;
        stream_details.emplace_back(std::move(cf_id), std::move(mr), estimated_keys, repaired_at);
    }
    if (!stream_details.empty()) {
        add_transfer_files(std::move(stream_details));
    }
}

void stream_session::add_transfer_files(std::vector<stream_detail> stream_details) {
    for (auto& detail : stream_details) {
#if 0
        if (details.sections.empty()) {
            // A reference was acquired on the sstable and we won't stream it
            // FIXME
            // details.sstable.releaseReference();
            continue;
        }
#endif
        UUID cf_id = detail.cf_id;
        auto it = _transfers.find(cf_id);
        if (it == _transfers.end()) {
            it = _transfers.emplace(cf_id, stream_transfer_task(shared_from_this(), cf_id)).first;
        }
        it->second.add_transfer_file(std::move(detail));
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
        _stream_result->handle_session_complete(shared_from_this());
    }
}

void stream_session::start() {
    if (_requests.empty() && _transfers.empty()) {
        sslog.info("[Stream #{}] Session does not have any tasks.", plan_id());
        close_session(stream_session_state::COMPLETE);
        return;
    }
    if (peer == connecting) {
        sslog.info("[Stream #{}] Starting streaming to {}", plan_id(), peer);
    } else {
        sslog.info("[Stream #{}] Starting streaming to {} through {}", plan_id(), peer, connecting);
    }
    initiate().then([this] {
        return on_initialization_complete();
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            this->on_error();
        }
    });
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
