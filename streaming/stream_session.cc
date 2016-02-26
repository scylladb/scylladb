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
#include "message/messaging_service.hh"
#include "streaming/stream_session.hh"
#include "streaming/prepare_message.hh"
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
#include "streaming/stream_session_state.hh"
#include "streaming/stream_exception.hh"
#include "service/storage_proxy.hh"
#include "service/priority_manager.hh"
#include "query-request.hh"
#include "schema_registry.hh"

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

static auto get_session(utils::UUID plan_id, gms::inet_address from, const char* verb, std::experimental::optional<utils::UUID> cf_id = {}) {
    if (cf_id) {
        sslog.debug("[Stream #{}] GOT {} from {}: cf_id={}", plan_id, verb, from, *cf_id);
    } else {
        sslog.debug("[Stream #{}] GOT {} from {}", plan_id, verb, from);
    }
    auto sr = get_stream_result_future(plan_id);
    if (!sr) {
        auto err = sprint("[Stream #%s] GOT %s from %s: Can not find stream_manager", plan_id, verb, from);
        sslog.warn(err.c_str());
        throw std::runtime_error(err);
    }
    auto coordinator = sr->get_coordinator();
    if (!coordinator) {
        auto err = sprint("[Stream #%s] GOT %s from %s: Can not find coordinator", plan_id, verb, from);
        sslog.warn(err.c_str());
        throw std::runtime_error(err);
    }
    return coordinator->get_or_create_session(from);
}

void stream_session::init_messaging_service_handler() {
    ms().register_prepare_message([] (const rpc::client_info& cinfo, prepare_message msg, UUID plan_id, sstring description) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto dst_cpu_id = engine().cpu_id();
        return smp::submit_to(dst_cpu_id, [msg = std::move(msg), plan_id, description = std::move(description), from, src_cpu_id, dst_cpu_id] () mutable {
            auto sr = stream_result_future::init_receiving_side(plan_id, description, from);
            auto session = get_session(plan_id, from, "PREPARE_MESSAGE");
            session->init(sr);
            session->dst_cpu_id = src_cpu_id;
            sslog.debug("[Stream #{}] GOT PREPARE_MESSAGE from {}: get session peer={}, dst_cpu_id={}",
                session->plan_id(), from, session->peer, session->dst_cpu_id);
            return session->prepare(std::move(msg.requests), std::move(msg.summaries));
        });
    });
    ms().register_prepare_done_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [plan_id, from] () mutable {
            auto session = get_session(plan_id, from, "PREPARE_DONE_MESSAGE");
            session->follower_start_sent();
            return make_ready_future<>();
        });
    });
    ms().register_stream_mutation([] (const rpc::client_info& cinfo, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id) {
        auto from = net::messaging_service::get_source(cinfo);
        return do_with(std::move(fm), [plan_id, from] (const auto& fm) {
            auto fm_size = fm.representation().size();
            get_local_stream_manager().update_progress(plan_id, from.addr, progress_info::direction::IN, fm_size);
            return service::get_schema_for_write(fm.schema_version(), from).then([&fm] (schema_ptr s) {
                return service::get_storage_proxy().local().mutate_locally(std::move(s), fm);
            });
        });
    });
    ms().register_stream_mutation_done([] (const rpc::client_info& cinfo, UUID plan_id, std::vector<range<dht::token>> ranges, UUID cf_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [ranges = std::move(ranges), plan_id, cf_id, from] () mutable {
            auto session = get_session(plan_id, from, "STREAM_MUTATION_DONE", cf_id);
            session->receive_task_completed(cf_id);
            return session->get_db().invoke_on_all([ranges = std::move(ranges), cf_id] (database& db) {
                auto& cf = db.find_column_family(cf_id);
                for (auto& range : ranges) {
                    cf.get_row_cache().invalidate(query::to_partition_range(range));
                }
            });
        });
    });
    ms().register_complete_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [plan_id, from, dst_cpu_id] () mutable {
            auto session = get_session(plan_id, from, "COMPLETE_MESSAGE");
            session->complete();
        });
    });
}

distributed<database>* stream_session::_db;

stream_session::stream_session() = default;

stream_session::stream_session(inet_address peer_)
    : peer(peer_) {
    //this.metrics = StreamingMetrics.get(connecting);
}

stream_session::~stream_session() = default;

future<> stream_session::init_streaming_service(distributed<database>& db) {
    _db = &db;
    // #293 - do not stop anything
    // engine().at_exit([] {
    //     return get_stream_manager().stop();
    // });
    return get_stream_manager().start().then([] {
        return _db->invoke_on_all([] (auto& db) {
            init_messaging_service_handler();
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
                auto& opts = cql3::query_options::DEFAULT;
                qp.local().process("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", qs, opts).get();
                sslog.debug("CREATE KEYSPACE = KS DONE");
                sleep(std::chrono::seconds(3)).get();
                qp.local().process("CREATE TABLE ks.tb ( key text PRIMARY KEY, C0 text, C1 text, C2 text, C3 blob, C4 text);", qs, opts).get();
                sslog.debug("CREATE TABLE = TB DONE");
                sleep(std::chrono::seconds(3)).get();
                qp.local().process("insert into ks.tb (key,c0) values ('1','1');", qs, opts).get();
                sslog.debug("INSERT VALUE DONE: 1");
                qp.local().process("insert into ks.tb (key,c0) values ('2','2');", qs, opts).get();
                sslog.debug("INSERT VALUE DONE: 2");
                qp.local().process("insert into ks.tb (key,c0) values ('3','3');", qs, opts).get();
                sslog.debug("INSERT VALUE DONE: 3");
                qp.local().process("insert into ks.tb (key,c0) values ('4','4');", qs, opts).get();
                sslog.debug("INSERT VALUE DONE: 4");
                qp.local().process("insert into ks.tb (key,c0) values ('5','5');", qs, opts).get();
                sslog.debug("INSERT VALUE DONE: 5");
                qp.local().process("insert into ks.tb (key,c0) values ('6','6');", qs, opts).get();
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
                    sp.transfer_ranges(to, ks, ranges, cfs).request_ranges(to, ks, ranges, cfs).execute().then_wrapped([] (auto&& f) {
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


future<> stream_session::on_initialization_complete() {
    // send prepare message
    set_state(stream_session_state::PREPARING);
    auto prepare = prepare_message();
    std::copy(_requests.begin(), _requests.end(), std::back_inserter(prepare.requests));
    for (auto& x : _transfers) {
        prepare.summaries.emplace_back(x.second.get_summary());
    }
    auto id = msg_addr{this->peer, 0};
    sslog.debug("[Stream #{}] SEND PREPARE_MESSAGE to {}", plan_id(), id);
    return ms().send_prepare_message(id, std::move(prepare), plan_id(), description()).then_wrapped([this, id] (auto&& f) {
        try {
            auto msg = f.get0();
            sslog.debug("[Stream #{}] GOT PREPARE_MESSAGE Reply from {}", this->plan_id(), this->peer);
            this->dst_cpu_id = msg.dst_cpu_id;
            for (auto& summary : msg.summaries) {
                this->prepare_receiving(summary);
            }
            _stream_result->handle_session_prepared(this->shared_from_this());
            this->start_streaming_files();
        } catch (...) {
            sslog.error("[Stream #{}] Fail to send PREPARE_MESSAGE to {}, {}", this->plan_id(), id, std::current_exception());
            throw;
        }
        return make_ready_future<>();
    }).then([this, id] {
        auto plan_id = this->plan_id();
        sslog.debug("[Stream #{}] SEND PREPARE_DONE_MESSAGE to {}", plan_id, id);
        return ms().send_prepare_done_message(id, plan_id, this->dst_cpu_id).then([this] {
            sslog.debug("[Stream #{}] GOT PREPARE_DONE_MESSAGE Reply from {}", this->plan_id(), this->peer);
        }).handle_exception([id, plan_id] (auto ep) {
            sslog.error("[Stream #{}] Fail to send PREPARE_DONE_MESSAGE to {}, {}", plan_id, id, ep);
            std::rethrow_exception(ep);
        });
    });
}

void stream_session::on_error() {
    sslog.error("[Stream #{}] Streaming error occurred", plan_id());
    // fail session
    close_session(stream_session_state::FAILED);
}

// Only follower calls this function upon receiving of prepare_message from initiator
future<prepare_message> stream_session::prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries) {
    auto plan_id = this->plan_id();
    sslog.debug("[Stream #{}] prepare requests nr={}, summaries nr={}", plan_id, requests.size(), summaries.size());
    // prepare tasks
    set_state(stream_session_state::PREPARING);
    auto& db = get_local_db();
    for (auto& request : requests) {
        // always flush on stream request
        sslog.debug("[Stream #{}] prepare stream_request={}", plan_id, request);
        auto ks = request.keyspace;
        // Make sure cf requested by peer node exists
        for (auto& cf : request.column_families) {
            try {
                db.find_column_family(ks, cf);
            } catch (no_such_column_family) {
                auto err = sprint("[Stream #{}] prepare requested ks={} cf={} does not exist", ks, cf);
                sslog.error(err.c_str());
                throw std::runtime_error(err);
            }
        }
        add_transfer_ranges(request.keyspace, request.ranges, request.column_families);
    }
    for (auto& summary : summaries) {
        sslog.debug("[Stream #{}] prepare stream_summary={}", plan_id, summary);
        auto cf_id = summary.cf_id;
        // Make sure cf the peer node will sent to us exists
        try {
            db.find_column_family(cf_id);
        } catch (no_such_column_family) {
            auto err = sprint("[Stream #{}] prepare cf_id=%s does not exist", plan_id, cf_id);
            sslog.error(err.c_str());
            throw std::runtime_error(err);
        }
        prepare_receiving(summary);
    }

    // Always send a prepare_message back to follower
    prepare_message prepare;
    if (!requests.empty()) {
        for (auto& x: _transfers) {
            auto& task = x.second;
            prepare.summaries.emplace_back(task.get_summary());
        }
    }
    prepare.dst_cpu_id = engine().cpu_id();;
    _stream_result->handle_session_prepared(shared_from_this());
    return make_ready_future<prepare_message>(std::move(prepare));
}

void stream_session::follower_start_sent() {
    sslog.debug("[Stream #{}] Follower start to sent", this->plan_id());
    this->start_streaming_files();
}

void stream_session::complete() {
    if (_state == stream_session_state::WAIT_COMPLETE) {
        send_complete_message();
        sslog.debug("[Stream #{}] complete: WAIT_COMPLETE -> COMPLETE: session={}", plan_id(), this);
        close_session(stream_session_state::COMPLETE);
    } else {
        sslog.debug("[Stream #{}] complete: {} -> WAIT_COMPLETE: session={}", plan_id(), _state, this);
        set_state(stream_session_state::WAIT_COMPLETE);
    }
}

void stream_session::session_failed() {
    close_session(stream_session_state::FAILED);
}

session_info stream_session::make_session_info() {
    std::vector<stream_summary> receiving_summaries;
    for (auto& receiver : _receivers) {
        receiving_summaries.emplace_back(receiver.second.get_summary());
    }
    std::vector<stream_summary> transfer_summaries;
    for (auto& transfer : _transfers) {
        transfer_summaries.emplace_back(transfer.second.get_summary());
    }
    return session_info(peer, std::move(receiving_summaries), std::move(transfer_summaries), _state);
}

void stream_session::receive_task_completed(UUID cf_id) {
    _receivers.erase(cf_id);
    sslog.debug("[Stream #{}] receive  task_completed: cf_id={} done, stream_receive_task.size={} stream_transfer_task.size={}",
        plan_id(), cf_id, _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::transfer_task_completed(UUID cf_id) {
    _transfers.erase(cf_id);
    sslog.debug("[Stream #{}] transfer task_completed: cf_id={} done, stream_receive_task.size={} stream_transfer_task.size={}",
        plan_id(), cf_id, _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::send_complete_message() {
    if (!_complete_sent) {
        _complete_sent = true;
    } else {
        return;
    }
    auto id = msg_addr{this->peer, this->dst_cpu_id};
    auto plan_id = this->plan_id();
    sslog.debug("[Stream #{}] SEND COMPLETE_MESSAGE to {}", plan_id, id);
    auto session = shared_from_this();
    this->ms().send_complete_message(id, plan_id, this->dst_cpu_id).then([session, id, plan_id] {
        sslog.debug("[Stream #{}] GOT COMPLETE_MESSAGE Reply from {}", plan_id, id.addr);
    }).handle_exception([session, id, plan_id] (auto ep) {
        sslog.warn("[Stream #{}] ERROR COMPLETE_MESSAGE Reply from {}: {}", plan_id, id.addr, ep);
        session->on_error();
    });
}

bool stream_session::maybe_completed() {
    bool completed = _receivers.empty() && _transfers.empty();
    if (completed) {
        if (_state == stream_session_state::WAIT_COMPLETE) {
            send_complete_message();
            sslog.debug("[Stream #{}] maybe_completed: WAIT_COMPLETE -> COMPLETE: session={}, peer={}", plan_id(), this, peer);
            close_session(stream_session_state::COMPLETE);
        } else {
            // notify peer that this session is completed
            sslog.debug("[Stream #{}] maybe_completed: {} -> WAIT_COMPLETE: session={}, peer={}", plan_id(), _state, this, peer);
            set_state(stream_session_state::WAIT_COMPLETE);
            send_complete_message();
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
    sslog.debug("[Stream #{}] {}: {} transfers to send", plan_id(), __func__, _transfers.size());
    if (!_transfers.empty()) {
        set_state(stream_session_state::STREAMING);
    }
    for (auto it = _transfers.begin(); it != _transfers.end();) {
        stream_transfer_task& task = it->second;
        it++;
        task.start();
    }
}

std::vector<column_family*> stream_session::get_column_family_stores(const sstring& keyspace, const std::vector<sstring>& column_families) {
    // if columnfamilies are not specified, we add all cf under the keyspace
    std::vector<column_family*> stores;
    auto& db = get_local_db();
    if (column_families.empty()) {
        for (auto& x : db.get_column_families()) {
            column_family& cf = *(x.second);
            auto cf_name = cf.schema()->cf_name();
            auto ks_name = cf.schema()->ks_name();
            if (ks_name == keyspace) {
                sslog.info("Find ks={} cf={}", ks_name, cf_name);
                stores.push_back(&cf);
            }
        }
    } else {
        // TODO: We can move this to database class and use shared_ptr<column_family> instead
        for (auto& cf_name : column_families) {
            try {
                auto& x = db.find_column_family(keyspace, cf_name);
                stores.push_back(&x);
            } catch (no_such_column_family) {
                sslog.warn("stream_session: {}.{} does not exist: {}\n", keyspace, cf_name, std::current_exception());
                continue;
            }
        }
    }
    return stores;
}

void stream_session::add_transfer_ranges(sstring keyspace, std::vector<query::range<token>> ranges, std::vector<sstring> column_families) {
    auto cfs = get_column_family_stores(keyspace, column_families);
    for (auto& cf : cfs) {
        auto cf_id = cf->schema()->id();
        stream_transfer_task task(shared_from_this(), cf_id, ranges);
        auto inserted = _transfers.emplace(cf_id, std::move(task)).second;
        assert(inserted);
    }
}

void stream_session::close_session(stream_session_state final_state) {
    sslog.debug("[Stream #{}] close_session session={}, state={}, is_aborted={}", plan_id(), this, final_state, _is_aborted);
    if (!_is_aborted) {
        _is_aborted = true;
        set_state(final_state);

        if (final_state == stream_session_state::FAILED) {
            for (auto& x : _transfers) {
                stream_transfer_task& task = x.second;
                sslog.debug("[Stream #{}] close_session session={}, state={}, abort stream_transfer_task cf_id={}", plan_id(), this, final_state, task.cf_id);
                task.abort();
            }
            for (auto& x : _receivers) {
                stream_receive_task& task = x.second;
                sslog.debug("[Stream #{}] close_session session={}, state={}, abort stream_receive_task cf_id={}", plan_id(), this, final_state, task.cf_id);
                task.abort();
            }
        }

        // Note that we shouldn't block on this close because this method is called on the handler
        // incoming thread (so we would deadlock).
        //handler.close();
        _stream_result->handle_session_complete(shared_from_this());

        sslog.debug("[Stream #{}] close_session session={}, state={}, cancel keep_alive timer", plan_id(), this, final_state);
        _keep_alive.cancel();
    }
}

void stream_session::start() {
    if (_requests.empty() && _transfers.empty()) {
        sslog.info("[Stream #{}] Session does not have any tasks.", plan_id());
        close_session(stream_session_state::COMPLETE);
        return;
    }
    auto connecting = net::get_local_messaging_service().get_preferred_ip(peer);
    if (peer == connecting) {
        sslog.info("[Stream #{}] Starting streaming to {}", plan_id(), peer);
    } else {
        sslog.info("[Stream #{}] Starting streaming to {} through {}", plan_id(), peer, connecting);
    }
    on_initialization_complete().handle_exception([this] (auto ep) {
        this->on_error();
    });
}

void stream_session::init(shared_ptr<stream_result_future> stream_result_) {
    _stream_result = stream_result_;
    _keep_alive.set_callback([this] {
        auto plan_id = this->plan_id();
        get_local_stream_manager().get_progress_on_all_shards(plan_id, this->peer).then([this] (stream_bytes sbytes) {
            if (sbytes != this->_last_stream_bytes) {
                this->_last_stream_bytes = sbytes;
                this->start_keep_alive_timer();
            } else {
                sslog.info("[Stream #{}] The session {} is idle for {} seconds, the peer {} is probably gone, close it",
                    this->plan_id(), this, this->_keep_alive_timeout.count(), this->peer);
                this->on_error();
            }
        }).handle_exception([plan_id] (auto ep) {
           sslog.info("[Stream #{}] keep alive timer callback fails: {}", plan_id, ep);
        });
    });
    start_keep_alive_timer();
}

utils::UUID stream_session::plan_id() {
    return _stream_result ? _stream_result->plan_id : UUID();
}

sstring stream_session::description() {
    return _stream_result  ? _stream_result->description : "";
}

future<> stream_session::update_progress() {
    return get_local_stream_manager().get_progress_on_all_shards(plan_id(), peer).then([this] (auto sbytes) {
        auto bytes_sent = sbytes.bytes_sent;
        if (bytes_sent > 0) {
            auto tx = progress_info(this->peer, "txnofile", progress_info::direction::OUT, bytes_sent, bytes_sent);
            _session_info.update_progress(std::move(tx));
        }
        auto bytes_received = sbytes.bytes_received;
        if (bytes_received > 0) {
            auto rx = progress_info(this->peer, "rxnofile", progress_info::direction::IN, bytes_received, bytes_received);
            _session_info.update_progress(std::move(rx));
        }
    });
}

} // namespace streaming
