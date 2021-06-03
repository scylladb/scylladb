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
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include "cql3/query_processor.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_session_state.hh"
#include "streaming/stream_exception.hh"
#include "service/storage_proxy.hh"
#include "service/priority_manager.hh"
#include "service/migration_manager.hh"
#include "query-request.hh"
#include "schema_registry.hh"
#include "mutation_writer/multishard_writer.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "db/system_keyspace.hh"
#include "db/view/view_update_checks.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/map.hpp>
#include "../db/view/view_update_generator.hh"
#include "mutation_source_metadata.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "consumer.hh"

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

static auto get_session(utils::UUID plan_id, gms::inet_address from, const char* verb, std::optional<utils::UUID> cf_id = {}) {
    if (cf_id) {
        sslog.debug("[Stream #{}] GOT {} from {}: cf_id={}", plan_id, verb, from, *cf_id);
    } else {
        sslog.debug("[Stream #{}] GOT {} from {}", plan_id, verb, from);
    }
    auto sr = get_stream_result_future(plan_id);
    if (!sr) {
        auto err = format("[Stream #{}] GOT {} from {}: Can not find stream_manager", plan_id, verb, from);
        sslog.debug("{}", err.c_str());
        throw std::runtime_error(err);
    }
    auto coordinator = sr->get_coordinator();
    if (!coordinator) {
        auto err = format("[Stream #{}] GOT {} from {}: Can not find coordinator", plan_id, verb, from);
        sslog.debug("{}", err.c_str());
        throw std::runtime_error(err);
    }
    return coordinator->get_or_create_session(from);
}

void stream_session::init_messaging_service_handler(netw::messaging_service& ms, shared_ptr<service::migration_manager> mm) {
    ms.register_prepare_message([] (const rpc::client_info& cinfo, prepare_message msg, UUID plan_id, sstring description, rpc::optional<stream_reason> reason_opt) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto dst_cpu_id = this_shard_id();
        auto reason = reason_opt ? *reason_opt : stream_reason::unspecified;
        return smp::submit_to(dst_cpu_id, [msg = std::move(msg), plan_id, description = std::move(description), from, src_cpu_id, dst_cpu_id, reason] () mutable {
            auto sr = stream_result_future::init_receiving_side(plan_id, description, from);
            auto session = get_session(plan_id, from, "PREPARE_MESSAGE");
            session->init(sr);
            session->dst_cpu_id = src_cpu_id;
            session->set_reason(reason);
            return session->prepare(std::move(msg.requests), std::move(msg.summaries));
        });
    });
    ms.register_prepare_done_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [plan_id, from] () mutable {
            auto session = get_session(plan_id, from, "PREPARE_DONE_MESSAGE");
            session->follower_start_sent();
            return make_ready_future<>();
        });
    });
    ms.register_stream_mutation_fragments([&ms, mm] (const rpc::client_info& cinfo, UUID plan_id, UUID schema_id, UUID cf_id, uint64_t estimated_partitions, rpc::optional<stream_reason> reason_opt, rpc::source<frozen_mutation_fragment, rpc::optional<stream_mutation_fragments_cmd>> source) {
        auto from = netw::messaging_service::get_source(cinfo);
        auto reason = reason_opt ? *reason_opt: stream_reason::unspecified;
        sslog.trace("Got stream_mutation_fragments from {} reason {}", from, int(reason));
        table& cf = get_local_db().find_column_family(cf_id);
        if (!_sys_dist_ks->local_is_initialized() || !_view_update_generator->local_is_initialized()) {
            return make_exception_future<rpc::sink<int>>(std::runtime_error(format("Node {} is not fully initialized for streaming, try again later",
                    utils::fb_utilities::get_broadcast_address())));
        }

        return mm->get_schema_for_write(schema_id, from, ms).then([from, estimated_partitions, plan_id, schema_id, &cf, source, reason] (schema_ptr s) mutable {
            auto sink = stream_session::ms().make_sink_for_stream_mutation_fragments(source);
            struct stream_mutation_fragments_cmd_status {
                bool got_cmd = false;
                bool got_end_of_stream = false;
            };
            auto cmd_status = make_lw_shared<stream_mutation_fragments_cmd_status>();
            auto permit = cf.streaming_read_concurrency_semaphore().make_permit(s.get(), "stream-session");
            auto get_next_mutation_fragment = [source, plan_id, from, s, cmd_status, permit] () mutable {
                return source().then([plan_id, from, s, cmd_status, permit] (std::optional<std::tuple<frozen_mutation_fragment, rpc::optional<stream_mutation_fragments_cmd>>> opt) mutable {
                    if (opt) {
                        auto cmd = std::get<1>(*opt);
                        if (cmd) {
                            cmd_status->got_cmd = true;
                            switch (*cmd) {
                            case stream_mutation_fragments_cmd::mutation_fragment_data:
                                break;
                            case stream_mutation_fragments_cmd::error:
                                return make_exception_future<mutation_fragment_opt>(std::runtime_error("Sender failed"));
                            case stream_mutation_fragments_cmd::end_of_stream:
                                cmd_status->got_end_of_stream = true;
                                return make_ready_future<mutation_fragment_opt>();
                            default:
                                return make_exception_future<mutation_fragment_opt>(std::runtime_error("Sender sent wrong cmd"));
                            }
                        }
                        frozen_mutation_fragment& fmf = std::get<0>(*opt);
                        auto sz = fmf.representation().size();
                        auto mf = fmf.unfreeze(*s, permit);
                        streaming::get_local_stream_manager().update_progress(plan_id, from.addr, progress_info::direction::IN, sz);
                        return make_ready_future<mutation_fragment_opt>(std::move(mf));
                    } else {
                        // If the sender has sent stream_mutation_fragments_cmd it means it is
                        // a node that understands the new protocol. It must send end_of_stream
                        // before close the stream.
                        if (cmd_status->got_cmd && !cmd_status->got_end_of_stream) {
                            return make_exception_future<mutation_fragment_opt>(std::runtime_error("Sender did not sent end_of_stream"));
                        }
                        return make_ready_future<mutation_fragment_opt>();
                    }
                });
            };
            //FIXME: discarded future.
            (void)mutation_writer::distribute_reader_and_consume_on_shards(s,
                make_generating_reader(s, permit, std::move(get_next_mutation_fragment)),
                make_streaming_consumer("streaming", *_db, *_sys_dist_ks, *_view_update_generator, estimated_partitions, reason, sstables::offstrategy::no),
                cf.stream_in_progress()
            ).then_wrapped([s, plan_id, from, sink, estimated_partitions] (future<uint64_t> f) mutable {
                int32_t status = 0;
                uint64_t received_partitions = 0;
                if (f.failed()) {
                    sslog.error("[Stream #{}] Failed to handle STREAM_MUTATION_FRAGMENTS (receive and distribute phase) for ks={}, cf={}, peer={}: {}",
                            plan_id, s->ks_name(), s->cf_name(), from.addr, f.get_exception());
                    status = -1;
                } else {
                    received_partitions = f.get0();
                }
                if (received_partitions) {
                    sslog.info("[Stream #{}] Write to sstable for ks={}, cf={}, estimated_partitions={}, received_partitions={}",
                            plan_id, s->ks_name(), s->cf_name(), estimated_partitions, received_partitions);
                }
                return sink(status).finally([sink] () mutable {
                    return sink.close();
                });
            }).handle_exception([s, plan_id, from, sink] (std::exception_ptr ep) {
                sslog.error("[Stream #{}] Failed to handle STREAM_MUTATION_FRAGMENTS (respond phase) for ks={}, cf={}, peer={}: {}",
                        plan_id, s->ks_name(), s->cf_name(), from.addr, ep);
            });
            return make_ready_future<rpc::sink<int>>(sink);
        });
    });
    ms.register_stream_mutation_done([] (const rpc::client_info& cinfo, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [ranges = std::move(ranges), plan_id, cf_id, from] () mutable {
            auto session = get_session(plan_id, from, "STREAM_MUTATION_DONE", cf_id);
            session->receive_task_completed(cf_id);
        });
    });
    ms.register_complete_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        if (failed && *failed) {
            return smp::submit_to(dst_cpu_id, [plan_id, from, dst_cpu_id] () {
                auto session = get_session(plan_id, from, "COMPLETE_MESSAGE");
                sslog.debug("[Stream #{}] COMPLETE_MESSAGE with error flag from {} dst_cpu_id={}", plan_id, from, dst_cpu_id);
                session->received_failed_complete_message();
                return make_ready_future<>();
            });
        } else {
            // Be compatible with old version. Do nothing but return a ready future.
            sslog.debug("[Stream #{}] COMPLETE_MESSAGE from {} dst_cpu_id={}", plan_id, from, dst_cpu_id);
            return make_ready_future<>();
        }
    });
}

future<> stream_session::uninit_messaging_service_handler(netw::messaging_service& ms) {
    return when_all_succeed(
        ms.unregister_prepare_message(),
        ms.unregister_prepare_done_message(),
        ms.unregister_stream_mutation_fragments(),
        ms.unregister_stream_mutation_done(),
        ms.unregister_complete_message()).discard_result();
}

distributed<database>* stream_session::_db;
distributed<db::system_distributed_keyspace>* stream_session::_sys_dist_ks;
distributed<db::view::view_update_generator>* stream_session::_view_update_generator;
sharded<netw::messaging_service>* stream_session::_messaging;

stream_session::stream_session() = default;

stream_session::stream_session(inet_address peer_)
    : peer(peer_) {
    //this.metrics = StreamingMetrics.get(connecting);
}

stream_session::~stream_session() = default;

future<> stream_session::init_streaming_service(distributed<database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks,
        distributed<db::view::view_update_generator>& view_update_generator, sharded<netw::messaging_service>& ms, sharded<service::migration_manager>& mm) {
    _db = &db;
    _sys_dist_ks = &sys_dist_ks;
    _view_update_generator = &view_update_generator;
    _messaging = &ms;
    // #293 - do not stop anything
    // engine().at_exit([] {
    //     return get_stream_manager().stop();
    // });
    return get_stream_manager().start().then([&ms, &mm] {
        gms::get_local_gossiper().register_(get_local_stream_manager().shared_from_this());
        return ms.invoke_on_all([&mm] (netw::messaging_service& ms) { init_messaging_service_handler(ms, mm.local().shared_from_this()); });
    });
}

future<> stream_session::uninit_streaming_service() {
    return _messaging->invoke_on_all([] (netw::messaging_service& ms) {
        return uninit_messaging_service_handler(ms);
    });
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
    return ms().send_prepare_message(id, std::move(prepare), plan_id(), description(), get_reason()).then_wrapped([this, id] (auto&& f) {
        try {
            auto msg = f.get0();
            sslog.debug("[Stream #{}] GOT PREPARE_MESSAGE Reply from {}", this->plan_id(), this->peer);
            this->dst_cpu_id = msg.dst_cpu_id;
            for (auto& summary : msg.summaries) {
                this->prepare_receiving(summary);
            }
            if (_stream_result) {
                _stream_result->handle_session_prepared(this->shared_from_this());
            }
        } catch (...) {
            sslog.warn("[Stream #{}] Fail to send PREPARE_MESSAGE to {}, {}", this->plan_id(), id, std::current_exception());
            throw;
        }
        return make_ready_future<>();
    }).then([this, id] {
        auto plan_id = this->plan_id();
        sslog.debug("[Stream #{}] SEND PREPARE_DONE_MESSAGE to {}", plan_id, id);
        return ms().send_prepare_done_message(id, plan_id, this->dst_cpu_id).then([this] {
            sslog.debug("[Stream #{}] GOT PREPARE_DONE_MESSAGE Reply from {}", this->plan_id(), this->peer);
        }).handle_exception([id, plan_id] (auto ep) {
            sslog.warn("[Stream #{}] Fail to send PREPARE_DONE_MESSAGE to {}, {}", plan_id, id, ep);
            std::rethrow_exception(ep);
        });
    }).then([this] {
        sslog.debug("[Stream #{}] Initiator starts to sent", this->plan_id());
        this->start_streaming_files();
    });
}

void stream_session::received_failed_complete_message() {
    sslog.info("[Stream #{}] Received failed complete message, peer={}", plan_id(), peer);
    _received_failed_complete_message = true;
    close_session(stream_session_state::FAILED);
}

void stream_session::abort() {
    if (sslog.is_enabled(logging::log_level::debug)) {
        sslog.debug("[Stream #{}] Aborted stream session={}, peer={}, is_initialized={}", plan_id(), fmt::ptr(this), peer, is_initialized());
    } else {
        sslog.info("[Stream #{}] Aborted stream session, peer={}, is_initialized={}", plan_id(), peer, is_initialized());
    }
    close_session(stream_session_state::FAILED);
}

void stream_session::on_error() {
    sslog.warn("[Stream #{}] Streaming error occurred, peer={}", plan_id(), peer);
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
            } catch (no_such_column_family&) {
                auto err = format("[Stream #{}] prepare requested ks={} cf={} does not exist", plan_id, ks, cf);
                sslog.warn("{}", err.c_str());
                throw std::runtime_error(err);
            }
        }
        add_transfer_ranges(request.keyspace, request.ranges, request.column_families);
    }
    for (auto& summary : summaries) {
        sslog.debug("[Stream #{}] prepare stream_summary={}", plan_id, summary);
        auto cf_id = summary.cf_id;
        // Make sure cf the peer node will send to us exists
        try {
            db.find_column_family(cf_id);
        } catch (no_such_column_family&) {
            auto err = format("[Stream #{}] prepare cf_id={} does not exist", plan_id, cf_id);
            sslog.warn("{}", err.c_str());
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
    prepare.dst_cpu_id = this_shard_id();
    if (_stream_result) {
        _stream_result->handle_session_prepared(shared_from_this());
    }
    return make_ready_future<prepare_message>(std::move(prepare));
}

void stream_session::follower_start_sent() {
    sslog.debug("[Stream #{}] Follower start to sent", this->plan_id());
    this->start_streaming_files();
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

void stream_session::transfer_task_completed_all() {
    _transfers.clear();
    sslog.debug("[Stream #{}] transfer task_completed: all done, stream_receive_task.size={} stream_transfer_task.size={}",
        plan_id(), _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::send_failed_complete_message() {
    if (!is_initialized()) {
        return;
    }
    auto plan_id = this->plan_id();
    if (_received_failed_complete_message) {
        sslog.debug("[Stream #{}] Skip sending failed message back to peer", plan_id);
        return;
    }
    if (!_complete_sent) {
        _complete_sent = true;
    } else {
        return;
    }
    auto id = msg_addr{this->peer, this->dst_cpu_id};
    sslog.debug("[Stream #{}] SEND COMPLETE_MESSAGE to {}", plan_id, id);
    auto session = shared_from_this();
    bool failed = true;
    //FIXME: discarded future.
    (void)this->ms().send_complete_message(id, plan_id, this->dst_cpu_id, failed).then([session, id, plan_id] {
        sslog.debug("[Stream #{}] GOT COMPLETE_MESSAGE Reply from {}", plan_id, id.addr);
    }).handle_exception([session, id, plan_id] (auto ep) {
        sslog.debug("[Stream #{}] COMPLETE_MESSAGE for {} has failed: {}", plan_id, id.addr, ep);
    });
}

bool stream_session::maybe_completed() {
    bool completed = _receivers.empty() && _transfers.empty();
    if (completed) {
        sslog.debug("[Stream #{}] maybe_completed: {} -> COMPLETE: session={}, peer={}", plan_id(), _state, fmt::ptr(this), peer);
        close_session(stream_session_state::COMPLETE);
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
    //FIXME: discarded future.
    (void)do_for_each(_transfers.begin(), _transfers.end(), [this] (auto& item) {
        sslog.debug("[Stream #{}] Start to send cf_id={}", this->plan_id(), item.first);
        return item.second.execute();
    }).then([this] {
        this->transfer_task_completed_all();
    }).handle_exception([this] (auto ep) {
        sslog.warn("[Stream #{}] Failed to send: {}", this->plan_id(), ep);
        this->on_error();
    });
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
                sslog.debug("Find ks={} cf={}", ks_name, cf_name);
                stores.push_back(&cf);
            }
        }
    } else {
        // TODO: We can move this to database class and use shared_ptr<column_family> instead
        for (auto& cf_name : column_families) {
            try {
                auto& x = db.find_column_family(keyspace, cf_name);
                stores.push_back(&x);
            } catch (no_such_column_family&) {
                sslog.warn("stream_session: {}.{} does not exist: {}\n", keyspace, cf_name, std::current_exception());
                continue;
            }
        }
    }
    return stores;
}

void stream_session::add_transfer_ranges(sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families) {
    auto cfs = get_column_family_stores(keyspace, column_families);
    for (auto& cf : cfs) {
        auto cf_id = cf->schema()->id();
        auto it = _transfers.find(cf_id);
        if (it == _transfers.end()) {
            stream_transfer_task task(shared_from_this(), cf_id, ranges);
            auto inserted = _transfers.emplace(cf_id, std::move(task)).second;
            assert(inserted);
        } else {
            it->second.append_ranges(ranges);
        }
    }
}

future<> stream_session::receiving_failed(UUID cf_id)
{
    return make_ready_future<>();
}

void stream_session::close_session(stream_session_state final_state) {
    sslog.debug("[Stream #{}] close_session session={}, state={}, is_aborted={}", plan_id(), fmt::ptr(this), final_state, _is_aborted);
    if (!_is_aborted) {
        _is_aborted = true;
        set_state(final_state);

        if (final_state == stream_session_state::FAILED) {
            for (auto& x : _transfers) {
                stream_transfer_task& task = x.second;
                sslog.debug("[Stream #{}] close_session session={}, state={}, abort stream_transfer_task cf_id={}", plan_id(), fmt::ptr(this), final_state, task.cf_id);
                task.abort();
            }
            for (auto& x : _receivers) {
                stream_receive_task& task = x.second;
                sslog.debug("[Stream #{}] close_session session={}, state={}, abort stream_receive_task cf_id={}", plan_id(), fmt::ptr(this), final_state, task.cf_id);
                //FIXME: discarded future.
                (void)receiving_failed(x.first);
                task.abort();
            }
            send_failed_complete_message();
        }

        // Note that we shouldn't block on this close because this method is called on the handler
        // incoming thread (so we would deadlock).
        //handler.close();
        if (_stream_result) {
            _stream_result->handle_session_complete(shared_from_this());
        }

        sslog.debug("[Stream #{}] close_session session={}, state={}", plan_id(), fmt::ptr(this), final_state);
    }
}

void stream_session::start() {
    if (_requests.empty() && _transfers.empty()) {
        sslog.info("[Stream #{}] Session does not have any tasks.", plan_id());
        close_session(stream_session_state::COMPLETE);
        return;
    }
    auto connecting = ms().get_preferred_ip(peer);
    if (peer == connecting) {
        sslog.debug("[Stream #{}] Starting streaming to {}", plan_id(), peer);
    } else {
        sslog.debug("[Stream #{}] Starting streaming to {} through {}", plan_id(), peer, connecting);
    }
    //FIXME: discarded future.
    (void)on_initialization_complete().handle_exception([this] (auto ep) {
        this->on_error();
    });
}

bool stream_session::is_initialized() const {
    return bool(_stream_result);
}

void stream_session::init(shared_ptr<stream_result_future> stream_result_) {
    _stream_result = stream_result_;
}

utils::UUID stream_session::plan_id() const {
    return _stream_result ? _stream_result->plan_id : UUID();
}

sstring stream_session::description() const {
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
