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
 * Copyright (C) 2015 ScyllaDB
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
#include "multishard_writer.hh"
#include "sstables/sstables.hh"
#include "db/system_keyspace.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/map.hpp>

namespace streaming {

logging::logger sslog("stream_session");

/*
 * This reader takes a get_next_fragment generator that produces mutation_fragment_opt which is returned by
 * generating_reader.
 *
 */
class generating_reader final : public flat_mutation_reader::impl {
    std::function<future<mutation_fragment_opt> ()> _get_next_fragment;
public:
    generating_reader(schema_ptr s, std::function<future<mutation_fragment_opt> ()> get_next_fragment)
        : impl(std::move(s)), _get_next_fragment(std::move(get_next_fragment))
    { }
    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _get_next_fragment().then([this] (mutation_fragment_opt mopt) {
                if (!mopt) {
                    _end_of_stream = true;
                } else {
                    push_mutation_fragment(std::move(*mopt));
                }
            });
        });
    }
    virtual void next_partition() override {
        throw std::bad_function_call();
    }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override {
        throw std::bad_function_call();
    }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override {
        throw std::bad_function_call();
    }
};

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
        sslog.debug(err.c_str());
        throw std::runtime_error(err);
    }
    auto coordinator = sr->get_coordinator();
    if (!coordinator) {
        auto err = sprint("[Stream #%s] GOT %s from %s: Can not find coordinator", plan_id, verb, from);
        sslog.debug(err.c_str());
        throw std::runtime_error(err);
    }
    return coordinator->get_or_create_session(from);
}

static future<bool> check_view_build_ongoing(db::system_distributed_keyspace& sys_dist_ks, const sstring& ks_name, const sstring& cf_name) {
    return sys_dist_ks.view_status(ks_name, cf_name).then([] (std::unordered_map<utils::UUID, sstring>&& view_statuses) {
        return boost::algorithm::any_of(view_statuses | boost::adaptors::map_values, [] (const sstring& view_status) {
            return view_status == "STARTED";
        });
    });
}

static future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const table& t, stream_reason reason) {
    if (is_internal_keyspace(t.schema()->ks_name())) {
        return make_ready_future<bool>(false);
    }
    if (reason == stream_reason::repair && !t.views().empty()) {
        return make_ready_future<bool>(true);
    }
    return do_with(t.views(), [&sys_dist_ks] (auto& views) {
        return map_reduce(views,
                [&sys_dist_ks] (const view_ptr& view) { return check_view_build_ongoing(sys_dist_ks, view->ks_name(), view->cf_name()); },
                false,
                std::logical_or<bool>());
    });
}

void stream_session::init_messaging_service_handler() {
    ms().register_prepare_message([] (const rpc::client_info& cinfo, prepare_message msg, UUID plan_id, sstring description, rpc::optional<stream_reason> reason_opt) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto dst_cpu_id = engine().cpu_id();
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
    ms().register_prepare_done_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [plan_id, from] () mutable {
            auto session = get_session(plan_id, from, "PREPARE_DONE_MESSAGE");
            session->follower_start_sent();
            return make_ready_future<>();
        });
    });
    ms().register_stream_mutation([] (const rpc::client_info& cinfo, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id, rpc::optional<bool> fragmented_opt, rpc::optional<stream_reason> reason_opt) {
        auto from = netw::messaging_service::get_source(cinfo);
        auto src_cpu_id = from.cpu_id;
        auto fragmented = fragmented_opt && *fragmented_opt;
        auto reason = reason_opt ? *reason_opt: stream_reason::unspecified;
        sslog.trace("Got stream_mutation from {} reason {}", from, int(reason));
        return smp::submit_to(src_cpu_id % smp::count, [fm = std::move(fm), plan_id, from, fragmented] () mutable {
            return do_with(std::move(fm), [plan_id, from, fragmented] (const auto& fm) {
                auto fm_size = fm.representation().size();
                get_local_stream_manager().update_progress(plan_id, from.addr, progress_info::direction::IN, fm_size);
                return service::get_schema_for_write(fm.schema_version(), from).then([plan_id, from, &fm, fragmented] (schema_ptr s) {
                    auto cf_id = fm.column_family_id();
                    sslog.debug("[Stream #{}] GOT STREAM_MUTATION from {}: cf_id={}", plan_id, from.addr, cf_id);

                    auto& db = service::get_local_storage_proxy().get_db().local();
                    if (!db.column_family_exists(cf_id)) {
                        sslog.warn("[Stream #{}] STREAM_MUTATION from {}: cf_id={} is missing, assume the table is dropped",
                                   plan_id, from.addr, cf_id);
                        return make_ready_future<>();
                    }
                    return service::get_storage_proxy().local().mutate_streaming_mutation(std::move(s), plan_id, fm, fragmented).then_wrapped([plan_id, cf_id, from] (auto&& f) {
                        try {
                            f.get();
                            return make_ready_future<>();
                        } catch (no_such_column_family&) {
                            sslog.warn("[Stream #{}] STREAM_MUTATION from {}: cf_id={} is missing, assume the table is dropped",
                                       plan_id, from.addr, cf_id);
                            return make_ready_future<>();
                        } catch (...) {
                            throw;
                        }
                        return make_ready_future<>();
                    });
                });
            });
        });
    });
    ms().register_stream_mutation_fragments([] (const rpc::client_info& cinfo, UUID plan_id, UUID schema_id, UUID cf_id, uint64_t estimated_partitions, rpc::optional<stream_reason> reason_opt, rpc::source<frozen_mutation_fragment> source) {
        auto from = netw::messaging_service::get_source(cinfo);
        auto reason = reason_opt ? *reason_opt: stream_reason::unspecified;
        sslog.trace("Got stream_mutation_fragments from {} reason {}", from, int(reason));
        return with_scheduling_group(service::get_local_storage_service().db().local().get_streaming_scheduling_group(), [from, estimated_partitions, plan_id, schema_id, cf_id, source, reason] () mutable {
                return service::get_schema_for_write(schema_id, from).then([from, estimated_partitions, plan_id, schema_id, cf_id, source, reason] (schema_ptr s) mutable {
                    auto sink = ms().make_sink_for_stream_mutation_fragments(source);
                    auto get_next_mutation_fragment = [source, plan_id, from, s] () mutable {
                        return source().then([plan_id, from, s] (stdx::optional<std::tuple<frozen_mutation_fragment>> fmf_opt) mutable {
                            if (fmf_opt) {
                                frozen_mutation_fragment& fmf = std::get<0>(fmf_opt.value());
                                auto sz = fmf.representation().size();
                                auto mf = fmf.unfreeze(*s);
                                streaming::get_local_stream_manager().update_progress(plan_id, from.addr, progress_info::direction::IN, sz);
                                return make_ready_future<mutation_fragment_opt>(std::move(mf));
                            } else {
                                return make_ready_future<mutation_fragment_opt>();
                            }
                        });
                    };
                    distribute_reader_and_consume_on_shards(s, dht::global_partitioner(),
                        make_flat_mutation_reader<generating_reader>(s, std::move(get_next_mutation_fragment)),
                        [cf_id, plan_id, estimated_partitions, reason] (flat_mutation_reader reader) {
                            auto& cf = service::get_local_storage_service().db().local().find_column_family(cf_id);

                            return check_needs_view_update_path(_sys_dist_ks->local(), cf, reason).then([cf = cf.shared_from_this(), estimated_partitions, reader = std::move(reader)] (bool use_view_update_path) mutable {
                                sstables::shared_sstable sst = use_view_update_path ? cf->make_streaming_staging_sstable() : cf->make_streaming_sstable_for_write();
                                schema_ptr s = reader.schema();
                                sstables::sstable_writer_config sst_cfg;
                                sst_cfg.large_partition_handler = cf->get_large_partition_handler();
                                auto& pc = service::get_local_streaming_write_priority();
                                return sst->write_components(std::move(reader), std::max(1ul, estimated_partitions), s, sst_cfg, {}, pc).then([sst] {
                                    return sst->open_data();
                                }).then([cf, sst] {
                                    return cf->add_sstable_and_update_cache(sst);
                                }).then([cf, s, sst, use_view_update_path]() mutable -> future<> {
                                    if (!use_view_update_path) {
                                        return make_ready_future<>();
                                    }
                                    return _view_update_generator->local().register_staging_sstable(sst, std::move(cf));
                                });
                            });
                        }
                    ).then_wrapped([s, plan_id, from, sink, estimated_partitions] (future<uint64_t> f) mutable {
                        int32_t status = 0;
                        uint64_t received_partitions = 0;
                        if (f.failed()) {
                            f.ignore_ready_future();
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
                        sslog.error("[Stream #{}] Failed to handle STREAM_MUTATION_FRAGMENTS for ks={}, cf={}, peer={}: {}", plan_id, s->ks_name(), s->cf_name(), from.addr, ep);
                    });
                    return make_ready_future<rpc::sink<int>>(sink);
                });
        });
    });
    ms().register_stream_mutation_done([] (const rpc::client_info& cinfo, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(dst_cpu_id, [ranges = std::move(ranges), plan_id, cf_id, from] () mutable {
            auto session = get_session(plan_id, from, "STREAM_MUTATION_DONE", cf_id);
            return session->get_db().invoke_on_all([ranges = std::move(ranges), plan_id, from, cf_id] (database& db) {
                if (!db.column_family_exists(cf_id)) {
                    sslog.warn("[Stream #{}] STREAM_MUTATION_DONE from {}: cf_id={} is missing, assume the table is dropped",
                                plan_id, from, cf_id);
                    return make_ready_future<>();
                }
                dht::partition_range_vector query_ranges;
                try {
                    auto& cf = db.find_column_family(cf_id);
                    query_ranges.reserve(ranges.size());
                    for (auto& range : ranges) {
                        query_ranges.push_back(dht::to_partition_range(range));
                    }
                    return cf.flush_streaming_mutations(plan_id, std::move(query_ranges));
                } catch (no_such_column_family&) {
                    sslog.warn("[Stream #{}] STREAM_MUTATION_DONE from {}: cf_id={} is missing, assume the table is dropped",
                                plan_id, from, cf_id);
                    return make_ready_future<>();
                } catch (...) {
                    throw;
                }
            }).then([session, cf_id] {
                session->receive_task_completed(cf_id);
            });
        });
    });
    ms().register_complete_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed) {
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

distributed<database>* stream_session::_db;
distributed<db::system_distributed_keyspace>* stream_session::_sys_dist_ks;
distributed<db::view::view_update_from_staging_generator>* stream_session::_view_update_generator;

stream_session::stream_session() = default;

stream_session::stream_session(inet_address peer_)
    : peer(peer_) {
    //this.metrics = StreamingMetrics.get(connecting);
}

stream_session::~stream_session() = default;

future<> stream_session::init_streaming_service(distributed<database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks, distributed<db::view::view_update_from_staging_generator>& view_update_generator) {
    _db = &db;
    _sys_dist_ks = &sys_dist_ks;
    _view_update_generator = &view_update_generator;
    // #293 - do not stop anything
    // engine().at_exit([] {
    //     return get_stream_manager().stop();
    // });
    return get_stream_manager().start().then([] {
        gms::get_local_gossiper().register_(get_local_stream_manager().shared_from_this());
        return _db->invoke_on_all([] (auto& db) {
            init_messaging_service_handler();
        });
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
    sslog.info("[Stream #{}] Aborted stream session={}, peer={}, is_initialized={}", plan_id(), this, peer, is_initialized());
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
                auto err = sprint("[Stream #{}] prepare requested ks={} cf={} does not exist", ks, cf);
                sslog.warn(err.c_str());
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
            auto err = sprint("[Stream #{}] prepare cf_id=%s does not exist", plan_id, cf_id);
            sslog.warn(err.c_str());
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
    this->ms().send_complete_message(id, plan_id, this->dst_cpu_id, failed).then([session, id, plan_id] {
        sslog.debug("[Stream #{}] GOT COMPLETE_MESSAGE Reply from {}", plan_id, id.addr);
    }).handle_exception([session, id, plan_id] (auto ep) {
        sslog.debug("[Stream #{}] COMPLETE_MESSAGE for {} has failed: {}", plan_id, id.addr, ep);
    });
}

bool stream_session::maybe_completed() {
    bool completed = _receivers.empty() && _transfers.empty();
    if (completed) {
        sslog.debug("[Stream #{}] maybe_completed: {} -> COMPLETE: session={}, peer={}", plan_id(), _state, this, peer);
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
    do_for_each(_transfers.begin(), _transfers.end(), [this] (auto& item) {
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
    return get_db().invoke_on_all([cf_id, plan_id = plan_id()] (database& db) {
        try {
            auto& cf = db.find_column_family(cf_id);
            return cf.fail_streaming_mutations(plan_id);
        } catch (no_such_column_family&) {
            return make_ready_future<>();
        }
    });
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
                receiving_failed(x.first);
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
    auto connecting = netw::get_local_messaging_service().get_preferred_ip(peer);
    if (peer == connecting) {
        sslog.debug("[Stream #{}] Starting streaming to {}", plan_id(), peer);
    } else {
        sslog.debug("[Stream #{}] Starting streaming to {} through {}", plan_id(), peer, connecting);
    }
    on_initialization_complete().handle_exception([this] (auto ep) {
        this->on_error();
    });
}

bool stream_session::is_initialized() const {
    return bool(_stream_result);
}

void stream_session::init(shared_ptr<stream_result_future> stream_result_) {
    _stream_result = stream_result_;
    _keep_alive.set_callback([this] {
        auto plan_id = this->plan_id();
        auto peer = this->peer;
        get_local_stream_manager().get_progress_on_all_shards(plan_id, peer).then([this, peer, plan_id] (stream_bytes sbytes) {
            if (this->_is_aborted) {
                sslog.info("[Stream #{}] The session {} is closed, keep alive timer will do nothing", plan_id, this);
                return;
            }
            auto now = lowres_clock::now();
            sslog.debug("[Stream #{}] keep alive timer callback sbytes old: tx={}, rx={} new: tx={} rx={}",
                    plan_id, this->_last_stream_bytes.bytes_sent, this->_last_stream_bytes.bytes_received,
                    sbytes.bytes_sent, sbytes.bytes_received);
            if (sbytes.bytes_sent > this->_last_stream_bytes.bytes_sent ||
                sbytes.bytes_received > this->_last_stream_bytes.bytes_received) {
                sslog.debug("[Stream #{}] The session {} made progress with peer {}", plan_id, this, peer);
                // Progress has been made
                this->_last_stream_bytes = sbytes;
                this->_last_stream_progress = now;
                this->start_keep_alive_timer();
            } else if (now - this->_last_stream_progress >= this->_keep_alive_timeout) {
                // Timeout
                sslog.info("[Stream #{}] The session {} is idle for {} seconds, the peer {} is probably gone, close it",
                        plan_id, this, this->_keep_alive_timeout.count(), peer);
                this->on_error();
            } else {
                // Start the timer to check again
                sslog.info("[Stream #{}] The session {} made no progress with peer {}", plan_id, this, peer);
                this->start_keep_alive_timer();
            }
        }).handle_exception([plan_id, peer, session = this->shared_from_this()] (auto ep) {
           sslog.info("[Stream #{}] keep alive timer callback fails with peer {}: {}", plan_id, peer, ep);
        });
    });
    _last_stream_progress = lowres_clock::now();
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
