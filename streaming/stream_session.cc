/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "log.hh"
#include "message/messaging_service.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include "streaming/stream_session.hh"
#include "streaming/prepare_message.hh"
#include "streaming/stream_result_future.hh"
#include "streaming/stream_manager.hh"
#include "dht/auto_refreshing_sharder.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include "streaming/stream_session_state.hh"
#include "service/migration_manager.hh"
#include "mutation_writer/multishard_writer.hh"
#include "sstables/sstable_set.hh"
#include "db/view/view_update_checks.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/map.hpp>
#include "replica/database.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "consumer.hh"
#include "readers/generating_v2.hh"
#include "service/topology_guard.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"

namespace streaming {

logging::logger sslog("stream_session");

static sstables::offstrategy is_offstrategy_supported(streaming::stream_reason reason) {
    static const std::unordered_set<streaming::stream_reason> operations_supported = {
        streaming::stream_reason::bootstrap,
        streaming::stream_reason::replace,
        streaming::stream_reason::removenode,
        streaming::stream_reason::decommission,
        streaming::stream_reason::repair,
        streaming::stream_reason::rebuild,
    };
    return sstables::offstrategy(operations_supported.contains(reason));
}

class offstrategy_trigger {
    sharded<replica::database>& _db;
    table_id _id;
    streaming::plan_id _plan_id;
    replica::column_family& _cf;
    lowres_clock::time_point last_update;

public:
    offstrategy_trigger(sharded<replica::database>& db, table_id id, streaming::plan_id plan_id)
        : _db(db)
        , _id(id)
        , _plan_id(plan_id)
        , _cf(_db.local().find_column_family(_id))
    {
        _cf.enable_off_strategy_trigger();
    }
    void update() {
        auto now = lowres_clock::now();
        // Call update_off_strategy_trigger at most every 30s. In the worst
        // case, we would shorten the offstrategy trigger timer by 10%. The
        // reward is that we now batch thousands or even millions of calls to
        // update_off_strategy_trigger. The update() is called for each and
        // every mutation fragment we received. So it is worth the
        // optimization.
        if (now - last_update > std::chrono::seconds(30)) {
            sslog.debug("[Stream #{}] Updated offstrategy trigger for ks={}, table={}, table_id={}",
                _plan_id, _cf.schema()->ks_name(), _cf.schema()->cf_name(), _id);
            _cf.update_off_strategy_trigger();
            last_update = now;
        }
    }
};

std::function<future<>(mutation_reader)>
stream_manager::make_streaming_consumer(uint64_t estimated_partitions, stream_reason reason, service::frozen_topology_guard topo_guard) {
    return streaming::make_streaming_consumer("streaming", _db, _view_builder, estimated_partitions, reason, is_offstrategy_supported(reason), topo_guard);
}

void stream_manager::init_messaging_service_handler(abort_source& as) {
    auto& ms = _ms.local();

    ms.register_prepare_message([this] (const rpc::client_info& cinfo, prepare_message msg, streaming::plan_id plan_id, sstring description, rpc::optional<stream_reason> reason_opt, rpc::optional<service::session_id> session) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto dst_cpu_id = this_shard_id();
        auto reason = reason_opt ? *reason_opt : stream_reason::unspecified;
        auto topo_guard = service::frozen_topology_guard(session.value_or(service::default_session_id));
        return container().invoke_on(dst_cpu_id, [msg = std::move(msg), plan_id, description = std::move(description), from, src_cpu_id, reason, topo_guard] (auto& sm) mutable {
            auto sr = stream_result_future::init_receiving_side(sm, plan_id, description, from);
            auto session = sm.get_session(plan_id, from, "PREPARE_MESSAGE");
            session->init(sr);
            session->dst_cpu_id = src_cpu_id;
            session->set_reason(reason);
            session->set_topo_guard(topo_guard);
            return session->prepare(std::move(msg.requests), std::move(msg.summaries));
        });
    });
    ms.register_prepare_done_message([this] (const rpc::client_info& cinfo, streaming::plan_id plan_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(dst_cpu_id, [plan_id, from] (auto& sm) mutable {
            auto session = sm.get_session(plan_id, from, "PREPARE_DONE_MESSAGE");
            session->follower_start_sent();
            return make_ready_future<>();
        });
    });
    ms.register_stream_mutation_fragments([this, &as] (const rpc::client_info& cinfo, streaming::plan_id plan_id, table_schema_version schema_id, table_id cf_id, uint64_t estimated_partitions,
            rpc::optional<stream_reason> reason_opt,
            rpc::source<frozen_mutation_fragment, rpc::optional<stream_mutation_fragments_cmd>> source,
            rpc::optional<service::session_id> session) {
        auto from = netw::messaging_service::get_source(cinfo);
        auto reason = reason_opt ? *reason_opt: stream_reason::unspecified;
        service::frozen_topology_guard topo_guard = session.value_or(service::default_session_id);
        sslog.trace("Got stream_mutation_fragments from {} reason {}, session {}", from, int(reason), session);
        if (!_view_builder.local_is_initialized()) {
            return make_exception_future<rpc::sink<int>>(std::runtime_error(format("Node {} is not fully initialized for streaming, try again later",
                    _db.local().get_token_metadata().get_topology().my_address())));
        }
        return _mm.local().get_schema_for_write(schema_id, from, _ms.local(), as).then([this, from, estimated_partitions, plan_id, cf_id, source, reason, topo_guard, &as] (schema_ptr s) mutable {
          return _db.local().obtain_reader_permit(s, "stream-session", db::no_timeout, {}).then([this, from, estimated_partitions, plan_id, cf_id, source, reason, topo_guard, s, &as] (reader_permit permit) mutable {
            struct stream_mutation_fragments_cmd_status {
                bool got_cmd = false;
                bool got_end_of_stream = false;
            };
            auto cmd_status = make_lw_shared<stream_mutation_fragments_cmd_status>();
            auto offstrategy_update = make_lw_shared<offstrategy_trigger>(_db, cf_id, plan_id);
            auto guard = service::topology_guard(topo_guard);

            // Will log a message when streaming is done. Used to synchronize tests.
            lw_shared_ptr<std::any> log_done;
            if (utils::get_local_injector().is_enabled("stream_mutation_fragments")) {
                log_done = make_lw_shared<std::any>(seastar::make_shared(seastar::defer([] {
                    sslog.info("stream_mutation_fragments: done");
                })));
            }

            auto get_next_mutation_fragment = [guard = std::move(guard), &as, &sm = container(), source, plan_id, from, s, cmd_status, offstrategy_update, permit] () mutable {
                guard.check();
                return source().then([&sm, &guard, &as, plan_id, from, s, cmd_status, offstrategy_update, permit] (std::optional<std::tuple<frozen_mutation_fragment, rpc::optional<stream_mutation_fragments_cmd>>> opt) mutable {
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
                        sm.local().update_progress(plan_id, from.addr, progress_info::direction::IN, sz);
                        offstrategy_update->update();

                        return utils::get_local_injector().inject("stream_mutation_fragments", [&guard, &as] (auto& handler) -> future<> {
                            auto& guard_ = guard;
                            auto& as_ = as;
                            sslog.info("stream_mutation_fragments: waiting");
                            while (!handler.poll_for_message()) {
                                guard_.check();
                                co_await sleep_abortable(std::chrono::milliseconds(5), as_);
                            }
                            sslog.info("stream_mutation_fragments: released");
                        }).then([mf = std::move(mf)] () mutable {
                            return mutation_fragment_opt(std::move(mf));
                        });
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
          auto sink = _ms.local().make_sink_for_stream_mutation_fragments(source);
          try {
            // Make sure the table with cf_id is still present at this point.
            // Close the sink in case the table is dropped.
            auto& table = _db.local().find_column_family(cf_id);
            utils::get_local_injector().inject("stream_mutation_fragments_table_dropped", [this] () {
                _db.local().find_column_family(table_id::create_null_id());
            });
            auto op = table.stream_in_progress();
            auto sharder_ptr = std::make_unique<dht::auto_refreshing_sharder>(table.shared_from_this());
            auto& sharder = *sharder_ptr;
            //FIXME: discarded future.
            (void)mutation_writer::distribute_reader_and_consume_on_shards(s, sharder,
                make_generating_reader_v1(s, permit, std::move(get_next_mutation_fragment)),
                make_streaming_consumer(estimated_partitions, reason, topo_guard),
                std::move(op)
            ).then_wrapped([s, plan_id, from, sink, estimated_partitions, log_done, sh_ptr = std::move(sharder_ptr)] (future<uint64_t> f) mutable {
                int32_t status = 0;
                uint64_t received_partitions = 0;
                if (f.failed()) {
                    auto ex = f.get_exception();
                    auto level = seastar::log_level::error;
                    if (try_catch<seastar::rpc::stream_closed>(ex)) {
                        level = seastar::log_level::debug;
                    }
                    status = -1;
                    // The status code -2 means error and the table is dropped
                    if (try_catch<data_dictionary::no_such_column_family>(ex)) {
                        level = seastar::log_level::debug;
                        status = -2;
                    }
                    sslog.log(level, "[Stream #{}] Failed to handle STREAM_MUTATION_FRAGMENTS (receive and distribute phase) for ks={}, cf={}, peer={}: {}",
                            plan_id, s->ks_name(), s->cf_name(), from.addr, ex);
                } else {
                    received_partitions = f.get();
                }
                if (received_partitions) {
                    sslog.info("[Stream #{}] Write to sstable for ks={}, cf={}, estimated_partitions={}, received_partitions={}",
                            plan_id, s->ks_name(), s->cf_name(), estimated_partitions, received_partitions);
                }
                return sink(status).finally([sink] () mutable {
                    return sink.close();
                });
            }).handle_exception([s, plan_id, from, sink] (std::exception_ptr ep) {
                auto level = seastar::log_level::error;
                if (try_catch<seastar::rpc::closed_error>(ep)) {
                    level = seastar::log_level::debug;
                }
                sslog.log(level, "[Stream #{}] Failed to handle STREAM_MUTATION_FRAGMENTS (respond phase) for ks={}, cf={}, peer={}: {}",
                        plan_id, s->ks_name(), s->cf_name(), from.addr, ep);
            });
          } catch (...) {
            return sink.close().then([sink, eptr = std::current_exception()] () -> future<rpc::sink<int>> {
                return make_exception_future<rpc::sink<int>>(eptr);
            });
          }
            return make_ready_future<rpc::sink<int>>(sink);
        });
      });
    });
    ms.register_stream_mutation_done([this] (const rpc::client_info& cinfo, streaming::plan_id plan_id, dht::token_range_vector ranges, table_id cf_id, unsigned dst_cpu_id) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(dst_cpu_id, [ranges = std::move(ranges), plan_id, cf_id, from] (auto& sm) mutable {
            auto session = sm.get_session(plan_id, from, "STREAM_MUTATION_DONE", cf_id);
            session->receive_task_completed(cf_id);
        });
    });
    ms.register_complete_message([this] (const rpc::client_info& cinfo, streaming::plan_id plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed) {
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        if (failed && *failed) {
            return container().invoke_on(dst_cpu_id, [plan_id, from, dst_cpu_id] (auto& sm) {
                auto session = sm.get_session(plan_id, from, "COMPLETE_MESSAGE");
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

future<> stream_manager::uninit_messaging_service_handler() {
    auto& ms = _ms.local();
    return when_all_succeed(
        ms.unregister_prepare_message(),
        ms.unregister_prepare_done_message(),
        ms.unregister_stream_mutation_fragments(),
        ms.unregister_stream_mutation_done(),
        ms.unregister_complete_message()).discard_result();
}

stream_session::stream_session(stream_manager& mgr, inet_address peer_)
    : peer(peer_)
    , _mgr(mgr)
{
    //this.metrics = StreamingMetrics.get(connecting);
}

stream_session::~stream_session() = default;

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
    return manager().ms().send_prepare_message(id, std::move(prepare), plan_id(), description(), get_reason(), topo_guard()).then_wrapped([this, id] (auto&& f) {
        try {
            auto msg = f.get();
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
        return manager().ms().send_prepare_done_message(id, plan_id, this->dst_cpu_id).then([this] {
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
    auto nr_requests = requests.size();
    sslog.debug("[Stream #{}] prepare requests nr={}, summaries nr={}", plan_id, nr_requests, summaries.size());
    // prepare tasks
    set_state(stream_session_state::PREPARING);
    auto& db = manager().db();
    for (auto& request : requests) {
        // always flush on stream request
        sslog.debug("[Stream #{}] prepare stream_request={}", plan_id, request);
        const auto& ks = request.keyspace;
        // Make sure cf requested by peer node exists
        for (auto& cf : request.column_families) {
            try {
                db.find_column_family(ks, cf);
            } catch (replica::no_such_column_family&) {
                auto err = format("[Stream #{}] prepare requested ks={} cf={} does not exist", plan_id, ks, cf);
                sslog.warn("{}", err.c_str());
                throw std::runtime_error(err);
            }
        }
        add_transfer_ranges(std::move(request.keyspace), std::move(request.ranges), std::move(request.column_families));
        co_await coroutine::maybe_yield();
    }
    for (auto& summary : summaries) {
        sslog.debug("[Stream #{}] prepare stream_summary={}", plan_id, summary);
        auto cf_id = summary.cf_id;
        // Make sure cf the peer node will send to us exists
        try {
            db.find_column_family(cf_id);
        } catch (replica::no_such_column_family&) {
            auto err = format("[Stream #{}] prepare cf_id={} does not exist", plan_id, cf_id);
            sslog.warn("{}", err.c_str());
            throw std::runtime_error(err);
        }
        prepare_receiving(summary);
    }

    // Always send a prepare_message back to follower
    prepare_message prepare;
    if (nr_requests) {
        for (auto& x: _transfers) {
            auto& task = x.second;
            prepare.summaries.emplace_back(task.get_summary());
        }
    }
    prepare.dst_cpu_id = this_shard_id();
    if (_stream_result) {
        _stream_result->handle_session_prepared(shared_from_this());
    }
    co_return prepare;
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

void stream_session::receive_task_completed(table_id cf_id) {
    _receivers.erase(cf_id);
    sslog.debug("[Stream #{}] receive  task_completed: cf_id={} done, stream_receive_task.size={} stream_transfer_task.size={}",
        plan_id(), cf_id, _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::transfer_task_completed(table_id cf_id) {
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
    (void)manager().ms().send_complete_message(id, plan_id, this->dst_cpu_id, failed).then([session, id, plan_id] {
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

std::vector<replica::column_family*> stream_session::get_column_family_stores(const sstring& keyspace, const std::vector<sstring>& column_families) {
    // if columnfamilies are not specified, we add all cf under the keyspace
    std::vector<replica::column_family*> stores;
    auto& db = manager().db();
    if (column_families.empty()) {
        db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> tp) {
            replica::column_family& cf = *tp;
            auto cf_name = cf.schema()->cf_name();
            auto ks_name = cf.schema()->ks_name();
            if (ks_name == keyspace) {
                sslog.debug("Find ks={} cf={}", ks_name, cf_name);
                stores.push_back(&cf);
            }
        });
    } else {
        // TODO: We can move this to database class and use shared_ptr<column_family> instead
        for (auto& cf_name : column_families) {
            try {
                auto& x = db.find_column_family(keyspace, cf_name);
                stores.push_back(&x);
            } catch (replica::no_such_column_family&) {
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
            SCYLLA_ASSERT(inserted);
        } else {
            it->second.append_ranges(ranges);
        }
    }
}

future<> stream_session::receiving_failed(table_id cf_id)
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
    auto connecting = manager().ms().get_preferred_ip(peer);
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

streaming::plan_id stream_session::plan_id() const {
    return _stream_result ? _stream_result->plan_id : streaming::plan_id::create_null_id();
}

sstring stream_session::description() const {
    return _stream_result  ? _stream_result->description : "";
}

future<> stream_session::update_progress() {
    return manager().get_progress_on_all_shards(plan_id(), peer).then([this] (auto sbytes) {
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
