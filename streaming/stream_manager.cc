/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/distributed.hh>
#include "gms/gossiper.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_result_future.hh"
#include "log.hh"
#include "streaming/stream_session_state.hh"
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include "db/config.hh"

namespace streaming {

extern logging::logger sslog;

stream_manager::stream_manager(db::config& cfg,
            sharded<replica::database>& db,
            sharded<db::view::view_builder>& view_builder,
            sharded<netw::messaging_service>& ms,
            sharded<service::migration_manager>& mm,
            gms::gossiper& gossiper, scheduling_group sg)
        : _db(db)
        , _view_builder(view_builder)
        , _ms(ms)
        , _mm(mm)
        , _gossiper(gossiper)
        , _streaming_group(std::move(sg))
        , _io_throughput_mbs(cfg.stream_io_throughput_mb_per_sec)
{
    namespace sm = seastar::metrics;

    if (this_shard_id() == 0) {
        _io_throughput_option_observer.emplace(_io_throughput_mbs.observe(_io_throughput_updater.make_observer()));
        (void)_io_throughput_updater.trigger_later();
    }

    _finished_percentage[streaming::stream_reason::bootstrap] = 1;
    _finished_percentage[streaming::stream_reason::decommission] = 1;
    _finished_percentage[streaming::stream_reason::removenode] = 1;
    _finished_percentage[streaming::stream_reason::rebuild] = 1;
    _finished_percentage[streaming::stream_reason::repair] = 1;
    _finished_percentage[streaming::stream_reason::replace] = 1;

    auto ops_label_type = sm::label("ops");
    _metrics.add_group("streaming", {
        sm::make_counter("total_incoming_bytes", [this] { return _total_incoming_bytes; },
                        sm::description("Total number of bytes received on this shard.")),

        sm::make_counter("total_outgoing_bytes", [this] { return _total_outgoing_bytes; },
                        sm::description("Total number of bytes sent on this shard.")),

        sm::make_gauge("finished_percentage", [this] { return _finished_percentage[streaming::stream_reason::bootstrap]; },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("bootstrap")}),

        sm::make_gauge("finished_percentage", [this] { return _finished_percentage[streaming::stream_reason::decommission]; },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("decommission")}),

        sm::make_gauge("finished_percentage", [this] { return _finished_percentage[streaming::stream_reason::removenode]; },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("removenode")}),

        sm::make_gauge("finished_percentage", [this] { return _finished_percentage[streaming::stream_reason::rebuild]; },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("rebuild")}),

        sm::make_gauge("finished_percentage", [this] { return _finished_percentage[streaming::stream_reason::repair]; },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("repair")}),

        sm::make_gauge("finished_percentage", [this] { return _finished_percentage[streaming::stream_reason::replace]; },
                sm::description("Finished percentage of node operation on this shard"), {ops_label_type("replace")}),
    });
}

future<> stream_manager::start(abort_source& as) {
    _gossiper.register_(shared_from_this());
    init_messaging_service_handler(as);
    return make_ready_future<>();
}

future<> stream_manager::stop() {
    co_await _gossiper.unregister_(shared_from_this());
    co_await uninit_messaging_service_handler();
    co_await _io_throughput_updater.join();
}

future<> stream_manager::update_io_throughput(uint32_t value_mbs) {
    uint64_t bps = ((uint64_t)(value_mbs != 0 ? value_mbs : std::numeric_limits<uint32_t>::max())) << 20;
    return _streaming_group.update_io_bandwidth(bps).then_wrapped([value_mbs] (auto f) {
        if (f.failed()) {
            sslog.warn("Couldn't update streaming bandwidth: {}", f.get_exception());
        } else if (value_mbs != 0) {
            sslog.info("Set streaming bandwidth to {}MB/s", value_mbs);
        } else {
            sslog.info("Set unlimited streaming bandwidth");
        }
    });
}

void stream_manager::register_sending(shared_ptr<stream_result_future> result) {
#if 0
    result.addEventListener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            initiatedStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _initiated_streams[result->plan_id] = std::move(result);
}

void stream_manager::register_receiving(shared_ptr<stream_result_future> result) {
#if 0
    result->add_event_listener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            receivingStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _receiving_streams[result->plan_id] = std::move(result);
}

shared_ptr<stream_result_future> stream_manager::get_sending_stream(streaming::plan_id plan_id) const {
    auto it = _initiated_streams.find(plan_id);
    if (it != _initiated_streams.end()) {
        return it->second;
    }
    return {};
}

shared_ptr<stream_result_future> stream_manager::get_receiving_stream(streaming::plan_id plan_id) const {
    auto it = _receiving_streams.find(plan_id);
    if (it != _receiving_streams.end()) {
        return it->second;
    }
    return {};
}

void stream_manager::remove_stream(streaming::plan_id plan_id) {
    sslog.debug("stream_manager: removing plan_id={}", plan_id);
    _initiated_streams.erase(plan_id);
    _receiving_streams.erase(plan_id);
    // FIXME: Do not ignore the future
    (void)remove_progress_on_all_shards(plan_id).handle_exception([plan_id] (auto ep) {
        sslog.info("stream_manager: Fail to remove progress for plan_id={}: {}", plan_id, ep);
    });
}

void stream_manager::show_streams() const {
    for (auto const& x : _initiated_streams) {
        sslog.debug("stream_manager:initiated_stream: plan_id={}", x.first);
    }
    for (auto const& x : _receiving_streams) {
        sslog.debug("stream_manager:receiving_stream: plan_id={}", x.first);
    }
}

std::vector<shared_ptr<stream_result_future>> stream_manager::get_all_streams() const {
    std::vector<shared_ptr<stream_result_future>> result;
    for (auto& x : _initiated_streams) {
        result.push_back(x.second);
    }
    for (auto& x : _receiving_streams) {
        result.push_back(x.second);
    }
    return result;
}

void stream_manager::update_progress(streaming::plan_id plan_id, gms::inet_address peer, progress_info::direction dir, size_t fm_size) {
    auto& sbytes = _stream_bytes[plan_id];
    if (dir == progress_info::direction::OUT) {
        sbytes[peer].bytes_sent += fm_size;
        _total_outgoing_bytes += fm_size;
    } else {
        sbytes[peer].bytes_received += fm_size;
        _total_incoming_bytes += fm_size;
    }
}

future<> stream_manager::update_all_progress_info() {
    return seastar::async([this] {
        for (auto sr: get_all_streams()) {
            for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
                session->update_progress().get();
            }
        }
    });
}

void stream_manager::remove_progress(streaming::plan_id plan_id) {
    _stream_bytes.erase(plan_id);
}

stream_bytes stream_manager::get_progress(streaming::plan_id plan_id, gms::inet_address peer) const {
    auto it = _stream_bytes.find(plan_id);
    if (it == _stream_bytes.end()) {
        return stream_bytes();
    }
    auto const& sbytes = it->second;
    auto i = sbytes.find(peer);
    if (i == sbytes.end()) {
        return stream_bytes();
    }
    return i->second;
}

stream_bytes stream_manager::get_progress(streaming::plan_id plan_id) const {
    auto it = _stream_bytes.find(plan_id);
    if (it == _stream_bytes.end()) {
        return stream_bytes();
    }
    stream_bytes ret;
    for (auto const& x : it->second) {
        ret += x.second;
    }
    return ret;
}

future<> stream_manager::remove_progress_on_all_shards(streaming::plan_id plan_id) {
    return container().invoke_on_all([plan_id] (auto& sm) {
        sm.remove_progress(plan_id);
    });
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(streaming::plan_id plan_id, gms::inet_address peer) const {
    return container().map_reduce0(
        [plan_id, peer] (auto& sm) {
            return sm.get_progress(plan_id, peer);
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(streaming::plan_id plan_id) const {
    return container().map_reduce0(
        [plan_id] (auto& sm) {
            return sm.get_progress(plan_id);
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(gms::inet_address peer) const {
    return container().map_reduce0(
        [peer] (auto& sm) {
            stream_bytes ret;
            for (auto& sbytes : sm._stream_bytes) {
                if (sbytes.second.contains(peer)) {
                    ret += sbytes.second.at(peer);
                }
            }
            return ret;
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards() const {
    return container().map_reduce0(
        [] (auto& sm) {
            stream_bytes ret;
            for (auto& sbytes : sm._stream_bytes) {
                for (auto& sb : sbytes.second) {
                    ret += sb.second;
                }
            }
            return ret;
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

stream_bytes stream_manager::get_progress_on_local_shard() const {
    stream_bytes ret;
    for (auto const& sbytes : _stream_bytes) {
        for (auto const& sb : sbytes.second) {
            ret += sb.second;
        }
    }
    return ret;
}

bool stream_manager::has_peer(inet_address endpoint) const {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            if (session->peer == endpoint) {
                return true;
            }
        }
    }
    return false;
}

void stream_manager::fail_sessions(inet_address endpoint) {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            if (session->peer == endpoint) {
                session->close_session(stream_session_state::FAILED);
            }
        }
    }
}

void stream_manager::fail_all_sessions() {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            session->close_session(stream_session_state::FAILED);
        }
    }
}

future<> stream_manager::on_remove(inet_address endpoint, gms::permit_id) {
    if (has_peer(endpoint)) {
        sslog.info("stream_manager: Close all stream_session with peer = {} in on_remove", endpoint);
        //FIXME: discarded future.
        (void)container().invoke_on_all([endpoint] (auto& sm) {
            sm.fail_sessions(endpoint);
        }).handle_exception([endpoint] (auto ep) {
            sslog.warn("stream_manager: Fail to close sessions peer = {} in on_remove", endpoint);
        });
    }
    return make_ready_future();
}

future<> stream_manager::on_restart(inet_address endpoint, endpoint_state_ptr ep_state, gms::permit_id) {
    if (has_peer(endpoint)) {
        sslog.info("stream_manager: Close all stream_session with peer = {} in on_restart", endpoint);
        //FIXME: discarded future.
        (void)container().invoke_on_all([endpoint] (auto& sm) {
            sm.fail_sessions(endpoint);
        }).handle_exception([endpoint] (auto ep) {
            sslog.warn("stream_manager: Fail to close sessions peer = {} in on_restart", endpoint);
        });
    }
    return make_ready_future();
}

future<> stream_manager::on_dead(inet_address endpoint, endpoint_state_ptr ep_state, gms::permit_id) {
    if (has_peer(endpoint)) {
        sslog.info("stream_manager: Close all stream_session with peer = {} in on_dead", endpoint);
        //FIXME: discarded future.
        (void)container().invoke_on_all([endpoint] (auto& sm) {
            sm.fail_sessions(endpoint);
        }).handle_exception([endpoint] (auto ep) {
            sslog.warn("stream_manager: Fail to close sessions peer = {} in on_dead", endpoint);
        });
    }
    return make_ready_future();
}

shared_ptr<stream_session> stream_manager::get_session(streaming::plan_id plan_id, gms::inet_address from, const char* verb, std::optional<table_id> cf_id) {
    if (cf_id) {
        sslog.debug("[Stream #{}] GOT {} from {}: cf_id={}", plan_id, verb, from, *cf_id);
    } else {
        sslog.debug("[Stream #{}] GOT {} from {}", plan_id, verb, from);
    }
    auto sr = get_sending_stream(plan_id);
    if (!sr) {
        sr = get_receiving_stream(plan_id);
    }
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
    return coordinator->get_or_create_session(*this, from);
}

void stream_manager::update_finished_percentage(streaming::stream_reason reason, float percentage) {
    _finished_percentage[reason] = percentage;
}

} // namespace streaming
