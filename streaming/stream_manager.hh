/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once
#include "streaming/stream_fwd.hh"
#include "streaming/progress_info.hh"
#include "streaming/stream_reason.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include "utils/updateable_value.hh"
#include "utils/serialized_action.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "service/topology_guard.hh"
#include "readers/mutation_reader.hh"
#include <seastar/core/semaphore.hh>
#include <seastar/core/metrics_registration.hh>

namespace db {
class config;
namespace view {
class view_builder;
}
}

namespace service {
class migration_manager;
};

namespace netw {
class messaging_service;
};

namespace gms {
class gossiper;
}

namespace replica {
class database;
}

namespace streaming {

struct stream_bytes {
    int64_t bytes_sent = 0;
    int64_t bytes_received = 0;
    friend stream_bytes operator+(const stream_bytes& x, const stream_bytes& y) {
        stream_bytes ret(x);
        ret += y;
        return ret;
    }
    stream_bytes& operator+=(const stream_bytes& x) {
        bytes_sent += x.bytes_sent;
        bytes_received += x.bytes_received;
        return *this;
    }
};

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
class stream_manager : public gms::i_endpoint_state_change_subscriber, public enable_shared_from_this<stream_manager>, public peering_sharded_service<stream_manager> {
    using inet_address = gms::inet_address;
    using endpoint_state = gms::endpoint_state;
    using endpoint_state_ptr = gms::endpoint_state_ptr;
    using application_state = gms::application_state;
    using versioned_value = gms::versioned_value;
    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones within the same JVM.
     */
private:
    sharded<replica::database>& _db;
    sharded<db::view::view_builder>& _view_builder;
    sharded<netw::messaging_service>& _ms;
    sharded<service::migration_manager>& _mm;
    gms::gossiper& _gossiper;

    std::unordered_map<plan_id, shared_ptr<stream_result_future>> _initiated_streams;
    std::unordered_map<plan_id, shared_ptr<stream_result_future>> _receiving_streams;
    std::unordered_map<plan_id, std::unordered_map<gms::inet_address, stream_bytes>> _stream_bytes;
    uint64_t _total_incoming_bytes{0};
    uint64_t _total_outgoing_bytes{0};
    semaphore _mutation_send_limiter{256};
    seastar::metrics::metric_groups _metrics;
    std::unordered_map<streaming::stream_reason, float> _finished_percentage;

    scheduling_group _streaming_group;
    utils::updateable_value<uint32_t> _io_throughput_mbs;
    serialized_action _io_throughput_updater = serialized_action([this] { return update_io_throughput(_io_throughput_mbs()); });
    std::optional<utils::observer<uint32_t>> _io_throughput_option_observer;

public:
    stream_manager(db::config& cfg, sharded<replica::database>& db,
            sharded<db::view::view_builder>& view_builder,
            sharded<netw::messaging_service>& ms,
            sharded<service::migration_manager>& mm,
            gms::gossiper& gossiper, scheduling_group sg);

    future<> start(abort_source& as);
    future<> stop();

    semaphore& mutation_send_limiter() { return _mutation_send_limiter; }

    void register_sending(shared_ptr<stream_result_future> result);

    void register_receiving(shared_ptr<stream_result_future> result);

    shared_ptr<stream_result_future> get_sending_stream(streaming::plan_id plan_id) const;

    shared_ptr<stream_result_future> get_receiving_stream(streaming::plan_id plan_id) const;

    std::vector<shared_ptr<stream_result_future>> get_all_streams() const;

    replica::database& db() noexcept { return _db.local(); }
    netw::messaging_service& ms() noexcept { return _ms.local(); }
    service::migration_manager& mm() noexcept { return _mm.local(); }

    const std::unordered_map<plan_id, shared_ptr<stream_result_future>>& get_initiated_streams() const {
        return _initiated_streams;
    }

    const std::unordered_map<plan_id, shared_ptr<stream_result_future>>& get_receiving_streams() const {
        return _receiving_streams;
    }

    void remove_stream(streaming::plan_id plan_id);

    void show_streams() const;

    future<> shutdown() {
        fail_all_sessions();
        return make_ready_future<>();
    }

    void update_progress(streaming::plan_id plan_id, gms::inet_address peer, progress_info::direction dir, size_t fm_size);
    future<> update_all_progress_info();

    void remove_progress(streaming::plan_id plan_id);

    stream_bytes get_progress(streaming::plan_id plan_id, gms::inet_address peer) const;

    stream_bytes get_progress(streaming::plan_id plan_id) const;

    future<> remove_progress_on_all_shards(streaming::plan_id plan_id);

    future<stream_bytes> get_progress_on_all_shards(streaming::plan_id plan_id, gms::inet_address peer) const;

    future<stream_bytes> get_progress_on_all_shards(streaming::plan_id plan_id) const;

    future<stream_bytes> get_progress_on_all_shards(gms::inet_address peer) const;

    future<stream_bytes> get_progress_on_all_shards() const;

    stream_bytes get_progress_on_local_shard() const;

    shared_ptr<stream_session> get_session(streaming::plan_id plan_id, gms::inet_address from, const char* verb, std::optional<table_id> cf_id = {});

    std::function<future<>(mutation_reader)> make_streaming_consumer(
            uint64_t estimated_partitions, stream_reason, service::frozen_topology_guard);
public:
    virtual future<> on_join(inet_address endpoint, endpoint_state_ptr ep_state, gms::permit_id) override { return make_ready_future(); }
    virtual future<> on_change(gms::inet_address, const gms::application_state_map& states, gms::permit_id) override  { return make_ready_future(); }
    virtual future<> on_alive(inet_address endpoint, endpoint_state_ptr state, gms::permit_id) override { return make_ready_future(); }
    virtual future<> on_dead(inet_address endpoint, endpoint_state_ptr state, gms::permit_id) override;
    virtual future<> on_remove(inet_address endpoint, gms::permit_id) override;
    virtual future<> on_restart(inet_address endpoint, endpoint_state_ptr ep_state, gms::permit_id) override;

private:
    void fail_all_sessions();
    void fail_sessions(inet_address endpoint);
    bool has_peer(inet_address endpoint) const;

    void init_messaging_service_handler(abort_source& as);
    future<> uninit_messaging_service_handler();
    future<> update_io_throughput(uint32_t value_mbs);

public:
    void update_finished_percentage(streaming::stream_reason reason, float percentage);
};

} // namespace streaming
