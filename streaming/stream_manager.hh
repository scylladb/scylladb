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

#pragma once
#include "streaming/progress_info.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include "utils/UUID.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include <seastar/core/semaphore.hh>
#include <seastar/core/metrics_registration.hh>
#include <map>

namespace db {
class system_distributed_keyspace;
namespace view {
class view_update_generator;
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

namespace streaming {

class stream_session;
class stream_result_future;

struct stream_bytes {
    int64_t bytes_sent = 0;
    int64_t bytes_received = 0;
    friend stream_bytes operator+(const stream_bytes& x, const stream_bytes& y) {
        stream_bytes ret(x);
        ret += y;
        return ret;
    }
    friend bool operator!=(const stream_bytes& x, const stream_bytes& y) {
        return x.bytes_sent != y.bytes_sent && x.bytes_received != y.bytes_received;
    }
    friend bool operator==(const stream_bytes& x, const stream_bytes& y) {
        return x.bytes_sent == y.bytes_sent && x.bytes_received == y.bytes_received;
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
    using UUID = utils::UUID;
    using inet_address = gms::inet_address;
    using endpoint_state = gms::endpoint_state;
    using application_state = gms::application_state;
    using versioned_value = gms::versioned_value;
    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
private:
    sharded<replica::database>& _db;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::view::view_update_generator>& _view_update_generator;
    sharded<netw::messaging_service>& _ms;
    sharded<service::migration_manager>& _mm;
    gms::gossiper& _gossiper;

    std::unordered_map<UUID, shared_ptr<stream_result_future>> _initiated_streams;
    std::unordered_map<UUID, shared_ptr<stream_result_future>> _receiving_streams;
    std::unordered_map<UUID, std::unordered_map<gms::inet_address, stream_bytes>> _stream_bytes;
    uint64_t _total_incoming_bytes{0};
    uint64_t _total_outgoing_bytes{0};
    semaphore _mutation_send_limiter{256};
    seastar::metrics::metric_groups _metrics;

public:
    stream_manager(sharded<replica::database>& db,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::view::view_update_generator>& view_update_generator,
            sharded<netw::messaging_service>& ms,
            sharded<service::migration_manager>& mm,
            gms::gossiper& gossiper);

    future<> start();
    future<> stop();

    semaphore& mutation_send_limiter() { return _mutation_send_limiter; }

    void register_sending(shared_ptr<stream_result_future> result);

    void register_receiving(shared_ptr<stream_result_future> result);

    shared_ptr<stream_result_future> get_sending_stream(UUID plan_id) const;

    shared_ptr<stream_result_future> get_receiving_stream(UUID plan_id) const;

    std::vector<shared_ptr<stream_result_future>> get_all_streams() const;

    replica::database& db() noexcept { return _db.local(); }
    netw::messaging_service& ms() noexcept { return _ms.local(); }

    const std::unordered_map<UUID, shared_ptr<stream_result_future>>& get_initiated_streams() const {
        return _initiated_streams;
    }

    const std::unordered_map<UUID, shared_ptr<stream_result_future>>& get_receiving_streams() const {
        return _receiving_streams;
    }

    void remove_stream(UUID plan_id);

    void show_streams() const;

    future<> shutdown() {
        fail_all_sessions();
        return make_ready_future<>();
    }

    void update_progress(UUID cf_id, gms::inet_address peer, progress_info::direction dir, size_t fm_size);
    future<> update_all_progress_info();

    void remove_progress(UUID plan_id);

    stream_bytes get_progress(UUID plan_id, gms::inet_address peer) const;

    stream_bytes get_progress(UUID plan_id) const;

    future<> remove_progress_on_all_shards(UUID plan_id);

    future<stream_bytes> get_progress_on_all_shards(UUID plan_id, gms::inet_address peer) const;

    future<stream_bytes> get_progress_on_all_shards(UUID plan_id) const;

    future<stream_bytes> get_progress_on_all_shards(gms::inet_address peer) const;

    future<stream_bytes> get_progress_on_all_shards() const;

    stream_bytes get_progress_on_local_shard() const;

    shared_ptr<stream_session> get_session(utils::UUID plan_id, gms::inet_address from, const char* verb, std::optional<utils::UUID> cf_id = {});

public:
    virtual void on_join(inet_address endpoint, endpoint_state ep_state) override {}
    virtual void before_change(inet_address endpoint, endpoint_state current_state, application_state new_state_key, const versioned_value& new_value) override {}
    virtual void on_change(inet_address endpoint, application_state state, const versioned_value& value) override {}
    virtual void on_alive(inet_address endpoint, endpoint_state state) override {}
    virtual void on_dead(inet_address endpoint, endpoint_state state) override;
    virtual void on_remove(inet_address endpoint) override;
    virtual void on_restart(inet_address endpoint, endpoint_state ep_state) override;

private:
    void fail_all_sessions();
    void fail_sessions(inet_address endpoint);
    bool has_peer(inet_address endpoint) const;

    void init_messaging_service_handler();
    future<> uninit_messaging_service_handler();
};

} // namespace streaming
