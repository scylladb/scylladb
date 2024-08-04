/*
 * Modified by ScyllaDB
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/assert.hh"
#include "replica/database_fwd.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/gossiper.hh"

namespace service {
class load_broadcaster : public gms::i_endpoint_state_change_subscriber, public enable_shared_from_this<load_broadcaster>
{
public:
    static constexpr std::chrono::milliseconds BROADCAST_INTERVAL{60 * 1000};

private:
    distributed<replica::database>& _db;
    gms::gossiper& _gossiper;
    std::unordered_map<gms::inet_address, double> _load_info;
    timer<> _timer;
    future<> _done = make_ready_future<>();
    bool _stopped = false;

public:
    load_broadcaster(distributed<replica::database>& db, gms::gossiper& g) : _db(db), _gossiper(g) {
        _gossiper.register_(shared_from_this());
    }
    ~load_broadcaster() {
        SCYLLA_ASSERT(_stopped);
    }

    virtual future<> on_change(gms::inet_address endpoint, const gms::application_state_map& states, gms::permit_id pid) override {
        return on_application_state_change(endpoint, states, gms::application_state::LOAD, pid, [this] (gms::inet_address endpoint, const gms::versioned_value& value, gms::permit_id) {
            _load_info[endpoint] = std::stod(value.value());
            return make_ready_future<>();
        });
    }

    virtual future<> on_join(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id pid) override {
        auto* local_value = ep_state->get_application_state_ptr(gms::application_state::LOAD);
        if (local_value) {
            _load_info[endpoint] = std::stod(local_value->value());
        }
        return make_ready_future();
    }
    
    future<> on_alive(gms::inet_address endpoint, gms::endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }

    future<> on_dead(gms::inet_address endpoint, gms::endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }

    future<> on_restart(gms::inet_address endpoint, gms::endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }

    virtual future<> on_remove(gms::inet_address endpoint, gms::permit_id) override {
        _load_info.erase(endpoint);
        return make_ready_future();
    }

    const std::unordered_map<gms::inet_address, double> get_load_info() const {
        return _load_info;
    }

    void start_broadcasting();
    future<> stop_broadcasting();

    const gms::gossiper& gossiper() const noexcept {
        return _gossiper;
    };
};
}
