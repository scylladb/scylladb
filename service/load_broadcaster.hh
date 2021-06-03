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
 */

/*
 * Modified by ScyllaDB
 * Copyright 2015-present ScyllaDB
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

#include "database_fwd.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/gossiper.hh"

namespace service {
class load_broadcaster : public gms::i_endpoint_state_change_subscriber, public enable_shared_from_this<load_broadcaster>
{
public:
    static constexpr std::chrono::milliseconds BROADCAST_INTERVAL{60 * 1000};

private:
    distributed<database>& _db;
    gms::gossiper& _gossiper;
    std::unordered_map<gms::inet_address, double> _load_info;
    timer<> _timer;
    future<> _done = make_ready_future<>();
    bool _stopped = false;

public:
    load_broadcaster(distributed<database>& db, gms::gossiper& g) : _db(db), _gossiper(g) {
        _gossiper.register_(shared_from_this());
    }
    ~load_broadcaster() {
        assert(_stopped);
    }

    void on_change(gms::inet_address endpoint, gms::application_state state, const gms::versioned_value& value) {
        if (state == gms::application_state::LOAD) {
            _load_info[endpoint] = std::stod(value.value);
        }
    }

    void on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override {
        auto* local_value = ep_state.get_application_state_ptr(gms::application_state::LOAD);
        if (local_value) {
            on_change(endpoint, gms::application_state::LOAD, *local_value);
        }
    }
    
    void before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, const gms::versioned_value& newValue) {}

    void on_alive(gms::inet_address endpoint, gms::endpoint_state) override {}

    void on_dead(gms::inet_address endpoint, gms::endpoint_state) override {}

    void on_restart(gms::inet_address endpoint, gms::endpoint_state) override {}

    void on_remove(gms::inet_address endpoint) {
        _load_info.erase(endpoint);
    }

    const std::unordered_map<gms::inet_address, double> get_load_info() const {
        return _load_info;
    }

    void start_broadcasting();
    future<> stop_broadcasting();
};
}
