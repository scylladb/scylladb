/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "gms/gossiper.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace service {

class storage_proxy;

class view_update_backlog_broker final
        : public seastar::peering_sharded_service<view_update_backlog_broker>
        , public seastar::async_sharded_service<view_update_backlog_broker>
        , public gms::i_endpoint_state_change_subscriber {

    seastar::sharded<storage_proxy>& _sp;
    gms::gossiper& _gossiper;
    seastar::future<> _started = make_ready_future<>();
    seastar::abort_source _as;

public:
    view_update_backlog_broker(seastar::sharded<storage_proxy>&, gms::gossiper&);

    seastar::future<> start();

    seastar::future<> stop();

    virtual void on_change(gms::inet_address, gms::application_state, const gms::versioned_value&) override;

    virtual void on_remove(gms::inet_address) override;

    virtual void on_join(gms::inet_address, gms::endpoint_state) override { }
    virtual void before_change(gms::inet_address, gms::endpoint_state, gms::application_state, const gms::versioned_value&) { }
    virtual void on_alive(gms::inet_address, gms::endpoint_state) override { }
    virtual void on_dead(gms::inet_address, gms::endpoint_state) override { }
    virtual void on_restart(gms::inet_address, gms::endpoint_state) override { }
};

}
