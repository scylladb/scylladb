/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace gms {
class gossiper;
}

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

    virtual future<> on_change(gms::inet_address, locator::host_id id, const gms::application_state_map& states, gms::permit_id) override;

    virtual future<> on_remove(gms::inet_address, locator::host_id id, gms::permit_id) override;
};

}
