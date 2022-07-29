/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"

namespace netw {
class messaging_service;
}

namespace locator {

class i_endpoint_snitch;

// @note all callbacks should be called in seastar::async() context
class reconnectable_snitch_helper : public  gms::i_endpoint_state_change_subscriber {
private:
    i_endpoint_snitch& _snitch;
    netw::messaging_service& _ms;

    static logging::logger& logger();
    sstring _local_dc;
private:
    future<> reconnect(gms::inet_address public_address, const gms::versioned_value& local_address_value);
    future<> reconnect(gms::inet_address public_address, gms::inet_address local_address);
public:
    reconnectable_snitch_helper(sstring local_dc, i_endpoint_snitch&, netw::messaging_service&);
    future<> before_change(gms::inet_address endpoint, gms::endpoint_state cs, gms::application_state new_state_key, const gms::versioned_value& new_value) override;
    future<> on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    future<> on_change(gms::inet_address endpoint, gms::application_state state, const gms::versioned_value& value) override;
    future<> on_alive(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    future<> on_dead(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    future<> on_remove(gms::inet_address endpoint) override;
    future<> on_restart(gms::inet_address endpoint, gms::endpoint_state state) override;
};
} // namespace locator
