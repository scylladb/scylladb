/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "snitch_base.hh"

namespace locator {

using inet_address = gms::inet_address;

/**
 * A simple endpoint snitch implementation that assumes datacenter and rack information is encoded
 * in the 2nd and 3rd octets of the ip address, respectively.
 */
struct rack_inferring_snitch : public snitch_base {
    rack_inferring_snitch(const snitch_config& cfg)
        : snitch_base(cfg)
    {
        _my_dc = get_datacenter();
        _my_rack = get_rack();

        // This snitch is ready on creation
        set_snitch_ready();
    }

    virtual sstring get_rack() const override {
        auto& endpoint = _cfg.broadcast_address;
        return std::to_string(uint8_t(endpoint.bytes()[2]));
    }

    virtual sstring get_datacenter() const override {
        auto& endpoint = _cfg.broadcast_address;
        return std::to_string(uint8_t(endpoint.bytes()[1]));
    }

    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.RackInferringSnitch";
    }
};

} // namespace locator
