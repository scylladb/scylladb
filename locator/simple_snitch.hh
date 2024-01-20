/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once
#include "snitch_base.hh"

namespace locator {

/**
 * A simple endpoint snitch implementation that treats Strategy order as
 * proximity, allowing non-read-repaired reads to prefer a single endpoint,
 * which improves cache locality.
 */
struct simple_snitch : public snitch_base {
    simple_snitch(const snitch_config& cfg)
        : snitch_base(cfg)
    {
        _my_dc = get_datacenter();
        _my_rack = get_rack();

        // This snitch is ready on creation
        set_snitch_ready();
    }

    virtual sstring get_rack() const override {
        return "rack1";
    }

    virtual sstring get_datacenter() const override {
        return "datacenter1";
    }

    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.SimpleSnitch";
    }
};

}
