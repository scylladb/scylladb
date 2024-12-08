/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "dht/i_partitioner_fwd.hh"
#include "locator/host_id.hh"
#include "schema/schema_fwd.hh"

namespace service {

namespace vbc {
struct view_building_target {
    locator::host_id host;
    unsigned shard;

    bool operator<(const view_building_target& other) const {
        return host < other.host || (host == other.host && shard < other.shard);
    }
};

using view_name = std::pair<sstring, sstring>;
using view_tasks = std::map<view_building_target, dht::token_range_vector>;
using base_tasks = std::map<view_name, view_tasks>;
struct vbc_tasks : public std::map<table_id, base_tasks> {};
}

}
