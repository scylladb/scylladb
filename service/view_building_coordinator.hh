/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <compare>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "dht/i_partitioner_fwd.hh"
#include "locator/host_id.hh"
#include "schema/schema_fwd.hh"

namespace service {

struct view_building_target {
    locator::host_id host;
    unsigned shard;

    std::strong_ordering operator<=>(const view_building_target&) const = default;
};

using view_building_tasks = std::map<view_building_target, dht::token_range_vector>;
using base_building_tasks = std::map<table_id, view_building_tasks>;
using view_building_coordinator_tasks = std::map<table_id, base_building_tasks>;

}
