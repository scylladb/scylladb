/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include "local_strategy.hh"
#include "utils/class_registrator.hh"


namespace locator {

local_strategy::local_strategy(const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(config_options, replication_strategy_type::local) {
    _natural_endpoints_depend_on_token = false;
}

future<endpoint_set> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    return make_ready_future<endpoint_set>(endpoint_set({tm.get_topology().my_address()}));
}

void local_strategy::validate_options(const gms::feature_service&) const {
}

std::optional<std::unordered_set<sstring>> local_strategy::recognized_options(const topology&) const {
    // LocalStrategy doesn't expect any options.
    return {};
}

size_t local_strategy::get_replication_factor(const token_metadata&) const {
    return 1;
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
