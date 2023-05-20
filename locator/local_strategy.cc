/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include "local_strategy.hh"
#include "utils/class_registrator.hh"
#include "utils/fb_utilities.hh"


namespace locator {

local_strategy::local_strategy(const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(config_options, replication_strategy_type::local) {
    _natural_endpoints_depend_on_token = false;
}

future<endpoint_set> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    return make_ready_future<endpoint_set>(endpoint_set({utils::fb_utilities::get_broadcast_address()}));
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

inet_address_vector_replica_set local_strategy::get_natural_endpoints(const token&, const vnode_effective_replication_map&) const {
    return inet_address_vector_replica_set({utils::fb_utilities::get_broadcast_address()});
}

stop_iteration local_strategy::for_each_natural_endpoint_until(const token&, const vnode_effective_replication_map&, const noncopyable_function<stop_iteration(const inet_address&)>& func) const {
    return func(utils::fb_utilities::get_broadcast_address());
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
