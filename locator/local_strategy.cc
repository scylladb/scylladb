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

local_strategy_traits::local_strategy_traits(const replication_strategy_params&)
    : abstract_replication_strategy_traits(replication_strategy_type::local, local::yes)
{}

local_strategy::local_strategy(const topology&, replication_strategy_params params) :
        abstract_replication_strategy(local_strategy_traits(params), params) {
    _natural_endpoints_depend_on_token = false;
}

future<host_id_set> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    return make_ready_future<host_id_set>(host_id_set{host_id{}});
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

using registry = strategy_class_registry::registrator<local_strategy>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

using traits_registry = strategy_class_traits_registry::registrator<local_strategy_traits>;
static traits_registry traits_registrator("org.apache.cassandra.locator.LocalStrategy");
static traits_registry traits_registrator_short_name("LocalStrategy");

}
