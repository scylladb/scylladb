/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#include "locator/everywhere_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "locator/token_metadata.hh"

namespace locator {

everywhere_replication_strategy::everywhere_replication_strategy(const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(config_options, replication_strategy_type::everywhere_topology) {
    _natural_endpoints_depend_on_token = false;
}

future<endpoint_set> everywhere_replication_strategy::calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const {
    if (tm.sorted_tokens().empty()) {
        endpoint_set result{inet_address_vector_replica_set({tm.get_topology().my_address()})};
        return make_ready_future<endpoint_set>(std::move(result));
    }
    const auto& all_endpoints = tm.get_all_endpoints();
    return make_ready_future<endpoint_set>(endpoint_set(all_endpoints.begin(), all_endpoints.end()));
}

size_t everywhere_replication_strategy::get_replication_factor(const token_metadata& tm) const {
    return tm.sorted_tokens().empty() ? 1 : tm.count_normal_token_owners();
}

using registry = class_registrator<abstract_replication_strategy, everywhere_replication_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.EverywhereStrategy");
static registry registrator_short_name("EverywhereStrategy");
}
