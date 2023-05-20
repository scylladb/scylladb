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
#include "utils/fb_utilities.hh"
#include "locator/token_metadata.hh"

namespace locator {

everywhere_replication_strategy::everywhere_replication_strategy(const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(config_options, replication_strategy_type::everywhere_topology) {
    _natural_endpoints_depend_on_token = false;
}

future<endpoint_set> everywhere_replication_strategy::calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const {
    auto eps = tm.get_all_endpoints();
    return make_ready_future<endpoint_set>(endpoint_set(eps.begin(), eps.end()));
}

size_t everywhere_replication_strategy::get_replication_factor(const token_metadata& tm) const {
    return tm.sorted_tokens().empty() ? 1 : tm.count_normal_token_owners();
}

inet_address_vector_replica_set everywhere_replication_strategy::get_natural_endpoints(const token&, const vnode_effective_replication_map& erm) const {
    const auto& tm = *erm.get_token_metadata_ptr();
    if (tm.sorted_tokens().empty()) {
        return inet_address_vector_replica_set({utils::fb_utilities::get_broadcast_address()});
    }
    return boost::copy_range<inet_address_vector_replica_set>(tm.get_all_endpoints());
}

stop_iteration everywhere_replication_strategy::for_each_natural_endpoint_until(const token&, const vnode_effective_replication_map& erm, const noncopyable_function<stop_iteration(const inet_address&)>& func) const {
    const auto& tm = *erm.get_token_metadata_ptr();
    if (tm.sorted_tokens().empty()) {
        return func(utils::fb_utilities::get_broadcast_address());
    }
    for (const auto& ep : tm.get_all_endpoints()) {
        if (func(ep)) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

using registry = class_registrator<abstract_replication_strategy, everywhere_replication_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.EverywhereStrategy");
static registry registrator_short_name("EverywhereStrategy");
}
