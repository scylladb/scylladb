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

future<natural_ep_type> everywhere_replication_strategy::calculate_natural_endpoints(const token& search_token, const token_metadata& tm, bool use_host_id) const {
    return select_tm([this]<typename NodeId>(const generic_token_metadata<NodeId>& tm) -> future<natural_ep_type> {
    if (tm.sorted_tokens().empty()) {
            set_type<NodeId> result{vector_type<NodeId>({this->get_self_id<NodeId>(tm)})};
        return make_ready_future<natural_ep_type>(std::move(result));
    }
    const auto& all_endpoints = tm.get_all_endpoints();
    return make_ready_future<natural_ep_type>(set_type<NodeId>(all_endpoints.begin(), all_endpoints.end()));
    }, tm, use_host_id);
}

size_t everywhere_replication_strategy::get_replication_factor(const token_metadata& tm) const {
    return tm.sorted_tokens().empty() ? 1 : tm.count_normal_token_owners();
}

using registry = class_registrator<abstract_replication_strategy, everywhere_replication_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.EverywhereStrategy");
static registry registrator_short_name("EverywhereStrategy");
}
