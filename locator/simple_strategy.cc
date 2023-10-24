/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "simple_strategy.hh"
#include "utils/class_registrator.hh"
#include <boost/algorithm/string.hpp>
#include "utils/sequenced_set.hh"

namespace locator {

simple_strategy::simple_strategy(const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(config_options, replication_strategy_type::simple) {
    for (auto& config_pair : config_options) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        if (boost::iequals(key, "replication_factor")) {
            validate_replication_factor(val);
            _replication_factor = std::stol(val);

            break;
        }
    }
}

future<natural_ep_type> simple_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm, bool use_host_id) const {
    return select_tm([&]<typename NodeId>(const generic_token_metadata<NodeId>& tm) -> future<natural_ep_type> {
    const std::vector<token>& tokens = tm.sorted_tokens();

    if (tokens.empty()) {
        co_return set_type<NodeId>{};
    }

    size_t replicas = _replication_factor;
    set_type<NodeId> endpoints;
    endpoints.reserve(replicas);

    for (auto& token : tm.ring_range(t)) {
        // If the number of nodes in the cluster is smaller than the desired
        // replication factor we should return the loop when endpoints already
        // contains all the nodes in the cluster because no more nodes could be
        // added to endpoints lists.
        if (endpoints.size() == replicas || endpoints.size() == tm.count_normal_token_owners()) {
           break;
        }

        auto ep = tm.get_endpoint(token);
        assert(ep);

        endpoints.push_back(*ep);
        co_await coroutine::maybe_yield();
    }

    co_return endpoints;
    }, tm, use_host_id);
}

size_t simple_strategy::get_replication_factor(const token_metadata&) const {
    return _replication_factor;
}

void simple_strategy::validate_options(const gms::feature_service&) const {
    auto it = _config_options.find("replication_factor");
    if (it == _config_options.end()) {
        throw exceptions::configuration_exception("SimpleStrategy requires a replication_factor strategy option.");
    }
    validate_replication_factor(it->second);
}

std::optional<std::unordered_set<sstring>>simple_strategy::recognized_options(const topology&) const {
    return {{ "replication_factor" }};
}

using registry = class_registrator<abstract_replication_strategy, simple_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.SimpleStrategy");
static registry registrator_short_name("SimpleStrategy");

}
