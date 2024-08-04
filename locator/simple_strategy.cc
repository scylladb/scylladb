/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "simple_strategy.hh"
#include "exceptions/exceptions.hh"
#include "utils/assert.hh"
#include "utils/class_registrator.hh"
#include <boost/algorithm/string.hpp>

namespace locator {

simple_strategy::simple_strategy(replication_strategy_params params) :
        abstract_replication_strategy(params, replication_strategy_type::simple) {
    for (auto& config_pair : _config_options) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        if (boost::iequals(key, "replication_factor")) {
            _replication_factor = parse_replication_factor(val);
            break;
        }
    }
}

future<host_id_set> simple_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    const std::vector<token>& tokens = tm.sorted_tokens();

    if (tokens.empty()) {
        co_return host_id_set{};
    }

    size_t replicas = _replication_factor;
    host_id_set endpoints;
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
        SCYLLA_ASSERT(ep);

        endpoints.push_back(*ep);
        co_await coroutine::maybe_yield();
    }

    co_return endpoints;
}

size_t simple_strategy::get_replication_factor(const token_metadata&) const {
    return _replication_factor;
}

void simple_strategy::validate_options(const gms::feature_service&) const {
    auto it = _config_options.find("replication_factor");
    if (it == _config_options.end()) {
        throw exceptions::configuration_exception("SimpleStrategy requires a replication_factor strategy option.");
    }
    parse_replication_factor(it->second);
}

std::optional<std::unordered_set<sstring>>simple_strategy::recognized_options(const topology&) const {
    return {{ "replication_factor" }};
}

using registry = class_registrator<abstract_replication_strategy, simple_strategy, replication_strategy_params>;
static registry registrator("org.apache.cassandra.locator.SimpleStrategy");
static registry registrator_short_name("SimpleStrategy");

}
