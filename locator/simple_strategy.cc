/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <algorithm>
#include "simple_strategy.hh"
#include "utils/class_registrator.hh"
#include <boost/algorithm/string.hpp>
#include "utils/sequenced_set.hh"

namespace locator {

simple_strategy::simple_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options) :
        abstract_replication_strategy(keyspace_name, token_metadata, snitch, config_options, replication_strategy_type::simple) {
    for (auto& config_pair : config_options) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        if (boost::iequals(key, "replication_factor")) {
            _replication_factor = std::stol(val);

            break;
        }
    }
}

std::vector<inet_address> simple_strategy::calculate_natural_endpoints(const token& t) const {
    const std::vector<token>& tokens = _token_metadata.sorted_tokens();

    if (tokens.empty()) {
        return std::vector<inet_address>();
    }

    size_t replicas = get_replication_factor();
    utils::sequenced_set<inet_address> endpoints;
    endpoints.reserve(replicas);

    for (auto& token : _token_metadata.ring_range(t)) {
        auto ep = _token_metadata.get_endpoint(token);
        assert(ep);

        endpoints.push_back(*ep);
        if (endpoints.size() == replicas) {
           break;
        }
    }

    return std::move(endpoints.get_vector());
}

size_t simple_strategy::get_replication_factor() const {
    return _replication_factor;
}

using registry = class_registrator<abstract_replication_strategy, simple_strategy, const sstring&, token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.SimpleStrategy");
static registry registrator_short_name("SimpleStrategy");

}
