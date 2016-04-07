/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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

std::vector<inet_address> simple_strategy::calculate_natural_endpoints(const token& t, token_metadata& tm) const {
    const std::vector<token>& tokens = tm.sorted_tokens();

    if (tokens.empty()) {
        return std::vector<inet_address>();
    }

    size_t replicas = get_replication_factor();
    utils::sequenced_set<inet_address> endpoints;
    endpoints.reserve(replicas);

    for (auto& token : tm.ring_range(t)) {
        auto ep = tm.get_endpoint(token);
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

void simple_strategy::validate_options() const {
    auto it = _config_options.find("replication_factor");
    if (it == _config_options.end()) {
        throw exceptions::configuration_exception("SimpleStrategy requires a replication_factor strategy option.");
    }
    validate_replication_factor(it->second);
}

std::experimental::optional<std::set<sstring>>simple_strategy::recognized_options() const {
    return {{ "replication_factor" }};
}

using registry = class_registrator<abstract_replication_strategy, simple_strategy, const sstring&, token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.SimpleStrategy");
static registry registrator_short_name("SimpleStrategy");

}
