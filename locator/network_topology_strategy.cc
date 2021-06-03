/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

#include <functional>
#include "locator/network_topology_strategy.hh"
#include "utils/sequenced_set.hh"
#include <boost/algorithm/string.hpp>
#include "utils/hash.hh"

namespace std {
template<>
struct hash<locator::endpoint_dc_rack> {
    size_t operator()(const locator::endpoint_dc_rack& v) const {
        return utils::tuple_hash()(std::tie(v.dc, v.rack));
    }
};
}

namespace locator {

bool operator==(const endpoint_dc_rack& d1, const endpoint_dc_rack& d2) {
    return std::tie(d1.dc, d1.rack) == std::tie(d2.dc, d2.rack);
}

network_topology_strategy::network_topology_strategy(
    const sstring& keyspace_name,
    const shared_token_metadata& token_metadata,
    snitch_ptr& snitch,
    const std::map<sstring, sstring>& config_options) :
        abstract_replication_strategy(keyspace_name,
                                      token_metadata,
                                      snitch,
                                      config_options,
                                      replication_strategy_type::network_topology) {
    for (auto& config_pair : config_options) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        //
        // FIXME!!!
        // The first option we get at the moment is a class name. Skip it!
        //
        if (boost::iequals(key, "class")) {
            continue;
        }

        if (boost::iequals(key, "replication_factor")) {
            throw exceptions::configuration_exception(
                "replication_factor is an option for SimpleStrategy, not "
                "NetworkTopologyStrategy");
        }

        validate_replication_factor(val);
        _dc_rep_factor.emplace(key, std::stol(val));
        _datacenteres.push_back(key);
    }

    _rep_factor = 0;

    for (auto& one_dc_rep_factor : _dc_rep_factor) {
        _rep_factor += one_dc_rep_factor.second;
    }

    debug("Configured datacenter replicas are:");
    for (auto& p : _dc_rep_factor) {
        debug("{}: {}", p.first, p.second);
    }
}

inet_address_vector_replica_set
network_topology_strategy::calculate_natural_endpoints(
    const token& search_token, const token_metadata& tm, can_yield can_yield) const {

    using endpoint_set = utils::sequenced_set<inet_address>;
    using endpoint_dc_rack_set = std::unordered_set<endpoint_dc_rack>;

    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    struct data_center_endpoints {
        /** List accepted endpoints get pushed into. */
        endpoint_set& _endpoints;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names aren't a problem.
         */
        endpoint_dc_rack_set& _racks;

        /** Number of replicas left to fill from this DC. */
        size_t _rf_left;
        ssize_t _acceptable_rack_repeats;

        data_center_endpoints(size_t rf, size_t rack_count, size_t node_count, endpoint_set& endpoints, endpoint_dc_rack_set& racks)
            : _endpoints(endpoints)
            , _racks(racks)
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            , _rf_left(std::min(rf, node_count))
            // If there aren't enough racks in this DC to fill the RF, we'll still use at least one node from each rack,
            // and the difference is to be filled by the first encountered nodes.
            , _acceptable_rack_repeats(rf - rack_count)
        {}

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the endpoints set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        bool add_endpoint_and_check_if_done(const inet_address& ep, const endpoint_dc_rack& location) {
            if (done()) {
                return false;
            }

            if (_racks.emplace(location).second) {
                // New rack.
                --_rf_left;
                auto added = _endpoints.insert(ep).second;
                if (!added) {
                    throw std::runtime_error(sprint("Topology error: found {} in more than one rack", ep));
                }
                return done();
            }

            /**
             * Ensure we don't allow too many endpoints in the same rack, i.e. we have
             * minimum current rf_left + 1 distinct racks. See above, _acceptable_rack_repeats
             * is defined as RF - rack_count, i.e. how many nodes in a single rack we are ok
             * with.
             *
             * With RF = 3 and 2 Racks in DC,
             *
             * IP1, Rack1
             * IP2, Rack1
             * IP3, Rack1,    The line _acceptable_rack_repeats <= 0 will reject IP3.
             * IP4, Rack2
             *
             */
            if (_acceptable_rack_repeats <= 0) {
                // There must be rf_left distinct racks left, do not add any more rack repeats.
                return false;
            }

            if (!_endpoints.insert(ep).second) {
                // Cannot repeat a node.
                return false;
            }

            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --_acceptable_rack_repeats;
            --_rf_left;

            return done();
        }

        bool done() const {
            return _rf_left == 0;
        }
    };

    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    endpoint_set replicas;
    // tracks the racks we have already placed replicas in
    endpoint_dc_rack_set  seen_racks;

    const topology& tp = tm.get_topology();

    //
    // all endpoints in each DC, so we can check when we have exhausted all
    // the members of a DC
    //
    const std::unordered_map<sstring,
                       std::unordered_set<inet_address>>&
        all_endpoints = tp.get_datacenter_endpoints();
    //
    // all racks in a DC so we can check when we have exhausted all racks in a
    // DC
    //
    const std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>&
        racks = tp.get_datacenter_racks();
    // not aware of any cluster members
    assert(!all_endpoints.empty() && !racks.empty());

    std::unordered_map<sstring_view, data_center_endpoints> dcs(_dc_rep_factor.size());

    auto size_for = [](auto& map, auto& k) {
        auto i = map.find(k);
        return i != map.end() ? i->second.size() : size_t(0);
    };

    // Create a data_center_endpoints object for each non-empty DC.
    for (auto& p : _dc_rep_factor) {
        auto& dc = p.first;
        auto rf = p.second;
        auto node_count = size_for(all_endpoints, dc);

        if (rf == 0 || node_count == 0) {
            continue;
        }

        dcs.emplace(dc, data_center_endpoints(rf, size_for(racks, dc), node_count, replicas, seen_racks));
    }

    auto dcs_to_fill = dcs.size();

    for (auto& next : tm.ring_range(search_token)) {
        if (dcs_to_fill == 0) {
            break;
        }
        if (can_yield) {
            seastar::thread::maybe_yield();
        }

        inet_address ep = *tm.get_endpoint(next);
        auto& loc = tp.get_location(ep);
        auto i = dcs.find(loc.dc);
        if (i != dcs.end() && i->second.add_endpoint_and_check_if_done(ep, loc)) {
            --dcs_to_fill;
        }
    }

    return boost::copy_range<inet_address_vector_replica_set>(replicas.get_vector());
}

void network_topology_strategy::validate_options() const {
    for (auto& c : _config_options) {
        if (c.first == sstring("replication_factor")) {
            throw exceptions::configuration_exception(
                "replication_factor is an option for simple_strategy, not "
                "network_topology_strategy");
        }
        validate_replication_factor(c.second);
    }
}

std::optional<std::set<sstring>> network_topology_strategy::recognized_options() const {
    std::set<sstring> datacenters;
    for (const auto& [dc_name, endpoints] : _shared_token_metadata.get()->get_topology().get_datacenter_endpoints()) {
        datacenters.insert(dc_name);
    }
    // We only allow datacenter names as options
    return datacenters;
}

using registry = class_registrator<abstract_replication_strategy, network_topology_strategy, const sstring&, const shared_token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.NetworkTopologyStrategy");
static registry registrator_short_name("NetworkTopologyStrategy");
}
