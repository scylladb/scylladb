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

#include "locator/network_topology_strategy.hh"
#include "utils/sequenced_set.hh"
#include <boost/algorithm/string.hpp>

namespace locator {


network_topology_strategy::network_topology_strategy(
    const sstring& keyspace_name,
    token_metadata& token_metadata,
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

std::vector<inet_address>
network_topology_strategy::calculate_natural_endpoints(
    const token& search_token, token_metadata& tm) const {
    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    utils::sequenced_set<inet_address> replicas;

    // replicas we have found in each DC
    std::unordered_map<sstring, std::unordered_set<inet_address>> dc_replicas;
    // tracks the racks we have already placed replicas in
    std::unordered_map<sstring, std::unordered_set<sstring>> seen_racks;
    //
    // tracks the endpoints that we skipped over while looking for unique racks
    // when we relax the rack uniqueness we can append this to the current
    // result so we don't have to wind back the iterator
    //
    std::unordered_map<sstring, utils::sequenced_set<inet_address>>
        skipped_dc_endpoints;

    //
    // Populate the temporary data structures.
    //
    replicas.reserve(get_replication_factor());
    for (auto& dc_rep_factor_pair : _dc_rep_factor) {
        auto& dc_name = dc_rep_factor_pair.first;

        dc_replicas[dc_name].reserve(dc_rep_factor_pair.second);
        seen_racks[dc_name] = {};
        skipped_dc_endpoints[dc_name] = {};
    }

    topology& tp = tm.get_topology();

    //
    // all endpoints in each DC, so we can check when we have exhausted all
    // the members of a DC
    //
    std::unordered_map<sstring,
                       std::unordered_set<inet_address>>&
        all_endpoints = tp.get_datacenter_endpoints();
    //
    // all racks in a DC so we can check when we have exhausted all racks in a
    // DC
    //
    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>&
        racks = tp.get_datacenter_racks();

    // not aware of any cluster members
    assert(!all_endpoints.empty() && !racks.empty());

    for (auto& next : tm.ring_range(search_token)) {

        if (has_sufficient_replicas(dc_replicas, all_endpoints)) {
            break;
        }

        inet_address ep = *tm.get_endpoint(next);
        sstring dc = _snitch->get_datacenter(ep);

        auto& seen_racks_dc_set = seen_racks[dc];
        auto& racks_dc_map = racks[dc];
        auto& skipped_dc_endpoints_set = skipped_dc_endpoints[dc];
        auto& dc_replicas_dc_set = dc_replicas[dc];

        // have we already found all replicas for this dc?
        if (_dc_rep_factor.find(dc)  == _dc_rep_factor.end() ||
            has_sufficient_replicas(dc, dc_replicas, all_endpoints)) {
            continue;
        }

        //
        // can we skip checking the rack? - namely, we've seen all racks in this
        // DC already and may add this endpoint right away.
        //
        if (seen_racks_dc_set.size() == racks_dc_map.size()) {
            dc_replicas_dc_set.insert(ep);
            replicas.push_back(ep);
        } else {
            sstring rack = _snitch->get_rack(ep);
            // is this a new rack? - we prefer to replicate on different racks
            if (seen_racks_dc_set.find(rack) != seen_racks_dc_set.end()) {
                skipped_dc_endpoints_set.push_back(ep);
            } else { // this IS a new rack
                dc_replicas_dc_set.insert(ep);
                replicas.push_back(ep);
                seen_racks_dc_set.insert(rack);
                //
                // if we've run out of distinct racks, add the hosts we skipped
                // past already (up to RF)
                //
                if (seen_racks_dc_set.size() == racks_dc_map.size())
                {
                    auto skipped_it = skipped_dc_endpoints_set.begin();
                    while (skipped_it != skipped_dc_endpoints_set.end() &&
                           !has_sufficient_replicas(dc, dc_replicas, all_endpoints)) {
                        inet_address skipped = *skipped_it++;
                        dc_replicas_dc_set.insert(skipped);
                        replicas.push_back(skipped);
                    }
                }
            }
        }
    }

    return std::move(replicas.get_vector());
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

std::experimental::optional<std::set<sstring>> network_topology_strategy::recognized_options() const {
    // We explicitely allow all options
    return std::experimental::nullopt;
}

inline bool network_topology_strategy::has_sufficient_replicas(
        const sstring& dc,
        std::unordered_map<sstring,
                           std::unordered_set<inet_address>>& dc_replicas,
        std::unordered_map<sstring,
                           std::unordered_set<inet_address>>& all_endpoints) const {

        return dc_replicas[dc].size() >=
            std::min(all_endpoints[dc].size(), get_replication_factor(dc));
}

inline bool network_topology_strategy::has_sufficient_replicas(
        std::unordered_map<sstring,
                           std::unordered_set<inet_address>>& dc_replicas,
        std::unordered_map<sstring,
                           std::unordered_set<inet_address>>& all_endpoints) const {

        for (auto& dc : get_datacenters()) {
            if (!has_sufficient_replicas(dc, dc_replicas, all_endpoints)) {
                return false;
            }
        }

        return true;
}

using registry = class_registrator<abstract_replication_strategy, network_topology_strategy, const sstring&, token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.NetworkTopologyStrategy");
static registry registrator_short_name("NetworkTopologyStrategy");
}
