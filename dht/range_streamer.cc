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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
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

#include "dht/range_streamer.hh"
#include "utils/fb_utilities.hh"
#include "locator/snitch_base.hh"
#include "database.hh"
#include "gms/gossiper.hh"
#include "gms/failure_detector.hh"
#include "log.hh"
#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"

namespace dht {

logging::logger logger("range_streamer");

using inet_address = gms::inet_address;

static std::unordered_map<range<token>, std::unordered_set<inet_address>>
unordered_multimap_to_unordered_map(const std::unordered_multimap<range<token>, inet_address>& multimap) {
    std::unordered_map<range<token>, std::unordered_set<inet_address>> ret;
    for (auto x : multimap) {
        auto& range_token = x.first;
        auto& ep = x.second;
        auto it = ret.find(range_token);
        if (it != ret.end()) {
            it->second.emplace(ep);
        } else {
            ret.emplace(range_token, std::unordered_set<inet_address>{ep});
        }
    }
    return ret;
}

std::unordered_multimap<inet_address, range<token>>
range_streamer::get_range_fetch_map(const std::unordered_multimap<range<token>, inet_address>& ranges_with_sources,
                                    const std::unordered_set<std::unique_ptr<i_source_filter>>& source_filters,
                                    const sstring& keyspace) {
    std::unordered_multimap<inet_address, range<token>> range_fetch_map_map;
    for (auto x : unordered_multimap_to_unordered_map(ranges_with_sources)) {
        const range<token>& range_ = x.first;
        const std::unordered_set<inet_address>& addresses = x.second;
        bool found_source = false;
        for (auto address : addresses) {
            if (address == utils::fb_utilities::get_broadcast_address()) {
                // If localhost is a source, we have found one, but we don't add it to the map to avoid streaming locally
                found_source = true;
                continue;
            }

            auto filtered = false;
            for (const auto& filter : source_filters) {
                if (!filter->should_include(address)) {
                    filtered = true;
                    break;
                }
            }

            if (filtered) {
                continue;
            }

            range_fetch_map_map.emplace(address, range_);
            found_source = true;
            break; // ensure we only stream from one other node for each range
        }

        if (!found_source) {
            throw std::runtime_error(sprint("unable to find sufficient sources for streaming range %s in keyspace %s", range_, keyspace));
        }
    }

    return range_fetch_map_map;
}

std::unordered_multimap<range<token>, inet_address>
range_streamer::get_all_ranges_with_sources_for(const sstring& keyspace_name, std::vector<range<token>> desired_ranges) {
    logger.debug("{} ks={}", __func__, keyspace_name);

    auto& ks = _db.local().find_keyspace(keyspace_name);
    auto& strat = ks.get_replication_strategy();

    // std::unordered_multimap<range<token>, inet_address>
    auto tm = _metadata.clone_only_token_map();
    auto range_addresses = unordered_multimap_to_unordered_map(strat.get_range_addresses(tm));

    std::unordered_multimap<range<token>, inet_address> range_sources;
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
    for (auto& desired_range : desired_ranges) {
        auto found = false;
        for (auto& x : range_addresses) {
            const range<token>& src_range = x.first;
            if (src_range.contains(desired_range, dht::tri_compare)) {
                std::unordered_set<inet_address>& addresses = x.second;
                auto preferred = snitch->get_sorted_list_by_proximity(_address, addresses);
                for (inet_address& p : preferred) {
                    range_sources.emplace(desired_range, p);
                }
                found = true;
            }
        }

        if (!found) {
            throw std::runtime_error(sprint("No sources found for %s", desired_range));
        }
    }

    return range_sources;
}

std::unordered_multimap<range<token>, inet_address>
range_streamer::get_all_ranges_with_strict_sources_for(const sstring& keyspace_name, std::vector<range<token>> desired_ranges) {
    logger.debug("{} ks={}", __func__, keyspace_name);
    assert (_tokens.empty() == false);

    auto& ks = _db.local().find_keyspace(keyspace_name);
    auto& strat = ks.get_replication_strategy();

    //Active ranges
    auto metadata_clone = _metadata.clone_only_token_map();
    auto range_addresses = unordered_multimap_to_unordered_map(strat.get_range_addresses(metadata_clone));

    //Pending ranges
    metadata_clone.update_normal_tokens(_tokens, _address);
    auto pending_range_addresses  = unordered_multimap_to_unordered_map(strat.get_range_addresses(metadata_clone));

    //Collects the source that will have its range moved to the new node
    std::unordered_multimap<range<token>, inet_address> range_sources;

    for (auto& desired_range : desired_ranges) {
        for (auto& x : range_addresses) {
            const range<token>& src_range = x.first;
            if (src_range.contains(desired_range, dht::tri_compare)) {
                auto old_endpoints = x.second;
                auto it = pending_range_addresses.find(desired_range);
                assert (it != pending_range_addresses.end());
                auto new_endpoints = it->second;

                //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                //So we need to be careful to only be strict when endpoints == RF
                if (old_endpoints.size() == strat.get_replication_factor()) {
                    std::unordered_set<inet_address> diff;
                    std::set_difference(old_endpoints.begin(), old_endpoints.end(),
                            new_endpoints.begin(), new_endpoints.end(), std::inserter(diff, diff.begin()));
                    old_endpoints = std::move(diff);
                    if (old_endpoints.size() != 1) {
                        throw std::runtime_error(sprint("Expected 1 endpoint but found ", old_endpoints.size()));
                    }
                }
                range_sources.emplace(desired_range, *(old_endpoints.begin()));
            }
        }

        //Validate
        auto nr = range_sources.count(desired_range);
        if (nr < 1) {
            throw std::runtime_error(sprint("No sources found for %s", desired_range));
        }

        if (nr > 1) {
            throw std::runtime_error(sprint("Multiple endpoints found for %s", desired_range));
        }

        inet_address source_ip = range_sources.find(desired_range)->second;
        auto& gossiper = gms::get_local_gossiper();
        auto source_state = gossiper.get_endpoint_state_for_endpoint(source_ip);
        if (gossiper.is_enabled() && source_state && !source_state->is_alive()) {
            throw std::runtime_error(sprint("A node required to move the data consistently is down (%s).  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false", source_ip));
        }
    }

    return range_sources;
}

bool range_streamer::use_strict_sources_for_ranges(const sstring& keyspace_name) {
    auto& ks = _db.local().find_keyspace(keyspace_name);
    auto& strat = ks.get_replication_strategy();
    // FIXME: DatabaseDescriptor.isReplacing()
    auto is_replacing = false;
    return !is_replacing
           && use_strict_consistency()
           && !_tokens.empty()
           && _metadata.get_all_endpoints().size() != strat.get_replication_factor();
}

void range_streamer::add_ranges(const sstring& keyspace_name, std::vector<range<token>> ranges) {
    auto ranges_for_keyspace = use_strict_sources_for_ranges(keyspace_name)
        ? get_all_ranges_with_strict_sources_for(keyspace_name, ranges)
        : get_all_ranges_with_sources_for(keyspace_name, ranges);

    if (logger.is_enabled(logging::log_level::debug)) {
        for (auto& x : ranges_for_keyspace) {
            logger.debug("{} : range {} exists on {}", _description, x.first, x.second);
        }
    }

    // TODO: share code with unordered_multimap_to_unordered_map
    std::unordered_map<inet_address, std::vector<range<token>>> tmp;
    for (auto& x : get_range_fetch_map(ranges_for_keyspace, _source_filters, keyspace_name)) {
        auto& addr = x.first;
        auto& range_ = x.second;
        auto it = tmp.find(addr);
        if (it != tmp.end()) {
            it->second.push_back(range_);
        } else {
            tmp.emplace(addr, std::vector<range<token>>{range_});
        }
    }

    if (logger.is_enabled(logging::log_level::debug)) {
        for (auto& x : tmp) {
            logger.debug("{} : range {} from source {} for keyspace {}", _description, x.second, x.first, keyspace_name);
        }
    }
    _to_fetch.emplace(keyspace_name, std::move(tmp));
}

future<streaming::stream_state> range_streamer::fetch_async() {
    for (auto& fetch : _to_fetch) {
        const auto& keyspace = fetch.first;
        for (auto& x : fetch.second) {
            auto& source = x.first;
            auto& ranges = x.second;
            auto preferred = net::get_local_messaging_service().get_preferred_ip(source);
            /* Send messages to respective folks to stream data over to me */
            if (logger.is_enabled(logging::log_level::debug)) {
                logger.debug("{}ing from {} ranges {}", _description, source, ranges);
            }
            _stream_plan.request_ranges(source, preferred, keyspace, ranges);
        }
    }

    return _stream_plan.execute();
}

std::unordered_multimap<inet_address, range<token>>
range_streamer::get_work_map(const std::unordered_multimap<range<token>, inet_address>& ranges_with_source_target,
             const sstring& keyspace) {
    auto filter = std::make_unique<dht::range_streamer::failure_detector_source_filter>(gms::get_local_failure_detector());
    std::unordered_set<std::unique_ptr<i_source_filter>> source_filters;
    source_filters.emplace(std::move(filter));
    return get_range_fetch_map(ranges_with_source_target, source_filters, keyspace);
}

} // dht
