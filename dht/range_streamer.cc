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

namespace dht {

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

} // dht
