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

#pragma once

#include "core/sstring.hh"
#include "utils/serialization.hh"
#include "gms/inet_address.hh"

namespace gms {

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
class gossip_digest { // implements Comparable<GossipDigest>
private:
    using inet_address = gms::inet_address;
    inet_address _endpoint;
    int32_t _generation;
    int32_t _max_version;
public:
    gossip_digest()
        : _endpoint(0)
        , _generation(0)
        , _max_version(0) {
    }

    gossip_digest(inet_address ep, int32_t gen, int32_t version)
        : _endpoint(ep)
        , _generation(gen)
        , _max_version(version) {
    }

    inet_address get_endpoint() const {
        return _endpoint;
    }

    int32_t get_generation() const {
        return _generation;
    }

    int32_t get_max_version() const {
        return _max_version;
    }

    int32_t compare_to(gossip_digest d) const {
        if (_generation != d.get_generation()) {
            return (_generation - d.get_generation());
        }
        return (_max_version - d.get_max_version());
    }

    friend bool operator<(const gossip_digest& x, const gossip_digest& y) {
        if (x._generation != y._generation) {
            return x._generation < y._generation;
        }
        return x._max_version <  y._max_version;
    }

    friend inline std::ostream& operator<<(std::ostream& os, const gossip_digest& d) {
        return os << d._endpoint << ":" << d._generation << ":" << d._max_version;
    }
}; // class gossip_digest

} // namespace gms
