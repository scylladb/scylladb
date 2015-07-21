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

 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "types.hh"
#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"

namespace gms {

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
class gossip_digest_ack {
private:
    using inet_address = gms::inet_address;
    std::vector<gossip_digest> _digests;
    std::map<inet_address, endpoint_state> _map;
public:
    gossip_digest_ack() {
    }

    gossip_digest_ack(std::vector<gossip_digest> d, std::map<inet_address, endpoint_state> m)
        : _digests(std::move(d))
        , _map(std::move(m)) {
    }

    std::vector<gossip_digest> get_gossip_digest_list() {
        return _digests;
    }

    std::map<inet_address, endpoint_state> get_endpoint_state_map() {
        return _map;
    }

    // The following replaces GossipDigestAckSerializer from the Java code
    void serialize(bytes::iterator& out) const {
        // 1) Digest
        gossip_digest_serialization_helper::serialize(out, _digests);
        // 2) Map size
        serialize_int32(out, int32_t(_map.size()));
        // 3) Map contents
        for (auto& entry : _map) {
            const inet_address& ep = entry.first;
            const endpoint_state& st = entry.second;
            ep.serialize(out);
            st.serialize(out);
        }
    }

    static gossip_digest_ack deserialize(bytes_view& v) {
        // 1) Digest
        std::vector<gossip_digest> _digests = gossip_digest_serialization_helper::deserialize(v);
        // 2) Map size
        int32_t map_size = read_simple<int32_t>(v);
        // 3) Map contents
        std::map<inet_address, endpoint_state> _map;
        for (int32_t i = 0; i < map_size; ++i) {
            inet_address ep = inet_address::deserialize(v);
            endpoint_state st = endpoint_state::deserialize(v);
            _map.emplace(std::move(ep), std::move(st));
        }
        return gossip_digest_ack(std::move(_digests), std::move(_map));
    }

    size_t serialized_size() const {
        size_t size = gossip_digest_serialization_helper::serialized_size(_digests);
        size += serialize_int32_size;
        for (auto& entry : _map) {
            const inet_address& ep = entry.first;
            const endpoint_state& st = entry.second;
            size += ep.serialized_size() + st.serialized_size();
        }
        return size;
    }

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_ack& ack);
};

}
