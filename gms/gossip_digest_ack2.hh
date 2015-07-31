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
 * This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */
class gossip_digest_ack2 {
private:
    using inet_address = gms::inet_address;
    std::map<inet_address, endpoint_state> _map;
public:
    gossip_digest_ack2() {
    }

    gossip_digest_ack2(std::map<inet_address, endpoint_state> m)
        : _map(std::move(m)) {
    }

    std::map<inet_address, endpoint_state>& get_endpoint_state_map() {
        return _map;
    }

    // The following replaces GossipDigestAck2Serializer from the Java code
    void serialize(bytes::iterator& out) const;

    static gossip_digest_ack2 deserialize(bytes_view& v);

    size_t serialized_size() const;

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_ack2& ack2);
};

} // gms
