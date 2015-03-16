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
#include "core/sstring.hh"
#include "util/serialization.hh"
#include "gms/inet_address.hh"

namespace gms {

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
class gossip_digest { // implements Comparable<GossipDigest>
private:
    using inet_address = gms::inet_address;
    const inet_address _endpoint;
    const int32_t _generation;
    const int32_t _max_version;
public:
    gossip_digest(inet_address ep, int32_t gen, int32_t version)
        : _endpoint(ep)
        , _generation(gen)
        , _max_version(version) {
    }

    inet_address get_endpoint() {
        return _endpoint;
    }

    int32_t get_generation() {
        return _generation;
    }

    int32_t get_max_version() {
        return _max_version;
    }

    int32_t compare_to(gossip_digest d) {
        if (_generation != d.get_generation()) {
            return (_generation - d.get_generation());
        }
        return (_max_version - d.get_max_version());
    }

    friend inline std::ostream& operator<<(std::ostream& os, const gossip_digest& d) {
        return os << d._endpoint << ":" << d._generation << ":" << d._max_version;
    }

    // The following replaces GossipDigestSerializer from the Java code
    void serialize(bytes::iterator& out) const {
        _endpoint.serialize(out);
        serialize_int32(out, _generation);
        serialize_int32(out, _max_version);
    }

    static gossip_digest deserialize(bytes_view& v) {
        auto endpoint = inet_address::deserialize(v);
        auto generation = read_simple<int32_t>(v);
        auto max_version = read_simple<int32_t>(v);
        return gossip_digest(endpoint, generation, max_version);
    }

    size_t serialized_size() const {
        return _endpoint.serialized_size() + serialize_int32_size + serialize_int32_size;
    }
}; // class gossip_digest

// serialization helper for std::vector<gossip_digest>
class gossip_digest_serialization_helper {
public:
    static void serialize(bytes::iterator& out, const std::vector<gossip_digest>& digests) {
        serialize_int32(out, int32_t(digests.size()));
        for (auto& digest : digests) {
           digest.serialize(out);
        }
    }

    static std::vector<gossip_digest> deserialize(bytes_view& v) {
        int32_t size = read_simple<int32_t>(v);
        std::vector<gossip_digest> digests;
        for (int32_t i = 0; i < size; ++i)
            digests.push_back(gossip_digest::deserialize(v));
        return digests;
    }

    static size_t serialized_size(const std::vector<gossip_digest>& digests) {
        size_t size = serialize_int32_size;
        for (auto& digest : digests)
            size += digest.serialized_size();
        return size;
    }
};

} // namespace gms
