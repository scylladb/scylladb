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

#include "core/sstring.hh"
#include "util/serialization.hh"
#include "gms/gossip_digest.hh"

namespace gms {

// serialization helper for std::vector<gossip_digest>
class gossip_digest_serialization_helper {
public:
    static void serialize(std::ostream& out, const std::vector<gossip_digest>& digests) {
        serialize_int32(out, int32_t(digests.size()));
        for (auto& digest : digests) {
           digest.serialize(out);
        }
    }

    static std::vector<gossip_digest> deserialize(std::istream& in) {
        int32_t size = deserialize_int32(in);
        std::vector<gossip_digest> digests;
        for (int32_t i = 0; i < size; ++i)
            digests.push_back(gossip_digest::deserialize(in));
        return digests;
    }

    static size_t serialized_size(const std::vector<gossip_digest>& digests) {
        size_t size = serialize_int32_size;
        for (auto& digest : digests)
            size += digest.serialized_size();
        return size;
    }
};


/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
class gossip_digest_syn {
private:
    const sstring _cluster_id;
    const sstring _partioner;
    const std::vector<gossip_digest> _digests;
public:
    gossip_digest_syn(sstring id, sstring p, std::vector<gossip_digest> digests)
        : _cluster_id(id)
        , _partioner(p)
        , _digests(digests) {
    }

    std::vector<gossip_digest> get_gossip_digests() {
        return _digests;
    }

    // The following replaces GossipDigestSynSerializer from the Java code
    void serialize(std::ostream& out) const {
        serialize_string(out, _cluster_id);
        serialize_string(out, _partioner);
        gossip_digest_serialization_helper::serialize(out, _digests);
    }

    static gossip_digest_syn deserialize(std::istream& in) {
        sstring cluster_id = deserialize_string(in);
        sstring partioner = deserialize_string(in);
        std::vector<gossip_digest> digests = gossip_digest_serialization_helper::deserialize(in);
        return gossip_digest_syn(cluster_id, partioner, std::move(digests));
    }

    size_t serialized_size() const {
        return serialize_string_size(_cluster_id) + serialize_string_size(_partioner) +
               gossip_digest_serialization_helper::serialized_size(_digests);
    }
};

}
