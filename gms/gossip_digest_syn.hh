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
#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"

namespace gms {

/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
class gossip_digest_syn {
private:
    sstring _cluster_id;
    sstring _partioner;
    std::vector<gossip_digest> _digests;
public:
    gossip_digest_syn() {
    }

    gossip_digest_syn(sstring id, sstring p, std::vector<gossip_digest> digests)
        : _cluster_id(id)
        , _partioner(p)
        , _digests(digests) {
    }

    sstring cluster_id() const {
        return _cluster_id;
    }

    sstring partioner() const {
        return _partioner;
    }

    std::vector<gossip_digest> get_gossip_digests() {
        return _digests;
    }

    // The following replaces GossipDigestSynSerializer from the Java code
    void serialize(bytes::iterator& out) const;

    static gossip_digest_syn deserialize(bytes_view& v);

    size_t serialized_size() const;

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_syn& syn);
};

}
