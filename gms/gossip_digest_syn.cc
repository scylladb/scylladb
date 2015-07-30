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

#include "gms/gossip_digest_syn.hh"
#include <ostream>

namespace gms {

std::ostream& operator<<(std::ostream& os, const gossip_digest_syn& syn) {
    os << "cluster_id:" << syn._cluster_id << ",partioner:" << syn._partioner << ",";
    os << "digests:{";
    for (auto& d : syn._digests) {
        os << d << " ";
    }
    return os << "}";
}

void gossip_digest_syn::serialize(bytes::iterator& out) const {
    serialize_string(out, _cluster_id);
    serialize_string(out, _partioner);
    gossip_digest_serialization_helper::serialize(out, _digests);
}

gossip_digest_syn gossip_digest_syn::deserialize(bytes_view& v) {
    sstring cluster_id = read_simple_short_string(v);
    sstring partioner = read_simple_short_string(v);
    std::vector<gossip_digest> digests = gossip_digest_serialization_helper::deserialize(v);
    return gossip_digest_syn(cluster_id, partioner, std::move(digests));
}

size_t gossip_digest_syn::serialized_size() const {
    return serialize_string_size(_cluster_id) + serialize_string_size(_partioner) +
           gossip_digest_serialization_helper::serialized_size(_digests);
}

} // namespace gms
