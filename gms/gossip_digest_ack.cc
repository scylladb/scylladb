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

#include "gms/gossip_digest_ack.hh"
#include <ostream>

namespace gms {

std::ostream& operator<<(std::ostream& os, const gossip_digest_ack& ack) {
    os << "digests:{";
    for (auto& d : ack._digests) {
        os << d << " ";
    }
    os << "} ";
    os << "endpoint_state:{";
    for (auto& d : ack._map) {
        os << "[" << d.first << "->" << d.second << "]";
    }
    return os << "}";
}

} // namespace gms
