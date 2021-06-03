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
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/sstring.hh>
#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"
#include "utils/chunked_vector.hh"

namespace gms {

/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
class gossip_digest_syn {
private:
    sstring _cluster_id;
    sstring _partioner;
    utils::chunked_vector<gossip_digest> _digests;
public:
    gossip_digest_syn() {
    }

    gossip_digest_syn(sstring id, sstring p, utils::chunked_vector<gossip_digest> digests)
        : _cluster_id(std::move(id))
        , _partioner(std::move(p))
        , _digests(std::move(digests)) {
    }

    sstring cluster_id() const {
        return _cluster_id;
    }

    sstring partioner() const {
        return _partioner;
    }

    sstring get_cluster_id() const {
        return cluster_id();
    }

    sstring get_partioner() const {
        return partioner();
    }

    const utils::chunked_vector<gossip_digest>& get_gossip_digests() const {
        return _digests;
    }

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_syn& syn);
};

}
