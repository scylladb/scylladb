/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "service/paxos/prepare_summary.hh"

namespace service {

namespace paxos {

prepare_summary::prepare_summary(size_t node_count) {
    committed_ballots_by_replica.reserve(node_count);
}

void prepare_summary::update_most_recent_promised_ballot(utils::UUID ballot) {
    // In case of clock skew, another node could be proposing with ballot that are quite a bit
    // older than our own. In that case, we record the more recent commit we've received to make
    // sure we re-prepare on an older ballot.
    if (ballot.timestamp() > most_recent_promised_ballot.timestamp()) {
        most_recent_promised_ballot = ballot;
    }
}

std::unordered_set<gms::inet_address>
prepare_summary::replicas_missing_most_recent_commit(schema_ptr s, std::chrono::seconds now_in_sec) const {
    std::unordered_set<gms::inet_address> replicas;
    // In general, we need every replica that has answered to the prepare (a quorum) to agree on the MRC (see
    // comment in storage_proxy::begin_and_repair_paxos(), but basically we need to make sure at least a quorum of nodes
    // have learned a commit before committing a new one, otherwise that previous commit is not guaranteed to have reached a
    // quorum and a further commit may proceed on incomplete information).
    // However, if that commit is too old, it may have been expired from some of the replicas paxos table (we don't
    // keep the paxos state forever or that could grow unchecked), and we could end up in some infinite loop as
    // explained on CASSANDRA-12043. To avoid that, we ignore a MRC that is too old, i.e. older than the TTL we set
    // on paxos tables. For such old commit, we rely on repair to ensure the commit has indeed be
    // propagated to all nodes.
    const std::chrono::seconds paxos_ttl_sec(s->paxos_grace_seconds());
    if (!most_recent_commit ||
            utils::UUID_gen::unix_timestamp_in_sec(most_recent_commit->ballot) + paxos_ttl_sec < now_in_sec) {
        return replicas;
    }

    for (const auto& it: committed_ballots_by_replica) {
        if (it.second != most_recent_commit->ballot) {
            replicas.insert(it.first);
        }
    }
    return replicas;
}

} // end of namespace "paxos"

} // end of namespace "service"
