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

#pragma once
#include "utils/UUID_gen.hh"
#include "service/paxos/proposal.hh"

namespace service {

namespace paxos {

// The state of the coordinator collecting responses to prepare Paxos verb.
class prepare_summary {
public:
    // Ballots of the most recent decisions, as seen by replicas. Before proceeding with its own
    // proposal, the coordinator must ensure all replicas are on the same page with the most recent
    // previous decision.
    std::unordered_map<gms::inet_address, utils::UUID> committed_ballots_by_replica;
    // Whether all replicas accepted the ballot or not. Even if there is only one reject, the
    // coordinator will not proceed, since it indicates there is a more recent ballot. It will
    // proceed, however, as soon as it receives a majority of responses.
    bool promised = true;
    // Most recent ballot, promised by any of the replicas that have responded. In case
    // a replica rejects a ballot, it responds with a newer ballot it promised, so that the proposer
    // can adjust its ballot accordingly and retry after a timeout.
    utils::UUID most_recent_promised_ballot = utils::UUID_gen::min_time_UUID();
    // Most recent decision known to any of the replicas. The coordinator
    // will use it to repair lagging replicas (make sure they learn it), if necessary, before it
    // proceeds.
    // If the decision is newer than the proposed ballot, it will reject it just the same way
    // a more recent proposal does.
    // May be empty if there is no such cell for this key in the paxos table on any of the replicas.
    std::optional<paxos::proposal> most_recent_commit;
    // Most recent proposal accepted by any of the replicas. If there is an in-progress unfinished
    // proposal with an older ballot, proposing a newer ballot has possibly hi-jacked the round from
    // the previous proposer, and the coordinator is now responsible for carrying out the round and
    // only then proposing the new ballot and new mutation. May be empty if there is no such cell for
    // this key in the paxos table on any of the replicas.
    std::optional<paxos::proposal> most_recent_proposal;
    // Value of the requested key received from participating replicas during the prepare phase.
    // May be none in case data hadn't been received before a consensus was reached or digests
    // received from different replicas didn't match.
    foreign_ptr<lw_shared_ptr<query::result>> data;

public:
    prepare_summary(size_t node_count);
    std::unordered_set<gms::inet_address> replicas_missing_most_recent_commit(schema_ptr s, std::chrono::seconds now_in_sec) const;
    void update_most_recent_promised_ballot(utils::UUID ballot);
};

} // end of namespace "paxos"

} // end of namespace "service"
