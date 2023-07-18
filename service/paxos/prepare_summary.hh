/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once
#include "utils/UUID_gen.hh"
#include "service/paxos/proposal.hh"
#include "inet_address_vectors.hh"
#include "query-result.hh"

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
    inet_address_vector_replica_set replicas_missing_most_recent_commit(schema_ptr s, std::chrono::seconds now_in_sec) const;
    void update_most_recent_promised_ballot(utils::UUID ballot);
};

} // end of namespace "paxos"

} // end of namespace "service"
