/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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

inet_address_vector_replica_set
prepare_summary::replicas_missing_most_recent_commit(schema_ptr s, std::chrono::seconds now_in_sec) const {
    inet_address_vector_replica_set replicas;
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
            replicas.push_back(it.first);
        }
    }
    return replicas;
}

} // end of namespace "paxos"

} // end of namespace "service"
