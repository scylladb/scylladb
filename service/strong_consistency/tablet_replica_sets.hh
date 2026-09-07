/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "locator/tablets.hh"

namespace service::strong_consistency {

// Which replicas of a strongly consistent tablet a request may be sent to, at the
// stage its migration is currently in.
//
// There are two answers, and they are not the same set. Deriving one of them from the
// other is what a request must never do: the sets serve different questions, and the
// wider one contains replicas that cannot answer a read.

// Replicas that may currently be the raft group's leader.
//
// A request that has to reach the leader may be sent to any of these: one that isn't
// the leader answers not_a_leader and names it, and the request is redirected. This is
// the wider set, because it has to name every replica a redirect could legitimately
// come from - a leader missing from it would leave the request with nowhere to go.
locator::tablet_replica_set get_leader_capable_tablet_replicas(
        const locator::tablet_info& tinfo, const locator::tablet_transition_info* trinfo);

// Replicas that hold the tablet's data at this stage.
//
// A read that doesn't go through the leader must be served by one of these, and only
// these. Unlike a request that needs the leader, there is no answer such a replica can
// give that sends the request somewhere better: it reads its local storage, so a
// replica that doesn't have the data yet returns a wrong result rather than a redirect.
//
// The pending replica joins this set at sc_become_voter and not before. That stage is
// published only after the snapshot transfer completed on it, which ends in a raft read
// barrier, which ends in waiting for the local state machine to apply up to the leader's
// read index. So from sc_become_voter on, the pending replica's staleness is ordinary
// follower lag; before it, nothing bounds it.
//
// This is also the set the routing information handed to drivers is built from, so that
// where a driver is told to go and where a request is willing to be served are the same
// answer.
//
// Always a subset of get_leader_capable_tablet_replicas() for the same stage. A replica
// that holds the data can always be redirected to.
locator::tablet_replica_set get_readable_tablet_replicas(
        const locator::tablet_info& tinfo, const locator::tablet_transition_info* trinfo);

}
