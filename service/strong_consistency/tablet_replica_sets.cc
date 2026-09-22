/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/strong_consistency/tablet_replica_sets.hh"

#include "utils/assert.hh"
#include "utils/log.hh"

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_tablet_replica_sets");

locator::tablet_replica_set get_leader_capable_tablet_replicas(
        const locator::tablet_info& tinfo, const locator::tablet_transition_info* trinfo) {
    if (!trinfo) {
        return tinfo.replicas;
    }

    // The pending replica joins the raft group as a non-voter at sc_add_nonvoter and is
    // promoted at sc_become_voter, so it can win an election only from that stage on.
    // A node that still sees an earlier stage can't be looking at a group where the
    // promotion has already happened: the topology coordinator runs it only after the
    // global barrier of sc_become_voter, which waits until no node holds a view of an
    // earlier stage.
    auto with_pending = [&] {
        auto replicas = tinfo.replicas;
        if (trinfo->pending_replica) {
            replicas.push_back(*trinfo->pending_replica);
        }
        return replicas;
    };

    switch (trinfo->stage) {
        case tablet_transition_stage::start_migration:
        case tablet_transition_stage::sc_add_nonvoter:
        case tablet_transition_stage::sc_snapshot_transfer:
            return tinfo.replicas;

        case tablet_transition_stage::sc_become_voter:
            return with_pending();

        case tablet_transition_stage::use_new:
            // The precondition of use_new is that the leaving replica is no longer a
            // member of the raft group, so it can no longer be the leader.
        case tablet_transition_stage::cleanup:
        case tablet_transition_stage::end_migration:
            return trinfo->next;

        case tablet_transition_stage::sc_rollback:
            // A rollback may be entered from sc_become_voter, where the pending replica
            // can already be a voter and the leader driving its own removal.
            return with_pending();

        case tablet_transition_stage::cleanup_target:
            // The precondition of cleanup_target is that the pending replica is no
            // longer a member of the raft group.
        case tablet_transition_stage::revert_migration:
            return tinfo.replicas;

        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
        case tablet_transition_stage::rebuild_repair:
        case tablet_transition_stage::repair:
        case tablet_transition_stage::end_repair:
        case tablet_transition_stage::restore:
            // Stages a strongly consistent tablet doesn't go through.
            break;
    }
    on_internal_error(logger, format("get_leader_capable_tablet_replicas: unexpected transition stage {} "
            "of a strongly consistent tablet", trinfo->stage));
}

locator::tablet_replica_set get_readable_tablet_replicas(
        const locator::tablet_info& tinfo, const locator::tablet_transition_info* trinfo) {
    if (!trinfo) {
        return tinfo.replicas;
    }

    switch (trinfo->stage) {
        case tablet_transition_stage::start_migration:
        case tablet_transition_stage::sc_add_nonvoter:
        case tablet_transition_stage::sc_snapshot_transfer:
            // The pending replica is a member of the group by now, but nothing bounds
            // how far behind it is until the snapshot transfer of sc_snapshot_transfer
            // has completed - which is what publishing the next stage attests to.
            return tinfo.replicas;

        case tablet_transition_stage::sc_become_voter:
        case tablet_transition_stage::use_new:
        case tablet_transition_stage::cleanup:
        case tablet_transition_stage::end_migration:
            // The leaving replica still holds the data until the cleanup, but it is left
            // out from here on so that this stays one set with the routing information
            // drivers are given, and because by use_new it has torn its raft server down.
            return trinfo->next;

        case tablet_transition_stage::sc_rollback:
        case tablet_transition_stage::cleanup_target:
        case tablet_transition_stage::revert_migration:
            // The rollback path keeps the old replica set, which never stopped holding
            // the data.
            return tinfo.replicas;

        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
        case tablet_transition_stage::rebuild_repair:
        case tablet_transition_stage::repair:
        case tablet_transition_stage::end_repair:
        case tablet_transition_stage::restore:
            // Stages a strongly consistent tablet doesn't go through.
            break;
    }
    on_internal_error(logger, format("get_readable_tablet_replicas: unexpected transition stage {} "
            "of a strongly consistent tablet", trinfo->stage));
}

}
