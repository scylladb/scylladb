/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/strong_consistency/raft_resize_coordinator.hh"

#include "cql3/untyped_result_set.hh"
#include "cql3/query_processor.hh"
#include "db/system_keyspace.hh"
#include "mutation/frozen_mutation.hh"
#include "replica/database.hh"
#include "schema/schema_registry.hh"
#include "service/raft/raft_timeout.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "utils/exceptions.hh"

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

namespace service::strong_consistency {

using namespace std::chrono_literals;

static logging::logger logger("raft_resize_coordinator");

bool is_resize_mutation(const schema& s) {
    return s.id() == db::system_keyspace::raft_groups_metadata()->id();
}

future<> apply_resize_mutation(replica::database& db, const frozen_mutation& m, schema_ptr s) {
    auto erm = db.find_column_family(s->id()).get_effective_replication_map();
    const auto shards = erm->shard_for_writes(*s, m.token(*s));
    if (shards.size() != 1) {
        on_internal_error(logger, format("resize mutation has {} target shards, expected 1", shards.size()));
    }
    const auto shard = *shards.begin();
    if (shard == this_shard_id()) {
        co_await db.apply_in_memory(m, s, db::rp_handle(), db::no_timeout,
                db::noop_large_data_guardrail::instance());
        co_return;
    }
    co_await db.container().invoke_on(shard, [&m, gs = global_schema_ptr(s)] (replica::database& db) -> future<> {
        co_await db.apply_in_memory(m, gs.get(), db::rp_handle(), db::no_timeout,
                db::noop_large_data_guardrail::instance());
    });
}

future<> raft_resize_coordinator::stop() {
    for (auto& [gid, state] : _resize_states) {
        // Every colocator references this coordinator and the raft servers of groups_manager,
        // so it must have been joined by erase_resize_state() while its groups were torn down,
        // which happens in groups_manager::stop(), before this one. Erasing the states below
        // would otherwise destroy a future which a running fiber is about to resolve.
        if (!state.leader_colocator.available()) {
            on_internal_error(logger, format(
                    "raft_resize_coordinator is stopping while the leader colocator of group {} is still running", gid));
        }
        if (!state.groups_resized.get_shared_future().available()) {
            state.groups_resized.set_exception(std::runtime_error(
                    format("raft_resize_coordinator is stopping, resize of group {} abandoned", gid)));
        }
        state.leader_changed.broken();
    }
    _resize_states.clear();
    _child_to_parent.clear();
    return make_ready_future<>();
}

void raft_resize_coordinator::announce_resize(locator::global_tablet_id tablet, raft::group_id parent_gid, std::vector<raft::group_id> new_gids,
        groups_manager& gm) {
    auto& state = _resize_states[parent_gid];
    state.new_gids = std::move(new_gids);
    for (const auto new_gid : state.new_gids) {
        _child_to_parent[new_gid] = parent_gid;
    }
    state.leader_colocator_as.emplace();
    state.leader_colocator = leader_colocator(tablet, parent_gid, gm, *state.leader_colocator_as);
}

future<> raft_resize_coordinator::reload_group_resize_state(raft::group_id parent_gid) {
    // Only called for a group already known to be resizing, either from the tablet metadata or
    // from its own log, so this is also the point where a restarting replica learns about it.
    auto& state = _resize_states[parent_gid];

    static const auto load_cql = format("SELECT redirect_writes, groups_resized FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT_GROUPS_METADATA);
    auto rs = co_await _sys_ks.query_processor().execute_internal(load_cql, {parent_gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        // No marker applied yet, the resize is still in its first phase.
        co_return;
    }

    // Only the presence of the markers matters. Both are monotonic: reloading must never undo
    // a redirect_writes which fast_forward_redirect_writes() set ahead of its own apply.
    const auto& row = rs->one();
    if (row.has("redirect_writes") && !state.redirect_writes) {
        logger.debug("group {}: redirect_writes applied, writes are now served by the new groups", parent_gid);
        state.redirect_writes = true;
    }
    if (row.has("groups_resized") && !state.groups_resized.available()) {
        logger.debug("group {}: groups_resized applied, unblocking the appliers of the new groups", parent_gid);
        state.groups_resized.set_value();
    }
}

future<> raft_resize_coordinator::erase_resize_state(raft::group_id parent_gid) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        co_return;
    }
    auto& state = it->second;
    if (state.leader_colocator_as) {
        state.leader_colocator_as->request_abort();
    }
    co_await std::exchange(state.leader_colocator, make_ready_future<>());
    state.leader_colocator_as.reset();

    if (!state.groups_resized.get_shared_future().available()) {
        // The resize ended without groups_resized being applied, i.e. it was rolled back or
        // moved away from this replica before the group was sealed. Nothing can be waiting on
        // the promise: the groups replacing the parent only get entries to apply while it is
        // being sealed, which is never interrupted by either. Break it rather than drop it
        // silently, so that a waiter we did not think of fails loudly instead of hanging.
        state.groups_resized.set_exception(std::runtime_error(
                format("resize of group {} ended before groups_resized was applied", parent_gid)));
    }
    state.leader_changed.broken();
    std::erase_if(_child_to_parent, [parent_gid] (const auto& p) { return p.second == parent_gid; });
    _resize_states.erase(parent_gid);
    logger.debug("group {}: resize is over, dropped its state", parent_gid);
    co_return;
}

bool raft_resize_coordinator::is_resizing(raft::group_id parent_gid) const {
    return _resize_states.contains(parent_gid);
}

raft_resize_state& raft_resize_coordinator::get_resize_state(raft::group_id parent_gid) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        on_internal_error(logger, format("no resize state for group {}", parent_gid));
    }
    return it->second;
}

void raft_resize_coordinator::fast_forward_redirect_writes(raft::group_id parent_gid) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        on_internal_error(logger, format("no resize state for group {}", parent_gid));
    }
    it->second.redirect_writes = true;
}

std::optional<raft::group_id> raft_resize_coordinator::get_parent_group(raft::group_id child_gid) const {
    auto it = _child_to_parent.find(child_gid);
    if (it != _child_to_parent.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool raft_resize_coordinator::should_redirect_writes(raft::group_id parent_gid) const {
    auto it = _resize_states.find(parent_gid);
    return it != _resize_states.end() && it->second.redirect_writes;
}

std::optional<shared_future<>> raft_resize_coordinator::get_parent_finished_future(raft::group_id child_gid) const {
    auto parent_gid = get_parent_group(child_gid);
    if (!parent_gid) {
        return std::nullopt;
    }
    // announce_resize() creates the parent's state together with the mapping, so a group
    // which has a parent always has one to wait for.
    auto it = _resize_states.find(*parent_gid);
    if (it == _resize_states.end()) {
        on_internal_error(logger, format("no resize state for group {}, the parent of {}", *parent_gid, child_gid));
    }
    return it->second.groups_resized.get_shared_future();
}

void raft_resize_coordinator::notify_leader_change(raft::group_id gid) {
    // A group replacing another one is registered under its parent, so that a single state
    // covers a whole resize.
    const auto state_gid = get_parent_group(gid).value_or(gid);
    auto it = _resize_states.find(state_gid);
    if (it == _resize_states.end()) {
        return;
    }
    ++it->second.leader_change_seq;
    it->second.leader_changed.broadcast();
}

future<raft_resize_coordinator::colocation_status> raft_resize_coordinator::colocate_leaders(
        raft_server& parent, raft::group_id parent_gid, const std::vector<raft::group_id>& new_gids, groups_manager& gm) {
    // Writes are only redirected to a new group while its leader is co-located with the parent
    // leader, so that the timestamps generated for the redirected writes keep coming from the
    // same clock as the ones already committed in the parent group. The initial leader of a new
    // group is chosen to be co-located with the parent leader (see the fast bootstrap seed in
    // groups_manager::start_raft_group()), but either side may hold an election at any point
    // during the resize. Until the leaders are brought back together, writes to the affected
    // token range cannot make progress - coordinator::mutate() keeps bouncing them between the
    // two groups rather than committing them on a non-co-located leader.
    const auto parent_leader = parent.server().current_leader();
    if (!parent_leader) {
        // There is nothing to co-locate with yet. The election has to complete before writes
        // can be served by the parent group anyway.
        co_return colocation_status::parent_leader_unknown;
    }

    auto status = colocation_status::colocated;
    for (const auto new_gid : new_gids) {
        auto child = gm.try_acquire_server(new_gid);
        if (!child) {
            // The new group isn't running here yet. Its leader_info_updater signals us as soon
            // as it starts.
            status = colocation_status::awaiting_leader_change;
            continue;
        }
        const auto child_leader = child->server().current_leader();
        if (child_leader == parent_leader) {
            continue;
        }
        if (!child_leader) {
            // An election is in progress in the new group, it may still elect the co-located
            // replica on its own.
            status = colocation_status::awaiting_leader_change;
            continue;
        }
        // Only the current leader of the new group can hand its leadership over. Every replica
        // of the parent watches this, so the transfer is performed by whichever of them leads
        // the new group.
        if (child_leader != child->server().id()) {
            status = colocation_status::awaiting_leader_change;
            continue;
        }

        logger.info("colocate_leaders: group {} is led by this node while its parent {} is led by {}, "
            "transferring the leadership", new_gid, parent_gid, parent_leader);
        constexpr auto transfer_timeout = raft::logical_clock::duration(std::chrono::seconds(5) / raft_tick_interval);
        auto result = co_await coroutine::as_future(
            child->server().stepdown(transfer_timeout, parent_leader));
        if (result.failed()) {
            auto ex = result.get_exception();
            if (try_catch<raft::no_other_voting_member>(ex)) {
                // Single-voter group: leadership cannot be transferred and the leaders cannot be
                // co-located any other way. Unreachable in practice, because with a single voter
                // the parent and the new groups all share it.
                on_internal_error(logger, format("colocate_leaders: cannot co-locate group {} "
                    "with parent {} leader {}: {}", new_gid, parent_gid, parent_leader, ex));
            }
            // Transient: the transfer times out if the target doesn't catch up with the log in
            // time, and fails outright if a stepdown is already in progress.
            logger.info("colocate_leaders: leadership transfer of group {} to parent {} leader {} failed: {}",
                new_gid, parent_gid, parent_leader, ex);
            co_return colocation_status::transfer_failed;
        }
        // Don't transfer the remaining groups in the same round: each transfer may take up to
        // transfer_timeout, which would make this call outlive its caller's deadline. The
        // caller re-checks anyway, so the next round picks the remaining ones up.
        co_return colocation_status::transfer_done;
    }
    co_return status;
}

future<> raft_resize_coordinator::leader_colocator(locator::global_tablet_id tablet,
        raft::group_id parent_gid, groups_manager& gm, abort_source& as) {
    try {
        // groups_manager::update() starts this fiber synchronously, in the middle of committing
        // a token metadata change. Get off its stack before touching any raft server - the
        // first round may already transfer a leadership.
        co_await yield();

        // Announced before this fiber was spawned. The reference, and the new_gids it holds,
        // stay valid for as long as the fiber runs: a state is only erased once its colocator
        // has exited, and the last of them exits before this coordinator is stopped.
        auto& resize_state = get_resize_state(parent_gid);

        logger.debug("leader_colocator({}-{}): maintaining co-location with {}",
            tablet, parent_gid, resize_state.new_gids);

        // Runs until aborted, which erase_resize_state() does as soon as the resize leaves the
        // tablet metadata - be it finalized, rolled back, or moved away from this replica.
        while (true) {
            // Sampled before the check at the end, so that a leadership change which happens while
            // we are checking is not slept through.
            const auto seq = resize_state.leader_change_seq;

            auto parent = gm.try_acquire_server(parent_gid);
            if (!parent) {
                // Transient while the raft server is (re)starting. It also fails permanently
                // once the group is scheduled for deletion, but that aborts `as` too.
                co_await sleep_abortable(10ms, as);
                continue;
            }

            const auto status = co_await colocate_leaders(*parent, parent_gid, resize_state.new_gids, gm);
            switch (status) {
            case colocation_status::transfer_done:
                continue;
            case colocation_status::transfer_failed:
                co_await sleep_abortable(10ms, as);
                continue;
            case colocation_status::parent_leader_unknown:
                // A follower learning about a new leader is not a raft state change, so it does
                // not signal leader_changed. Wait for the election to conclude instead.
                co_await parent->server().wait_for_leader(&as);
                continue;
            case colocation_status::colocated:
            case colocation_status::awaiting_leader_change:
                break;
            }

            // Nothing to do until some leadership moves. Note that this covers both a new group
            // electing us as its leader and this node losing the leadership of the group being
            // resized - leader_info_updater() of every group taking part in the resize signals
            // leader_changed.
            parent.reset();
            while (resize_state.leader_change_seq == seq) {
                co_await wait_with_abort_source(resize_state.leader_changed, as);
            }
        }
    } catch (...) {
        if (as.abort_requested()) {
            // The resize is over, or the group is being stopped.
            logger.debug("leader_colocator({}-{}): stopping", tablet, parent_gid);
            co_return;
        }
        // Losing this fiber only means that the writes of this group may stall until the resize
        // completes, so don't bring the node down over it.
        logger.warn("leader_colocator({}-{}): stopped with an error: {}",
            tablet, parent_gid, std::current_exception());
    }
}

} // namespace service::strong_consistency
