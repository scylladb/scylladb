/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/strong_consistency/raft_resize_tracker.hh"

#include "cql3/untyped_result_set.hh"
#include "cql3/query_processor.hh"
#include "db/system_keyspace.hh"
#include <seastar/core/on_internal_error.hh>

namespace service::strong_consistency {

static logging::logger logger("raft_resize_tracker");

future<> raft_resize_tracker::stop() {
    for (auto& [gid, state] : _resize_states) {
        if (!state.end_resize.get_shared_future().available()) {
            state.end_resize.set_exception(std::runtime_error(
                    format("raft_resize_tracker is stopping, resize of group {} abandoned", gid)));
        }
    }
    _resize_states.clear();
    _child_to_parent.clear();
    return make_ready_future<>();
}

raft_resize_state& raft_resize_tracker::state_for(raft::group_id parent_gid) {
    return _resize_states[parent_gid];
}

void raft_resize_tracker::set_replacement_groups(raft::group_id parent_gid, std::vector<raft::group_id> new_gids) {
    auto& state = state_for(parent_gid);
    state.new_gids = std::move(new_gids);
    for (const auto new_gid : state.new_gids) {
        _child_to_parent[new_gid] = parent_gid;
    }
}

future<> raft_resize_tracker::restore_applied_markers(raft::group_id parent_gid) {
    // The caller has already established that the parent is being resized, from the tablet
    // metadata or from the children found in the commitlog. Create the state if this is the first
    // we hear of the resize, which after a restart it is.
    state_for(parent_gid);

    // The markers live in the parent's own row, which is on the shard hosting it - this one.
    static const auto load_cql = format("SELECT start_resize, end_resize FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    auto rs = co_await _sys_ks.query_processor().execute_internal(load_cql,
            {int16_t(this_shard_id()), parent_gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        // No marker applied yet, the resize is still in its first phase.
        co_return;
    }

    // Only the presence of the markers matters.
    const auto& row = rs->one();
    if (row.has("start_resize")) {
        mark_resize_phase(parent_gid, resize_marker_kind::start_resize);
    }
    if (row.has("end_resize")) {
        mark_resize_phase(parent_gid, resize_marker_kind::end_resize);
    }
}

void raft_resize_tracker::mark_resize_phase(raft::group_id parent_gid, resize_marker_kind kind) {
    // The markers are monotonic: marking a phase which was already reached, which happens
    // whenever a state is reloaded, is a no-op.
    auto& state = state_for(parent_gid);
    switch (kind) {
    case resize_marker_kind::start_resize:
        if (!state.start_resize) {
            logger.debug("group {}: start_resize applied, writes are now served by the children", parent_gid);
            state.start_resize = true;
        }
        return;
    case resize_marker_kind::end_resize:
        if (!state.end_resize.available()) {
            logger.debug("group {}: end_resize applied, unblocking the appliers of the children", parent_gid);
            state.end_resize.set_value();
        }
        return;
    }
}

void raft_resize_tracker::erase_resize_state(raft::group_id parent_gid) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        return;
    }
    auto& state = it->second;

    if (!state.end_resize.get_shared_future().available()) {
        // The resize ended before the parent was sealed, i.e. it was rolled back or moved away
        // from this replica. Nothing can be waiting on the promise: the children only get entries
        // to apply while the parent is being sealed, and neither a rollback nor a move interrupts
        // the sealing. Break the promise rather than drop it silently, so that a waiter we did not
        // think of fails loudly instead of hanging.
        state.end_resize.set_exception(std::runtime_error(
                format("resize of group {} ended before end_resize was applied", parent_gid)));
    }
    std::erase_if(_child_to_parent, [parent_gid] (const auto& p) { return p.second == parent_gid; });
    _resize_states.erase(parent_gid);
    logger.debug("group {}: resize is over, dropped its state", parent_gid);
}

bool raft_resize_tracker::is_resizing(raft::group_id parent_gid) const {
    return _resize_states.contains(parent_gid);
}

bool raft_resize_tracker::has_applied_end_resize(raft::group_id parent_gid) const {
    auto it = _resize_states.find(parent_gid);
    return it != _resize_states.end() && it->second.end_resize.get_shared_future().available();
}

std::optional<raft::group_id> raft_resize_tracker::get_parent_group(raft::group_id child_gid) const {
    auto it = _child_to_parent.find(child_gid);
    if (it != _child_to_parent.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool raft_resize_tracker::should_handoff_writes(raft::group_id parent_gid) const {
    auto it = _resize_states.find(parent_gid);
    return it != _resize_states.end() && it->second.start_resize;
}

std::optional<shared_future<>> raft_resize_tracker::get_parent_finished_future(raft::group_id child_gid) const {
    auto parent_gid = get_parent_group(child_gid);
    if (!parent_gid) {
        return std::nullopt;
    }
    // set_replacement_groups() creates the parent's state together with the mapping, so a child
    // always has a parent state to wait for.
    auto it = _resize_states.find(*parent_gid);
    if (it == _resize_states.end()) {
        on_internal_error(logger, format("no resize state for group {}, the parent of {}", *parent_gid, child_gid));
    }
    return it->second.end_resize.get_shared_future();
}

} // namespace service::strong_consistency
