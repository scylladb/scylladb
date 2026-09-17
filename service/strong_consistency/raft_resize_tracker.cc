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

namespace service::strong_consistency {

static logging::logger logger("raft_resize_tracker");

future<> raft_resize_tracker::stop() {
    _resize_states.clear();
    _child_to_parent.clear();
    return make_ready_future<>();
}

void raft_resize_tracker::set_replacement_groups(raft::group_id parent_gid, const utils::small_vector<raft::group_id, 2>& new_gids) {
    _resize_states.try_emplace(parent_gid);
    for (const auto new_gid : new_gids) {
        _child_to_parent[new_gid] = parent_gid;
    }
}

future<> raft_resize_tracker::restore_applied_markers(raft::group_id parent_gid) {
    // The caller has already established that the parent is being resized, from the tablet
    // metadata or from the children found in the commitlog. Create the state if this is the first
    // we hear of the resize, which after a restart it is.
    //
    // Called from the commitlog replay, this runs before update() has observed anything. A parent
    // whose group is then never started here leaves the entry behind until stop(), which breaks the
    // promise cleanly. That happens when the table is dropped, or the tablet is gone from the
    // metadata by the time update() first runs. Memory only, and bounded by the number of groups
    // the replay found.
    _resize_states.try_emplace(parent_gid);

    // The markers live in the parent's own row, which is on the shard hosting it - this one. A
    // marker's cell carries the marker's timestamp as its write time (see resize_marker), so the
    // row gives back the markers as they were applied.
    static const auto load_cql = format("SELECT start_resize, end_resize, WRITETIME(start_resize) AS start_ts, "
            "WRITETIME(end_resize) AS end_ts FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    auto rs = co_await _sys_ks.query_processor().execute_internal(load_cql,
            {int16_t(this_shard_id()), parent_gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        // No marker applied yet, the resize is still in its first phase.
        co_return;
    }

    // Only the presence of a marker is read, the timestamp comes from its cell.
    const auto& row = rs->one();
    if (row.has("start_resize")) {
        mark_resize_phase(parent_gid, resize_marker{
            .kind = resize_marker_kind::start_resize,
            .timestamp = row.get_as<api::timestamp_type>("start_ts"),
        });
    }
    if (row.has("end_resize")) {
        mark_resize_phase(parent_gid, resize_marker{
            .kind = resize_marker_kind::end_resize,
            .timestamp = row.get_as<api::timestamp_type>("end_ts"),
        });
    }
}

void raft_resize_tracker::mark_resize_phase(raft::group_id parent_gid, const resize_marker& marker) {
    // A marker of a resize with no state here is moot. The state is dropped when the resize ends,
    // and only a call left over from a finalization which is already over brings one in
    // afterwards.
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        logger.debug("group {}: a marker was applied after its resize ended here, ignoring it", parent_gid);
        return;
    }
    // The markers are monotonic: marking a phase which was already reached, which happens
    // whenever a state is reloaded, is a no-op.
    auto& state = it->second;
    switch (marker.kind) {
    case resize_marker_kind::start_resize:
        if (!state.start_resize) {
            logger.debug("group {}: start_resize applied, writes are now served by the children", parent_gid);
            state.start_resize = true;
        }
        return;
    case resize_marker_kind::end_resize:
        if (!state.end_resize) {
            logger.debug("group {}: end_resize applied, releasing the appliers of its {} children", parent_gid, state.children.size());
            state.end_resize = true;
            state.end_resize_timestamp = marker.timestamp;
            for (auto* child : std::exchange(state.children, {})) {
                child->enable();
            }
        }
        return;
    }
}

void raft_resize_tracker::erase_resize_state(raft::group_id parent_gid) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        return;
    }
    if (!it->second.end_resize) {
        // A finalization applies end_resize on every replica before the tablet map is replaced,
        // so only a shutdown or a table drop gets here, and only then is a child still parked.
        // Enabling it lets its applier fiber exit; what it applies on the way out is harmless,
        // because the children are torn down with the parent and nothing reads them meanwhile,
        // and a handed-off write carries a timestamp above every write of the parent, so the
        // order the two are applied in does not change what the table ends up holding.
        logger.debug("group {}: resize ended before end_resize was applied, enabling its {} children",
                parent_gid, it->second.children.size());
        for (auto* child : std::exchange(it->second.children, {})) {
            child->enable();
        }
    }
    _resize_states.erase(it);
    logger.debug("group {}: resize is over, dropped its state", parent_gid);
}

void raft_resize_tracker::erase_group(raft::group_id gid) {
    const auto it = _child_to_parent.find(gid);
    if (it == _child_to_parent.end()) {
        // The parent itself, so its resize is over either way.
        erase_resize_state(gid);
        return;
    }
    // A child drops only its own mapping. Its parent is torn down on its own, together with it or
    // after it, and dropping the state is that teardown's part.
    logger.debug("group {}: dropped the mapping of its child {}", it->second, gid);
    _child_to_parent.erase(it);
}

bool raft_resize_tracker::is_resizing(raft::group_id parent_gid) const {
    return _resize_states.contains(parent_gid);
}

bool raft_resize_tracker::has_applied_end_resize(raft::group_id parent_gid) const {
    auto it = _resize_states.find(parent_gid);
    return it != _resize_states.end() && it->second.end_resize;
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

std::optional<api::timestamp_type> raft_resize_tracker::parent_end_resize_timestamp(raft::group_id child_gid) const {
    const auto parent_gid = get_parent_group(child_gid);
    if (!parent_gid) {
        return std::nullopt;
    }
    const auto it = _resize_states.find(*parent_gid);
    if (it == _resize_states.end() || !it->second.end_resize) {
        return std::nullopt;
    }
    return it->second.end_resize_timestamp;
}

void raft_resize_tracker::register_child(raft::group_id parent_gid, tablet_state_machine& child) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        // The resize is over on this replica, and with it the parent's log is applied here in
        // full.
        logger.debug("group {}: has no resize state, a child of it has nothing to wait for", parent_gid);
        child.enable();
        return;
    }
    if (it->second.end_resize) {
        child.enable();
        return;
    }
    it->second.children.push_back(&child);
}

void raft_resize_tracker::unregister_child(raft::group_id parent_gid, tablet_state_machine& child) {
    // The state may be gone already, with the child released or abandoned when it went.
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        return;
    }
    std::erase(it->second.children, &child);
}

} // namespace service::strong_consistency
