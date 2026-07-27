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

#include <seastar/core/on_internal_error.hh>

namespace service::strong_consistency {

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
        if (!state.groups_resized.get_shared_future().available()) {
            state.groups_resized.set_exception(std::runtime_error(
                    format("raft_resize_coordinator is stopping, resize of group {} abandoned", gid)));
        }
    }
    _resize_states.clear();
    _child_to_parent.clear();
    return make_ready_future<>();
}

void raft_resize_coordinator::announce_resize(raft::group_id parent_gid, std::vector<raft::group_id> new_gids) {
    auto& state = _resize_states[parent_gid];
    state.new_gids = std::move(new_gids);
    for (const auto new_gid : state.new_gids) {
        _child_to_parent[new_gid] = parent_gid;
    }
}

future<> raft_resize_coordinator::reload_group_resize_state(raft::group_id parent_gid) {
    // Only called for a group already known to be resizing, either from the tablet metadata or
    // from its own log, so this is also the point where a restarting replica learns about it.
    auto& state = _resize_states[parent_gid];

    static const auto load_cql = format("SELECT groups_resized FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT_GROUPS_METADATA);
    auto rs = co_await _sys_ks.query_processor().execute_internal(load_cql, {parent_gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        // No marker applied yet, the resize is still in its first phase.
        co_return;
    }

    // Only the presence of the marker matters.
    const auto& row = rs->one();
    if (row.has("groups_resized") && !state.groups_resized.available()) {
        logger.debug("group {}: groups_resized applied, unblocking the appliers of the new groups", parent_gid);
        state.groups_resized.set_value();
    }
}

void raft_resize_coordinator::erase_resize_state(raft::group_id parent_gid) {
    auto it = _resize_states.find(parent_gid);
    if (it == _resize_states.end()) {
        return;
    }
    auto& state = it->second;

    if (!state.groups_resized.get_shared_future().available()) {
        // The resize ended without groups_resized being applied, i.e. it was rolled back or
        // moved away from this replica. Break the promise rather than drop it silently, so
        // that a waiter we did not think of fails loudly instead of hanging.
        state.groups_resized.set_exception(std::runtime_error(
                format("resize of group {} ended before groups_resized was applied", parent_gid)));
    }
    std::erase_if(_child_to_parent, [parent_gid] (const auto& p) { return p.second == parent_gid; });
    _resize_states.erase(parent_gid);
    logger.debug("group {}: resize is over, dropped its state", parent_gid);
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

std::optional<raft::group_id> raft_resize_coordinator::get_parent_group(raft::group_id child_gid) const {
    auto it = _child_to_parent.find(child_gid);
    if (it != _child_to_parent.end()) {
        return it->second;
    }
    return std::nullopt;
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

} // namespace service::strong_consistency
