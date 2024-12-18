/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "service/raft/group0_state_id_handler.hh"
#include "db/config.hh"
#include "compaction/compaction_manager.hh"
#include "gms/gossiper.hh"
#include "utils/log.hh"
#include "raft/server.hh"
#include "replica/database.hh"
#include "service/raft/raft_group_registry.hh"

namespace service {

static logging::logger slogger("group0_tombstone_gc_handler");

raft::server* group0_server_accessor::get_server() const {
    return _raft_gr.find_server(_group0_id);
}

lowres_clock::duration group0_state_id_handler::get_refresh_interval(const replica::database& db) {
    return std::chrono::milliseconds{db.get_config().group0_tombstone_gc_refresh_interval_in_ms()};
}

void group0_state_id_handler::refresh() {
    auto* const group0_server = _server_accessor.get_server();
    if (!group0_server) {
        slogger.debug("Skipping due to group0 server not found");
        return;
    }

    const auto& group0_members = std::invoke([&] {
        auto members = group0_server->get_configuration().current;
        members.merge(group0_server->get_configuration().previous);
        return members;
    });
    if (group0_members.empty()) {
        slogger.info("Skipping due to empty group0");
        return;
    }

    slogger.trace("Current node count: {}", group0_members.size());

    std::vector<raft::server_id> group0_members_missing_endpoint;
    std::vector<raft::server_id> group0_members_missing_state_id;

    const auto& group0_members_state_ids = group0_members | std::ranges::views::transform([&](const auto& member) -> std::optional<utils::UUID> {
        const auto* state_id_ptr = _gossiper.get_application_state_ptr(locator::host_id{member.addr.id.uuid()}, gms::application_state::GROUP0_STATE_ID);
        if (!state_id_ptr) {
            group0_members_missing_state_id.push_back(member.addr.id);
            return std::nullopt;
        }

        return utils::UUID{state_id_ptr->value()};
    }) | std::ranges::views::filter([](const auto& state_id) {
        return state_id.has_value();
    }) | std::ranges::views::transform([](const auto& state_id) {
        return state_id.value();
    }) | std::ranges::to<std::vector>();

    if (!group0_members_missing_endpoint.empty()) {
        slogger.info("Skipping due to missing endpoints for members: {}", fmt::join(group0_members_missing_endpoint, ", "));
        return;
    }

    if (!group0_members_missing_state_id.empty()) {
        slogger.info("Skipping due to missing state id of some endpoints: {}", fmt::join(group0_members_missing_state_id, ", "));
        return;
    }

    const auto min_state_id = std::ranges::min(group0_members_state_ids, [](auto a, auto b) {
        if (!a || !b) {
            // This should never happen, but if it does, it's a bug.
            on_fatal_internal_error(slogger, "unexpected empty state_id");
        }
        return utils::timeuuid_tri_compare(a, b) < 0;
    });

    if (_state_id_last_reconcile && utils::timeuuid_tri_compare(_state_id_last_reconcile, min_state_id) > 0) {
        slogger.info("Skipping due to the stale minimum state id: {}", min_state_id);
        return;
    }

    _state_id_last_reconcile = min_state_id;

    const auto tombstone_gc_time = to_gc_clock(db_clock::time_point(utils::UUID_gen::unix_timestamp(min_state_id)))
                                   // subtracting one resolution unit:
                                   // - the GC time has 1s resolution
                                   // - there might be state_ids with the same GC time that didn't propagate yet
                                   //   (and thus we don't want to GC these)
                                   - gc_clock::duration{1};

    slogger.info("Setting reconcile time to {} (min id={})", tombstone_gc_time, min_state_id);

    auto& gc_state = _local_db.get_compaction_manager().get_tombstone_gc_state();
    gc_state.update_group0_refresh_time(tombstone_gc_time);
}

group0_state_id_handler::group0_state_id_handler(
        replica::database& local_db, gms::gossiper& gossiper, group0_server_accessor server_accessor)
    : _local_db(local_db)
    , _gossiper(gossiper)
    , _server_accessor(server_accessor)
    , _refresh_interval(get_refresh_interval(local_db)) {
}

void group0_state_id_handler::run() {
    if (this_shard_id() != 0) {
        on_fatal_internal_error(slogger, "group0_state_id_handler must be started on shard 0");
    }

    _timer.set_callback([this] {
        refresh();

        _timer.arm(_refresh_interval);
    });

    // do the first refresh "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
    // after that send every _refresh_interval.

    _timer.arm(2 * gms::gossiper::INTERVAL);
}

future<> group0_state_id_handler::advertise_state_id(utils::UUID state_id) {
    if (!_gossiper.is_enabled()) {
        slogger.debug("Skipping advertisement of state id {} because gossiper is not active", state_id);
        return make_ready_future();
    }

    if (_state_id_last_advertised && utils::timeuuid_tri_compare(_state_id_last_advertised, state_id) > 0) {
        slogger.debug("Skipping advertisement of stale state id {}", state_id);
        return make_ready_future();
    }

    _state_id_last_advertised = state_id;

    const auto gc_time = to_gc_clock(db_clock::time_point(utils::UUID_gen::unix_timestamp(state_id)));
    slogger.debug("Advertising state id: {} (gc_time: {})", state_id, gc_time);
    return _gossiper.add_local_application_state(gms::application_state::GROUP0_STATE_ID, gms::versioned_value::state_id(service::state_id(state_id)));
}

} // namespace service
