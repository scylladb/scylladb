/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <ranges>
#include <seastar/core/condition-variable.hh>
#include "db/view/view_build_status.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "utils/UUID.hh"
#include <fmt/base.h>
#include "schema/schema_fwd.hh"

namespace db {

namespace view {

// Holds information about view building task for a tablet_replica.
//
// There are two types of tasks - `build_range` and `process_staging`, chosen by the `type` member.
// The meaning of these two task types are:
// build_range      ->  Build a token_range owned by tablet_id, from base_id to view_id.
//                      Multiple `build_range` tasks are executed together
//                      if their `base_id` and `tid` are the same.
// process_staging  ->  Register all staging sstables owned by the tablet_replica
//                      to the view_update_generator and wait until all of the staging sstables
//                      are processed (view updates are generated and
//                      the sstables are moved to normal directory).
struct view_building_task {
    enum class task_type {
        build_range,
        process_staging,
    };

    // When a task is created, it starts with `IDLE` state.
    // Then, the view building coordinator will decide to do the task and it will
    // set the state to `STARTED`.
    // When a task is finished the entry is removed.
    //
    // If a task is in progress when a tablet operation (migration/resize) starts,
    // the task's state is set to `ABORTED`.
    enum class task_state {
        idle,
        started,
        aborted,
    };
    utils::UUID id;
    task_type type;
    task_state state;

    table_id base_id;
    std::optional<table_id> view_id; // nullopt when task_type is `process_staging`
    locator::tablet_replica replica;
    dht::token last_token;

    view_building_task(utils::UUID id, task_type type, task_state state,
            table_id base_id, std::optional<table_id> view_id,
            locator::tablet_replica replica, dht::token last_token);
};

using task_map = std::map<utils::UUID, view_building_task>;

struct replica_tasks {
    std::map<table_id, task_map> view_tasks;    // owned by a particular view
    task_map staging_tasks;                     // owned by whole base table
};

using base_table_tasks = std::map<locator::tablet_replica, replica_tasks>;
using building_tasks = std::map<table_id, base_table_tasks>;

// Represents cluster-wide view building state (only for tablet-based views).
// The state stores all unfinished view building tasks for all tablet-based views
// and table_id of currently processed base table by view building coordinator.
//
// The state is stored in group0 tables and the in-memory structure is reloaded
// when mutations are applied to relevant tables.
struct view_building_state {
    building_tasks tasks_state;
    std::optional<table_id> currently_processed_base_table;

    view_building_state(building_tasks tasks_state, std::optional<table_id> processed_base_table);
    view_building_state() = default;
    
    std::optional<std::reference_wrapper<const view_building_task>> get_task(table_id base_id, locator::tablet_replica replica, utils::UUID id) const;
    std::vector<std::reference_wrapper<const view_building_task>> get_tasks_for_host(table_id base_id, locator::host_id host) const;
    std::map<dht::token, std::vector<view_building_task>> collect_tasks_by_last_token(table_id base_table_id) const;
    std::map<dht::token, std::vector<view_building_task>> collect_tasks_by_last_token(table_id base_table_id, const locator::tablet_replica& replica) const;
    std::vector<utils::UUID> get_started_tasks(table_id base_table_id, locator::tablet_replica replica) const;
};

// Represents global state of tablet-based views.
// Contains information about existing views and their build statuses.
struct views_state {
    using view_build_status_map = std::map<table_id, std::map<locator::host_id, db::view::build_status>>;

    std::map<table_id, std::vector<table_id>> views_per_base;
    view_build_status_map status_map;

    views_state(std::map<table_id, std::vector<table_id>> views_per_base, view_build_status_map status_map);
    views_state() = default;
};

struct view_building_state_machine {
    view_building_state building_state;
    views_state views_state;
    condition_variable event;
};

struct view_task_result {
    enum class command_status: uint8_t {
        success = 0,
        abort = 1,
    };
    db::view::view_task_result::command_status status;
};

view_building_task::task_type task_type_from_string(std::string_view str);
seastar::sstring task_type_to_sstring(view_building_task::task_type type);
view_building_task::task_state task_state_from_string(std::string_view str);
seastar::sstring task_state_to_sstring(view_building_task::task_state state);

} // namespace view_building

} // namespace service

template <> struct fmt::formatter<db::view::view_building_task::task_type> : fmt::formatter<string_view> {
    auto format(db::view::view_building_task::task_type type, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", db::view::task_type_to_sstring(type));
    }
};

template <> struct fmt::formatter<db::view::view_building_task::task_state> : fmt::formatter<string_view> {
    auto format(db::view::view_building_task::task_state state, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", db::view::task_state_to_sstring(state));
    }
};

template <> struct fmt::formatter<db::view::view_building_task> : fmt::formatter<string_view> {
    auto format(db::view::view_building_task task, fmt::format_context& ctx) const {
        auto view_id = task.view_id ? fmt::to_string(*task.view_id) : "nullopt";
        return fmt::format_to(ctx.out(), "view_building_task{{type: {}, state: {}, base_id: {}, view_id: {}, last_token: {}}}",
                task.type, task.state, task.base_id, view_id, task.last_token);
    }
};

template <> struct fmt::formatter<db::view::task_map> : fmt::formatter<string_view> {
    auto format(db::view::task_map task_map, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", task_map | std::views::keys);
    }
};

template <> struct fmt::formatter<db::view::replica_tasks> : fmt::formatter<string_view> {
    auto format(db::view::replica_tasks replica_tasks, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{view_tasks: {}, staging_tasks: {}}}", replica_tasks.view_tasks, replica_tasks.staging_tasks);
    }
};

template <> struct fmt::formatter<db::view::view_task_result> : fmt::formatter<string_view> {
    auto format(db::view::view_task_result result, fmt::format_context& ctx) const {
        std::string_view res;
        switch (result.status) {
            case db::view::view_task_result::command_status::success:
            res = "success";
            break;
        case db::view::view_task_result::command_status::abort:
            res = "abort";
            break;
        }
        return format_to(ctx.out(), "{}", res);
    }
};
