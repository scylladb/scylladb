/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/view/view_building_state.hh"

namespace db {

namespace view {

view_building_task::view_building_task(utils::UUID id, task_type type, task_state state, table_id base_id, std::optional<table_id> view_id, locator::tablet_replica replica, dht::token last_token)
        : id(id)
        , type(type)
        , state(state)
        , base_id(base_id)
        , view_id(view_id)
        , replica(replica)
        , last_token(last_token) {}

view_building_state::view_building_state(building_tasks tasks_state, std::optional<table_id> processed_base_table)
        : tasks_state(std::move(tasks_state))
        , currently_processed_base_table(std::move(processed_base_table)) {}

views_state::views_state(std::map<table_id, std::vector<table_id>> views_per_base, view_build_status_map status_map)
        : views_per_base(std::move(views_per_base))
        , status_map(std::move(status_map)) {}

view_building_task::task_type task_type_from_string(std::string_view str) {
    if (str == "BUILD_RANGE") {
        return view_building_task::task_type::build_range;
    }
    if (str == "PROCESS_STAGING") {
        return view_building_task::task_type::process_staging;
    }
    throw std::runtime_error(fmt::format("Unknown view building task type: {}", str));
}

seastar::sstring task_type_to_sstring(view_building_task::task_type type) {
    switch (type) {
    case view_building_task::task_type::build_range:
        return "BUILD_RANGE";
    case view_building_task::task_type::process_staging:
        return "PROCESS_STAGING";
    }
}

view_building_task::task_state task_state_from_string(std::string_view str) {
    if (str == "IDLE") {
        return view_building_task::task_state::idle;
    }
    if (str == "STARTED") {
        return view_building_task::task_state::started;
    }
    if (str == "ABORTED") {
        return view_building_task::task_state::aborted;
    }
    throw std::runtime_error(fmt::format("Unknown view building task state: {}", str));
}

seastar::sstring task_state_to_sstring(view_building_task::task_state state) {
    switch (state) {
    case view_building_task::task_state::idle:
        return "IDLE";
    case view_building_task::task_state::started:
        return "STARTED";
    case view_building_task::task_state::aborted:
        return "ABORTED";
    }
}

std::vector<std::reference_wrapper<const view_building_task>> view_building_state::get_tasks_for_host(table_id base_id, locator::host_id host) const {
    if (!tasks_state.contains(base_id)) {
        return {};
    }

    std::vector<std::reference_wrapper<const view_building_task>> host_tasks;
    for (auto& [replica, replica_tasks]: tasks_state.at(base_id)) {
        if (replica.host != host) {
            continue;
        }

        for (auto& [_, view_tasks]: replica_tasks.view_tasks) {
            for (auto& [_, task]: view_tasks) {
                host_tasks.emplace_back(task);
            }
        }
        for (auto& [_, task]: replica_tasks.staging_tasks) {
            host_tasks.emplace_back(task);
        }
    }
    return host_tasks;
}

}

}
