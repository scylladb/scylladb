/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "node_ops/task_manager_module.hh"
#include "service/storage_service.hh"
#include "tasks/task_handler.hh"

namespace node_ops {

tasks::task_manager::task_group node_ops_virtual_task::get_group() const noexcept {
    return tasks::task_manager::task_group::topology_change_group;
}

future<std::set<tasks::task_id>> node_ops_virtual_task::get_ids() const {
    return make_ready_future<std::set<tasks::task_id>>(std::set<tasks::task_id>{});
}

future<tasks::is_abortable> node_ops_virtual_task::is_abortable() const {
    return make_ready_future<tasks::is_abortable>(tasks::is_abortable::no);
}

future<std::optional<tasks::task_status>> node_ops_virtual_task::get_status(tasks::task_id id) {
    return make_ready_future<std::optional<tasks::task_status>>(std::nullopt);
}

future<std::optional<tasks::task_status>> node_ops_virtual_task::wait(tasks::task_id id) {
    return make_ready_future<std::optional<tasks::task_status>>(std::nullopt);
}

future<> node_ops_virtual_task::abort(tasks::task_id id) noexcept {
    return make_ready_future();
}

future<std::vector<tasks::task_stats>> node_ops_virtual_task::get_stats() {
    return make_ready_future<std::vector<tasks::task_stats>>(std::vector<tasks::task_stats>{});
}

task_manager_module::task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept
    : tasks::task_manager::module(tm, "node_ops")
    , _ss(ss)
{}

std::set<gms::inet_address> task_manager_module::get_nodes() const noexcept {
    return boost::copy_range<std::set<gms::inet_address>>(
        boost::join(
            _ss._topology_state_machine._topology.normal_nodes,
            _ss._topology_state_machine._topology.transition_nodes
        ) | boost::adaptors::transformed([&ss = _ss] (auto& node) {
            return ss.host2ip(locator::host_id{node.first.uuid()});
        })
    );
}

}
