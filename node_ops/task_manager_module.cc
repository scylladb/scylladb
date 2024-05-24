/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/system_keyspace.hh"
#include "node_ops/task_manager_module.hh"
#include "service/storage_service.hh"
#include "service/topology_coordinator.hh"
#include "service/topology_state_machine.hh"
#include "tasks/task_handler.hh"
#include "utils/error_injection.hh"

#include <boost/range/adaptor/transformed.hpp>

using namespace std::chrono_literals;

namespace node_ops {

static tasks::task_manager::task_state get_state(const db::system_keyspace::topology_requests_entry& entry) {
    if (!entry.id) {
        return tasks::task_manager::task_state::created;
    } else if (!entry.done) {
        return tasks::task_manager::task_state::running;
    } else if (entry.error == "") {
        return tasks::task_manager::task_state::done;
    } else {
        return tasks::task_manager::task_state::failed;
    }
}

static std::set<tasks::task_id> get_pending_ids(service::topology& topology) {
    std::set<tasks::task_id> ids;
    for (auto& request : topology.requests) {
        ids.emplace(topology.find(request.first)->second.request_id);
    }
    return ids;
}

static future<db::system_keyspace::topology_requests_entries> get_entries(db::system_keyspace& sys_ks, service::topology& topology, std::chrono::seconds ttl) {
    // Started requests.
    auto entries = co_await sys_ks.get_topology_request_entries(db_clock::now() - ttl);

    // Pending requests.
    for (auto& id : get_pending_ids(topology)) {
        entries.try_emplace(id.uuid(), db::system_keyspace::topology_requests_entry{});
    }

    co_return entries;
}

future<std::optional<tasks::task_status>> node_ops_virtual_task::get_status_helper(tasks::task_id id) const {
    auto entry = co_await _ss._sys_ks.local().get_topology_request_entry(id.uuid(), false);
    auto started = entry.id;
    service::topology& topology = _ss._topology_state_machine._topology;
    if (!started && !get_pending_ids(topology).contains(id)) {
        co_return std::nullopt;
    }
    auto type = entry.request_type ? fmt::format("{}", entry.request_type.value()) : "";
    co_return tasks::task_status{
        .task_id = id,
        .type = std::move(type),
        .kind = tasks::task_kind::cluster,
        .scope = "cluster",
        .state = get_state(entry),
        .is_abortable = co_await is_abortable(),
        .start_time = entry.start_time,
        .end_time = entry.end_time,
        .error = entry.error,
        .parent_id = tasks::task_id::create_null_id(),
        .sequence_number = 0,
        .shard = 0,
        .keyspace = "",
        .table = "",
        .entity = "",
        .progress_units = "",
        .progress = tasks::task_manager::task::progress{},
        .children = started ? co_await get_children(get_module(), id) : std::vector<tasks::task_identity>{}
    };
}

tasks::task_manager::task_group node_ops_virtual_task::get_group() const noexcept {
    return tasks::task_manager::task_group::topology_change_group;
}

future<std::set<tasks::task_id>> node_ops_virtual_task::get_ids() const {
    db::system_keyspace& sys_ks = _ss._sys_ks.local();
    service::topology& topology = _ss._topology_state_machine._topology;
    co_return boost::copy_range<std::set<tasks::task_id>>(co_await get_entries(sys_ks, topology, get_task_manager().get_task_ttl()) | boost::adaptors::map_keys);
}

future<tasks::is_abortable> node_ops_virtual_task::is_abortable() const {
    return make_ready_future<tasks::is_abortable>(tasks::is_abortable::no);
}

future<std::optional<tasks::task_status>> node_ops_virtual_task::get_status(tasks::task_id id) {
    return get_status_helper(id);
}

future<std::optional<tasks::task_status>> node_ops_virtual_task::wait(tasks::task_id id) {
    auto entry = co_await get_status_helper(id);
    if (!entry) {
        co_return std::nullopt;
    }

    co_await _ss.wait_for_topology_request_completion(id.uuid(), false);
    co_return co_await get_status_helper(id);
}

future<> node_ops_virtual_task::abort(tasks::task_id id) noexcept {
    return make_ready_future();
}

future<std::vector<tasks::task_stats>> node_ops_virtual_task::get_stats() {
    db::system_keyspace& sys_ks = _ss._sys_ks.local();
    service::topology& topology = _ss._topology_state_machine._topology;
    co_return boost::copy_range<std::vector<tasks::task_stats>>(co_await get_entries(sys_ks, topology, get_task_manager().get_task_ttl())
            | boost::adaptors::transformed([] (const auto& e) {
        auto id = e.first;
        auto& entry = e.second;
        auto type = entry.request_type ? fmt::format("{}", entry.request_type.value()) : "";
        return tasks::task_stats {
            .task_id = tasks::task_id{id},
            .type = std::move(type),
            .kind = tasks::task_kind::cluster,
            .scope = "cluster",
            .state = get_state(entry),
            .sequence_number = 0,
            .keyspace = "",
            .table = "",
            .entity = ""
        };
    }));
}

streaming_task_impl::streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        streaming::stream_reason reason,
        std::optional<shared_future<>>& result,
        std::function<future<>()> action) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", "", "", "", parent_id)
    , _reason(reason)
    , _result(result)
    , _action(std::move(action))
{}

std::string streaming_task_impl::type() const {
    return fmt::format("{}: streaming", _reason);
}

tasks::is_internal streaming_task_impl::is_internal() const noexcept {
    return tasks::is_internal::no;
}

future<> streaming_task_impl::run() {
    // If no operation was previously started - start it now
    // If previous operation still running - wait for it an return its result
    // If previous operation completed successfully - return immediately
    // If previous operation failed - restart it
    if (!_result || _result->failed()) {
        if (_result) {
            service::rtlogger.info("retry streaming after previous attempt failed with {}", _result->get_future().get_exception());
        } else {
            service::rtlogger.info("start streaming");
        }
        _result = _action();
    } else {
        service::rtlogger.debug("already streaming");
    }
    co_await _result.value().get_future();
    service::rtlogger.info("streaming completed");
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
