/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/system_keyspace.hh"
#include "dht/boot_strapper.hh"
#include "dht/range_streamer.hh"
#include "node_ops/task_manager_module.hh"
#include "repair/row_level.hh"
#include "service/raft/raft_group0.hh"
#include "service/storage_service.hh"
#include "service/topology_state_machine.hh"
#include "tasks/task_handler.hh"

#include <boost/range/adaptor/transformed.hpp>

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

static future<db::system_keyspace::topology_requests_entries> get_entries(db::system_keyspace& sys_ks, service::topology& topology) {
    // Started requests.
    auto entries = co_await sys_ks.get_topology_request_entries();

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
    co_return boost::copy_range<std::set<tasks::task_id>>(co_await get_entries(sys_ks, topology) | boost::adaptors::map_keys);
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
    co_return boost::copy_range<std::vector<tasks::task_stats>>(co_await get_entries(sys_ks, topology)
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

std::string node_ops_streaming_task_impl::type() const {
    return fmt::format("{}: streaming", _reason);
}

tasks::is_internal node_ops_streaming_task_impl::is_internal() const noexcept {
    return tasks::is_internal::no;
}

node_ops_streaming_task_impl::node_ops_streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        service::storage_service& ss,
        streaming::stream_reason reason,
        std::optional<shared_future<>>& result) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", "", "", "", parent_id)
    , _reason(reason)
    , _result(result)
    , _ss(ss)
{}

future<> node_ops_streaming_task_impl::run() {
    // If no operation was previously started - start it now
    // If previous operation still running - wait for it an return its result
    // If previous operation completed successfully - return immediately
    // If previous operation failed - restart it
    if (!_result || _result->failed()) {
        if (_result) {
            tasks::tmlogger.info("retry streaming after previous attempt failed with {}", _result->get_future().get_exception());
        } else {
            tasks::tmlogger.info("start streaming");
        }
        _result = stream();
    } else {
        tasks::tmlogger.debug("already streaming");
    }
    co_await _result.value().get_future();
    tasks::tmlogger.info("streaming completed");
}

bootstrap_streaming_task_impl::bootstrap_streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        service::storage_service& ss,
        const service::replica_state& rs) noexcept
    : node_ops_streaming_task_impl(module, parent_id, ss, streaming::stream_reason::bootstrap, ss._bootstrap_result)
    , _rs(rs)
{}

future<> bootstrap_streaming_task_impl::stream() {
    if (_ss.is_repair_based_node_ops_enabled(streaming::stream_reason::bootstrap)) {
        co_await _ss._repair.local().bootstrap_with_repair(_ss.get_token_metadata_ptr(), _rs.ring.value().tokens);
    } else {
        dht::boot_strapper bs(_ss._db, _ss._stream_manager, _ss._abort_source, _ss.get_token_metadata_ptr()->get_my_id(),
            locator::endpoint_dc_rack{_rs.datacenter, _rs.rack}, _rs.ring.value().tokens, _ss.get_token_metadata_ptr());
        co_await bs.bootstrap(streaming::stream_reason::bootstrap, _ss._gossiper, _ss._topology_state_machine._topology.session);
    }
}

replace_streaming_task_impl::replace_streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        service::storage_service& ss,
        const service::replica_state& rs) noexcept
    : node_ops_streaming_task_impl(module, parent_id, ss, streaming::stream_reason::replace, ss._bootstrap_result)
    , _rs(rs)
{}

future<> replace_streaming_task_impl::stream() {
    auto& raft_server = _ss._group0->group0_server();
    if (!_ss._topology_state_machine._topology.req_param.contains(raft_server.id())) {
        on_internal_error(tasks::tmlogger, ::format("Cannot find request_param for node id {}", raft_server.id()));
    }
    if (_ss.is_repair_based_node_ops_enabled(streaming::stream_reason::replace)) {
        // FIXME: we should not need to translate ids to IPs here. See #6403.
        std::unordered_set<gms::inet_address> ignored_ips;
        for (const auto& id : _ss._topology_state_machine._topology.ignored_nodes) {
            auto ip = _ss._group0->address_map().find(id);
            if (!ip) {
                on_fatal_internal_error(tasks::tmlogger, ::format("Cannot find a mapping from node id {} to its ip", id));
            }
            ignored_ips.insert(*ip);
        }
        co_await _ss._repair.local().replace_with_repair(_ss.get_token_metadata_ptr(), _rs.ring.value().tokens, std::move(ignored_ips));
    } else {
        dht::boot_strapper bs(_ss._db, _ss._stream_manager, _ss._abort_source, _ss.get_token_metadata_ptr()->get_my_id(),
                                locator::endpoint_dc_rack{_rs.datacenter, _rs.rack}, _rs.ring.value().tokens, _ss.get_token_metadata_ptr());
        auto replaced_id = std::get<service::replace_param>(_ss._topology_state_machine._topology.req_param[raft_server.id()]).replaced_id;
        auto existing_ip = _ss._group0->address_map().find(replaced_id);
        assert(existing_ip);
        co_await bs.bootstrap(streaming::stream_reason::replace, _ss._gossiper, _ss._topology_state_machine._topology.session, *existing_ip);
    }
}

rebuild_streaming_task_impl::rebuild_streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        service::storage_service& ss,
        sstring source_dc) noexcept
    : node_ops_streaming_task_impl(module, parent_id, ss, streaming::stream_reason::rebuild, ss._rebuild_result)
    , _source_dc(source_dc)
{}

future<> rebuild_streaming_task_impl::stream() {
    auto tmptr = _ss.get_token_metadata_ptr();
    if (_ss.is_repair_based_node_ops_enabled(streaming::stream_reason::rebuild)) {
        co_await _ss._repair.local().rebuild_with_repair(tmptr, std::move(_source_dc));
    } else {
        auto streamer = make_lw_shared<dht::range_streamer>(_ss._db, _ss._stream_manager, tmptr, _ss._abort_source,
                tmptr->get_my_id(), _ss._snitch.local()->get_location(), "Rebuild", streaming::stream_reason::rebuild, _ss._topology_state_machine._topology.session);
        streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(_ss._gossiper.get_unreachable_members()));
        if (_source_dc != "") {
            streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(_source_dc));
        }
        auto ks_erms = _ss._db.local().get_non_local_strategy_keyspaces_erms();
        for (const auto& [keyspace_name, erm] : ks_erms) {
            co_await streamer->add_ranges(keyspace_name, erm, _ss.get_ranges_for_endpoint(erm, _ss.get_broadcast_address()), _ss._gossiper, false);
        }
        try {
            co_await streamer->stream_async();
            tasks::tmlogger.info("streaming for rebuild successful");
        } catch (...) {
            auto ep = std::current_exception();
            // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
            tasks::tmlogger.warn("error while rebuilding node: {}", ep);
            std::rethrow_exception(std::move(ep));
        }
    }
}

decommission_streaming_task_impl::decommission_streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        service::storage_service& ss) noexcept
    : node_ops_streaming_task_impl(module, parent_id, ss, streaming::stream_reason::decommission, ss._decommission_result)
{}

future<> decommission_streaming_task_impl::stream() {
    return _ss.unbootstrap();
}

remove_streaming_task_impl::remove_streaming_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id parent_id,
        service::storage_service& ss,
        gms::inet_address ip,
        raft::server_id id) noexcept
    : node_ops_streaming_task_impl(module, parent_id, ss, streaming::stream_reason::removenode, ss._remove_result[id])
    , _ip(ip)
{}

future<> remove_streaming_task_impl::stream() {
    auto as = make_shared<abort_source>();
    auto sub = _ss._abort_source.subscribe([as] () noexcept {
        if (!as->abort_requested()) {
            as->request_abort();
        }
    });
    if (_ss.is_repair_based_node_ops_enabled(streaming::stream_reason::removenode)) {
        // FIXME: we should not need to translate ids to IPs here. See #6403.
        std::list<gms::inet_address> ignored_ips;
        for (const auto& ignored_id : _ss._topology_state_machine._topology.ignored_nodes) {
            auto ip = _ss._group0->address_map().find(ignored_id);
            if (!ip) {
                on_fatal_internal_error(tasks::tmlogger, ::format("Cannot find a mapping from node id {} to its ip", ignored_id));
            }
            ignored_ips.push_back(*ip);
        }
        auto ops = seastar::make_shared<node_ops_info>(node_ops_id::create_random_id(), as, std::move(ignored_ips));
        return _ss._repair.local().removenode_with_repair(_ss.get_token_metadata_ptr(), _ip, ops);
    } else {
        return _ss.removenode_with_stream(_ip, _ss._topology_state_machine._topology.session, as);
    }
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
