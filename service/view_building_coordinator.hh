/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "dht/i_partitioner_fwd.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group0.hh"
#include "service/topology_state_machine.hh"
#include "service/migration_listener.hh"

namespace replica {
class database;
}

namespace service {

class group0_guard;
class group0_batch;


namespace vbc {
struct view_building_target {
    locator::host_id host;
    unsigned shard;

    bool operator<(const view_building_target& other) const {
        return host < other.host || (host == other.host && shard < other.shard);
    }
};

using view_name = std::pair<sstring, sstring>;
using view_tasks = std::map<view_building_target, dht::token_range_vector>;
using base_tasks = std::map<view_name, view_tasks>;
struct vbc_tasks : public std::map<table_id, base_tasks> {};

class view_building_coordinator : public migration_listener::only_view_notifications {
    struct vbc_state {
        vbc_tasks tasks;
        std::optional<table_id> processing_base;
    };

    replica::database& _db;
    raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    netw::messaging_service& _messaging;
    const topology_state_machine& _topo_sm;
    
    abort_source& _as;
    condition_variable _cond;
    semaphore _rpc_response_mutex = semaphore(1);
    std::map<view_building_target, future<>> _rpc_handlers;
    std::map<view_building_target, dht::token_range> _per_host_processing_range;

public:
    view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, netw::messaging_service& messaging, const topology_state_machine& topo_sm); 

    future<> run();
    future<> stop();

    future<std::vector<mutation>> get_migrate_tasks_mutations(const group0_guard& guard, table_id table_id, locator::tablet_replica abandoning_replica, locator::tablet_replica pending_replica, dht::token_range range);
    future<std::vector<mutation>> get_resize_tasks_mutations(const group0_guard& guard, table_id table_id, const locator::tablet_map& tablet_map, const locator::tablet_map& new_tablet_map);

    void notify() { _cond.broadcast(); }
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }

private:
    future<group0_guard> start_operation();
    future<> await_event();
    future<vbc_state> load_coordinator_state();
    bool handle_view_building_coordinator_error(std::exception_ptr eptr) noexcept;
    
    future<std::optional<vbc_state>> update_coordinator_state();
    future<std::vector<canonical_mutation>> add_view(const group0_guard& guard, const view_name& view_name);
    future<std::vector<canonical_mutation>> remove_view(const group0_guard& guard, const view_name& view_name);

    std::set<view_name> get_views_to_add(const vbc_state& state, const std::vector<view_name>& views, const std::vector<view_name>& built);
    std::set<view_name> get_views_to_remove(const vbc_state& state, const std::vector<view_name>& views);

    future<std::vector<mutation>> get_split_mutations(const group0_guard& guard, const locator::tablet_map& tablet_map, const view_name& view, const view_building_target& target, const std::vector<dht::token_range>& tasks);
    future<std::vector<mutation>> get_merge_mutations(const group0_guard& guard, const locator::tablet_map& tablet_map, const locator::tablet_map& new_tablet_map, const view_name& view, const view_building_target& target, const std::vector<dht::token_range>& tasks);
    
    table_id get_base_id(const view_name& view_name);

    future<> build_view(vbc_state state);
    future<> send_task(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views);
    future<> mark_task_completed(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views);
    future<> abort_work(locator::host_id host, unsigned shard);
};

future<> run_view_building_coordinator(std::unique_ptr<view_building_coordinator> vb_coordinator, replica::database& db, raft_group0& group0);
}

}

template <> struct fmt::formatter<service::vbc::view_building_target> : fmt::formatter<string_view> {
    auto format(const service::vbc::view_building_target& target, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{host={}, shard={}}}", target.host, target.shard);
    }
};
