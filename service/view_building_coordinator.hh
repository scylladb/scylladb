/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <compare>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "dht/i_partitioner_fwd.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "mutation/canonical_mutation.hh"
#include "schema/schema_fwd.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/topology_state_machine.hh"

namespace service {

struct view_building_target {
    locator::host_id host;
    unsigned shard;

    view_building_target(locator::host_id host, unsigned shard) : host(host), shard(shard) {}
    view_building_target(const locator::tablet_replica& replica) : view_building_target(replica.host, replica.shard) {}

    std::strong_ordering operator<=>(const view_building_target&) const = default;
};

using view_building_tasks = std::map<view_building_target, dht::token_range_vector>;
using base_building_tasks = std::map<table_id, view_building_tasks>;
using view_building_coordinator_tasks = std::map<table_id, base_building_tasks>;

class view_building_coordinator : public migration_listener::only_view_notifications {
    struct vbc_state {
        view_building_coordinator_tasks tasks;
    };

    replica::database& _db;
    raft::server& _raft;
    raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    const topology_state_machine& _topo_sm;
    const raft::term_t _term;
    
    abort_source& _as;
    condition_variable _cond;

public:
    view_building_coordinator(abort_source& as, replica::database& db, raft::server& raft, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm, const raft::term_t term); 

    future<> run();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }

private:
    future<group0_guard> start_operation();
    future<> await_event();
    future<vbc_state> load_coordinator_state();

    table_id get_base_table_id(table_id view_id);
    table_id table_name_to_id(const std::pair<sstring, sstring>& table_name);
    std::pair<sstring, sstring> table_id_to_name(table_id table_id);
    
    future<std::optional<vbc_state>> update_coordinator_state();
    future<std::vector<canonical_mutation>> add_view(const group0_guard& guard, const table_id& view_id);
    future<std::vector<canonical_mutation>> remove_view(const group0_guard& guard, const table_id& view_id);
    future<canonical_mutation> remove_built_view(const group0_guard& guard, const table_id& view_id);

    std::set<table_id> get_views_to_add(const vbc_state& state, const std::vector<table_id>& views, const std::vector<table_id>& built);
    std::set<table_id> get_views_to_remove(const vbc_state& state, const std::vector<table_id>& views);
    std::set<table_id> get_built_views_to_remove(const std::vector<table_id>& built, const std::vector<table_id>& views);
};

}
