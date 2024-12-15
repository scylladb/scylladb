/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <exception>
#include <iterator>
#include <ranges>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/abort_source.hh>

#include "cql3/query_processor.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "mutation/canonical_mutation.hh"
#include "schema/schema_fwd.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/topology_coordinator.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "service/migration_manager.hh"
#include "replica/database.hh"
#include "view_info.hh"

#include "service/view_building_coordinator.hh"

static logging::logger vbc_logger("view_building_coordinator");

namespace service {

view_building_coordinator::view_building_coordinator(abort_source& as, replica::database& db, raft::server& raft, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm, const raft::term_t term) 
    : _db(db)
    , _raft(raft)
    , _group0(group0)
    , _sys_ks(sys_ks)
    , _topo_sm(topo_sm)
    , _term(term)
    , _as(as) 
{}

future<group0_guard> view_building_coordinator::start_operation() {
    auto guard = co_await _group0.client().start_operation(_as);
    if (_term != _raft.get_current_term()) {
        throw term_changed_error{};
    }
    co_return std::move(guard);
}

future<> view_building_coordinator::await_event() {
    _as.check();
    co_await _cond.when();
    vbc_logger.debug("event awaited");
}

future<view_building_coordinator::vbc_state> view_building_coordinator::load_coordinator_state() {
    auto tasks = co_await _sys_ks.get_view_building_coordinator_tasks();
    co_return vbc_state {
        .tasks = std::move(tasks)
    };
}

table_id view_building_coordinator::get_base_table_id(table_id view_id) {
    return _db.find_schema(view_id)->view_info()->base_id();
}

table_id view_building_coordinator::table_name_to_id(const std::pair<sstring, sstring>& table_name) {
    return _db.find_schema(table_name.first, table_name.second)->id();
}

std::pair<sstring, sstring> view_building_coordinator::table_id_to_name(table_id table_id) {
    auto schema = _db.find_schema(table_id);
    return {schema->ks_name(), schema->cf_name()};
}

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _cond.broadcast();
    });

    while (!_as.abort_requested()) {
        vbc_logger.debug("coordinator loop iteration");
        try {
            auto state_opt = co_await update_coordinator_state();
            if (!state_opt) {
                // If state_opt is nullopt, it means there was work to do and the state has changed.
                continue;
            }
            // TODO: Do actual work, send RPCs to build a particular view's range
            co_await await_event();
        } catch (...) {
            // TODO: error handling
        }
    }
}

future<std::optional<view_building_coordinator::vbc_state>> view_building_coordinator::update_coordinator_state() {
    vbc_logger.debug("update_coordinator_state()");

    auto guard = co_await start_operation();
    std::vector<canonical_mutation> cmuts;

    const auto state = co_await load_coordinator_state();
    auto views = _db.get_views() 
            | std::views::filter([this] (view_ptr& v) { return _db.find_keyspace(v->ks_name()).uses_tablets(); })
            | std::views::transform([] (view_ptr& v) { return v->id(); })
            | std::ranges::to<std::vector>();
    auto built_views = co_await _sys_ks.load_built_views(db::system_keyspace_view_type::tablet_based)
            | std::views::transform([this] (db::system_keyspace_view_name& v) { 
                return table_name_to_id(v);
            }) | std::ranges::to<std::vector>();

    if (auto to_add = get_views_to_add(state, views, built_views); !to_add.empty()) {
        for (auto& view_id: to_add) {
            auto muts = co_await add_view(guard, view_id);
            cmuts.insert(cmuts.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
        }
    }
    if (auto to_remove = get_views_to_remove(state, views); !to_remove.empty()) {
        for (auto& view_id: to_remove) {
            auto muts = co_await remove_view(guard, view_id);
            cmuts.insert(cmuts.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
        }
    }
    if (auto built_to_remove = get_built_views_to_remove(built_views, views); !built_to_remove.empty()) {
        for (auto& view_id: built_to_remove) {
            auto mut = co_await remove_built_view(guard, view_id);
            cmuts.emplace_back(std::move(mut));
        }
    }

    if (!cmuts.empty()) {
        auto cmd = _group0.client().prepare_command(write_mutations{
            .mutations{std::move(cmuts)},
        }, guard, "update view building coordinator state");
        co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);
        co_return std::nullopt;
    }
    co_return state;
}

std::set<table_id> view_building_coordinator::get_views_to_add(const vbc_state& state, const std::vector<table_id>& views, const std::vector<table_id>& built) {
    std::set<table_id> views_to_add;
    for (auto& view_id: views) {
        if (std::find(built.begin(), built.end(), view_id) == built.end()) {
            auto base_id = get_base_table_id(view_id);
            if (!state.tasks.contains(base_id) || !state.tasks.at(base_id).contains(view_id)) {
                views_to_add.insert(view_id);
            }
        }
    }
    return views_to_add;
}

std::set<table_id> view_building_coordinator::get_views_to_remove(const vbc_state& state, const std::vector<table_id>& views) {
    std::set<table_id> views_to_remove;
    for (auto& [_, view_tasks]: state.tasks) {
        for (auto& [view_id, _]: view_tasks) {
            if (std::find(views.begin(), views.end(), view_id) == views.end()) {
                views_to_remove.insert(view_id);
            }
        }
    }
    return views_to_remove;
}

std::set<table_id> view_building_coordinator::get_built_views_to_remove(const std::vector<table_id>& built, const std::vector<table_id>& views) {
    std::set<table_id> built_views_to_remove;
    for (auto& view_id: built) {
        if (std::find(views.begin(), views.end(), view_id) == views.end()) {
            built_views_to_remove.insert(view_id);
        }
    }
    return built_views_to_remove;
}

future<std::vector<canonical_mutation>> view_building_coordinator::add_view(const group0_guard& guard, const table_id& view_id) {
    auto view_name = table_id_to_name(view_id);
    vbc_logger.info("Register new view: {}.{}", view_name.first, view_name.second);

    auto base_id = get_base_table_id(view_id);
    auto& base_cf = _db.find_column_family(base_id);
    auto erm = base_cf.get_effective_replication_map();
    auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(base_id);

    std::vector<canonical_mutation> muts;
    for (auto tid = std::optional(tablet_map.first_tablet()); tid; tid = tablet_map.next_tablet(*tid)) {
        const auto& tablet_info = tablet_map.get_tablet_info(*tid);
        auto range = tablet_map.get_token_range(*tid);

        for (auto& replica: tablet_info.replicas) {
            auto mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view_id, replica, range);
            muts.emplace_back(std::move(mut));
        }
    }
    co_return muts;
}

future<std::vector<canonical_mutation>> view_building_coordinator::remove_view(const group0_guard& guard, const table_id& view_id) {
    auto view_name = table_id_to_name(view_id);
    vbc_logger.info("Unregister all remaining tasks for view: {}.{}", view_name.first, view_name.second);
    
    auto muts = co_await _sys_ks.make_vbc_remove_view_tasks_mutations(guard.write_timestamp(), view_id);
    co_return std::vector<canonical_mutation>{muts.begin(), muts.end()};
}

future<canonical_mutation> view_building_coordinator::remove_built_view(const group0_guard& guard, const table_id& view_id) {
    auto view_name = table_id_to_name(view_id);
    vbc_logger.info("Remove status for built view: {}.{}", view_name.first, view_name.second);

    auto mut = co_await _sys_ks.make_remove_built_view_mutation(guard.write_timestamp(), view_name.first, view_name.second);
    co_return canonical_mutation(std::move(mut));
}

}
