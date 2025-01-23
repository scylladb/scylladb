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

#include "cql3/query_processor.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "mutation/canonical_mutation.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/loop.hh"
#include "seastar/coroutine/maybe_yield.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "seastar/core/abort_source.hh"
#include "seastar/core/with_scheduling_group.hh"
#include "service/migration_manager.hh"
#include "replica/database.hh"
#include "view_info.hh"

#include "service/view_building_coordinator.hh"

static logging::logger vbc_logger("view_building_coordinator");

namespace service {

namespace vbc {

view_building_coordinator::view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm) 
    : _db(db)
    , _group0(group0)
    , _sys_ks(sys_ks)
    , _topo_sm(topo_sm)
    , _as(as) 
{}

future<group0_guard> view_building_coordinator::start_operation() {
    auto guard = co_await _group0.client().start_operation(_as);
    co_return std::move(guard);
}

future<> view_building_coordinator::await_event() {
    _as.check();
    co_await _cond.when();
    vbc_logger.debug("event awaited");
}

future<view_building_coordinator::vbc_state> view_building_coordinator::load_coordinator_state() {
    auto tasks = co_await _sys_ks.get_view_building_coordinator_tasks();
    auto processing_base = co_await _sys_ks.get_vbc_processing_base();

    co_return vbc_state {
        .tasks = std::move(tasks),
        .processing_base = std::move(processing_base),
    };
}

table_id view_building_coordinator::get_base_id(const view_name& view_name) {
    return _db.find_schema(view_name.first, view_name.second)->view_info()->base_id();
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
            
        }
        co_await coroutine::maybe_yield();
    }
}

future<std::optional<view_building_coordinator::vbc_state>> view_building_coordinator::update_coordinator_state() {
    vbc_logger.debug("update_coordinator_state()");

    auto guard = co_await start_operation();
    std::vector<canonical_mutation> cmuts;

    auto state = co_await load_coordinator_state();
    auto views = _db.get_views() 
            | std::views::transform([] (view_ptr& v) { return view_name{v->ks_name(), v->cf_name()}; })
            | std::ranges::to<std::vector>();
    auto built_views = co_await _sys_ks.load_built_tablet_views();

    if (auto to_add = get_views_to_add(state, views, built_views); !to_add.empty()) {
        for (auto& view: to_add) {
            auto muts = co_await add_view(guard, view);
            cmuts.insert(cmuts.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
        }
    } else if (auto to_remove = get_views_to_remove(state, views); !to_remove.empty()) {
        for (auto& view: to_remove) {
            auto muts = co_await remove_view(guard, view);
            cmuts.insert(cmuts.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));

            if (state.processing_base && *state.processing_base == get_base_id(view)) {
                auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
                cmuts.emplace_back(std::move(mut));
                state.processing_base = std::nullopt;
            }
        }
    } else if (!state.processing_base && !state.tasks.empty()) {
        // select base table to process
        auto& base_id = state.tasks.cbegin()->first;
        vbc_logger.info("Start building views for base table: {}", base_id);

        auto mut = co_await _sys_ks.make_vbc_processing_base_mutation(guard.write_timestamp(), base_id);
        cmuts.emplace_back(std::move(mut));
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

std::set<view_name> view_building_coordinator::get_views_to_add(const vbc_state& state, const std::vector<view_name>& views, const std::vector<view_name>& built) {
    std::set<view_name> views_to_add;
    for (auto& view: views) {
        if (!_db.find_keyspace(view.first).uses_tablets() || std::find(built.begin(), built.end(), view) != built.end()) {
            continue;
        }

        auto base_id = get_base_id(view);
        if (!state.tasks.contains(base_id) || !state.tasks.at(base_id).contains(view)) {
            views_to_add.insert(view);
        }
    }
    return views_to_add;
}

std::set<view_name> view_building_coordinator::get_views_to_remove(const vbc_state& state, const std::vector<view_name>& views) {
    std::set<view_name> views_to_remove;
    for (auto& [_, view_tasks]: state.tasks) {
        for (auto& [view, _]: view_tasks) {
            if (std::find(views.begin(), views.end(), view) == views.end()) {
                views_to_remove.insert(view);
            }
        }
    }
    return views_to_remove;
}

future<std::vector<canonical_mutation>> view_building_coordinator::add_view(const group0_guard& guard, const view_name& view_name) {
    vbc_logger.info("Register new view: {}.{}", view_name.first, view_name.second);

    auto base_id = get_base_id(view_name);
    auto& base_cf = _db.find_column_family(base_id);
    auto erm = base_cf.get_effective_replication_map();
    auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(base_id);

    std::vector<canonical_mutation> muts;
    for (auto tid = std::optional(tablet_map.first_tablet()); tid; tid = tablet_map.next_tablet(*tid)) {
        const auto& tablet_info = tablet_map.get_tablet_info(*tid);
        auto range = tablet_map.get_token_range(*tid);

        for (auto& replica: tablet_info.replicas) {
            auto mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view_name, replica.host, replica.shard, range);
            muts.emplace_back(std::move((mut)));
        }
    }
    co_return muts;
}

future<std::vector<canonical_mutation>> view_building_coordinator::remove_view(const group0_guard& guard, const view_name& view_name) {
    vbc_logger.info("Unregister all remaining tasks for view: {}.{}", view_name.first, view_name.second);
    
    auto muts = co_await _sys_ks.make_vbc_remove_view_tasks_mutations(guard.write_timestamp(), view_name);
    co_return std::vector<canonical_mutation>{muts.begin(), muts.end()};
}

future<> run_view_building_coordinator(std::unique_ptr<view_building_coordinator> vb_coordinator, replica::database& db, raft_group0& group0) {
    std::exception_ptr ex;
    db.get_notifier().register_listener(vb_coordinator.get());
    try {
        co_await with_scheduling_group(group0.get_scheduling_group(), [&] {
            return vb_coordinator->run();
        });
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        on_fatal_internal_error(vbc_logger, format("unhandled exception in view_building_coordinator::run(): {}", ex));
    }

    co_await db.get_notifier().unregister_listener(vb_coordinator.get());
}

}

}