/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <chrono>
#include <exception>
#include <iterator>
#include <ranges>
#include <seastar/core/coroutine.hh>
#include <fmt/ranges.h>

#include "cql3/query_processor.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "dht/i_partitioner_fwd.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "message/messaging_service.hh"
#include "mutation/canonical_mutation.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/loop.hh"
#include "seastar/core/sleep.hh"
#include "seastar/coroutine/maybe_yield.hh"
#include "seastar/coroutine/parallel_for_each.hh"
#include "seastar/rpc/rpc_types.hh"
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
#include "idl/view.dist.hh"

#include "service/view_building_coordinator.hh"

static logging::logger vbc_logger("view_building_coordinator");

namespace service {

namespace vbc {

view_building_coordinator::view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, netw::messaging_service& messaging, const topology_state_machine& topo_sm) 
    : _db(db)
    , _group0(group0)
    , _sys_ks(sys_ks)
    , _messaging(messaging)
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
    auto currently_processed_base_table = co_await _sys_ks.get_vbc_processing_base();

    vbc_logger.debug("Loaded state: {}", tasks);
    vbc_logger.debug("Processing base: {}", currently_processed_base_table);

    co_return vbc_state {
        .tasks = std::move(tasks),
        .currently_processed_base_table = std::move(currently_processed_base_table),
    };
}

table_id view_building_coordinator::get_base_id(const view_name& view_name) {
    return _db.find_schema(view_name.first, view_name.second)->view_info()->base_id();
}

bool view_building_coordinator::handle_error(std::exception_ptr eptr) noexcept {
    try {
        std::rethrow_exception(std::move(eptr));
    } catch (group0_concurrent_modification&) {
        vbc_logger.info("view building coordinator got group0_concurrent_modification");
    } catch (seastar::abort_requested_exception&) {
        vbc_logger.debug("view building coordinator aborted");
    } catch (raft::request_aborted&) {
        vbc_logger.debug("view building coordinator aborted");
    } catch (raft::commit_status_unknown&) {
        vbc_logger.debug("view building coordinator got commit_status_unknown");
    } catch (...) {
        vbc_logger.error("view building coordinator got error: {}", std::current_exception());
        return true;
    }
    return false;
}

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _cond.broadcast();
    });

    co_await abort_previous_coordinator();

    while (!_as.abort_requested()) {
        bool sleep = false;
        vbc_logger.debug("coordinator loop iteration");
        try {
            auto state_opt = co_await update_coordinator_state();
            if (!state_opt) {
                // If state_opt is nullopt, it means there was work to do and the state has changed.
                continue;
            }
            co_await build_view(std::move(*state_opt));
            co_await await_event();
        } catch (...) {
            sleep = handle_error(std::current_exception());
        }

        if (sleep) {
            try {
                co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                vbc_logger.debug("sleep failed: {}", std::current_exception());
            }
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

            if (state.currently_processed_base_table && *state.currently_processed_base_table == get_base_id(view)) {
                auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
                cmuts.emplace_back(std::move(mut));
                state.currently_processed_base_table = std::nullopt;
            }
        }
    } else if (!state.currently_processed_base_table && !state.tasks.empty()) {
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
    vbc_logger.debug("no updates to process, returning current state...");
    co_return state;
}

static std::optional<dht::token_range> get_range_to_build(const locator::tablet_map& tablet_map, const dht::token_range_vector ranges) {
    for (auto& range: ranges) {
        auto tid = tablet_map.get_tablet_id(range.end().value().value());
        auto trinfo = tablet_map.get_tablet_transition_info(tid);

        if (!trinfo) {
            return range;
        }
    }
    return std::nullopt;
}

static std::pair<std::vector<view_name>, dht::token_range> get_views_and_range_for_target(replica::database& db, table_id base_id, const base_tasks& base_tasks, const view_building_target& target) {
    std::vector<view_name> views;
    std::optional<dht::token_range> range;

    auto& tmap = db.get_token_metadata().tablets().get_tablet_map(base_id);
    for (auto& [view, tasks]: base_tasks) {
        if (!tasks.contains(target)) {
            continue;
        }

        if (!range) {
            auto maybe_range = get_range_to_build(tmap, tasks.at(target));
            if (maybe_range) {
                range = std::move(maybe_range);
                views.push_back(view);
            }
        } else {
            auto& target_tasks = tasks.at(target);
            if (std::find(target_tasks.cbegin(), target_tasks.cend(), *range) != target_tasks.cend()) {
                views.push_back(view);
            }
        }
    }

    if (!range) {
        return {{}, dht::token_range()};
    }
    return {std::move(views), *range};
}

future<> view_building_coordinator::build_view(vbc_state state) {
    if (!state.currently_processed_base_table) {
        vbc_logger.info("No view to process");
        co_return;
    }

    SCYLLA_ASSERT(state.tasks.contains(*state.currently_processed_base_table));
    auto& base_tasks = state.tasks[*state.currently_processed_base_table];

    for (auto& [id, replica_state]: _topo_sm._topology.normal_nodes) {
        locator::host_id host_id{id.uuid()};

        for (size_t shard = 0; shard < replica_state.shard_count; ++shard) {
            view_building_target target{host_id, shard};
            if (_rpc_handlers.contains(target) && !_rpc_handlers.at(target).available()) {
                vbc_logger.debug("Target {} is still processing request.", target);
                continue;
            }
            if (_rpc_handlers.contains(target)) {
                co_await std::move(_rpc_handlers.extract(target).mapped());
            }

            auto [views, range] = get_views_and_range_for_target(_db, *state.currently_processed_base_table, base_tasks, target);
            if (views.empty()) {
                vbc_logger.debug("No views to build for target {}", target);
                continue;
            }

            future<> rpc = send_task(target, *state.currently_processed_base_table, range, std::move(views));
            _rpc_handlers.insert({target, std::move(rpc)});
        }
    }
}

future<> view_building_coordinator::send_task(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views) {
    vbc_logger.info("Sending view building task to node {}, shard {} (token range: {} | views: {})", target.host, target.shard, range, views);
    auto name_to_id = [this] (const view_name& view_name) -> table_id {
        return _db.find_uuid(view_name.first, view_name.second);
    };
    std::vector<table_id> views_ids = views | std::views::transform(name_to_id) | std::ranges::to<std::vector>();

    try {
        _per_host_processing_range[target] = range;
        co_await ser::view_rpc_verbs::send_build_views_range(&_messaging, target.host, _as, base_id, target.shard, range, std::move(views_ids));
    } catch (...) {
        vbc_logger.warn("Building views for base: {}, range: {} on node: {}, shard: {} failed: {}", base_id, range, target.host, target.shard, std::current_exception());
        _per_host_processing_range.erase(target);
        _cond.broadcast();
        co_return;
    }

    int retries = 3;
    while (retries-- > 0) {
        bool sleep = false;
        try {
            co_await mark_task_completed(target, base_id, range, std::move(views));
        } catch (...) {
            sleep = handle_error(std::current_exception());
        }
        if (sleep) {
            try {
                co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                vbc_logger.debug("sleep failed: {}", std::current_exception());
            }
        }
    }
    _per_host_processing_range.erase(target);
    _cond.broadcast();
}

future<> view_building_coordinator::mark_task_completed(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views) {
    auto lock = co_await get_units(_rpc_response_mutex, 1, _as);
    auto guard = co_await _group0.client().start_operation(_as);
    auto state = co_await load_coordinator_state();

    std::vector<canonical_mutation> muts;
    auto& base_tasks = state.tasks[base_id];
    for (auto& view: views) {
        // Mark token_range as completed (remove it from vb state)
        auto mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target.host, target.shard, range);
        muts.emplace_back(std::move(mut));

        auto& ranges = base_tasks[view][target];
        std::erase(ranges, range);
        if (ranges.empty()) {
            base_tasks[view].erase(target);
        }
        vbc_logger.info("Token range {} (view: {}.{} | base_id: {}) was built on node {}, shard {}", range, view.first, view.second, base_id, target.host, target.shard);

        // Mark view as built if all tasks were completed
        if (base_tasks[view].empty()) {
            auto mut = co_await _sys_ks.make_tablet_view_built_mutation(guard.write_timestamp(), view);
            muts.emplace_back(std::move(mut));

            base_tasks.erase(view);
            vbc_logger.info("View {}.{} was built", view.first, view.second);
        }
    }

    // Unset currently processing base if all views were built
    if (base_tasks.empty()) {
        auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
        muts.emplace_back(std::move(mut));
        vbc_logger.info("All views for base {} were built", base_id);
    }

    auto cmd = _group0.client().prepare_command(write_mutations{.mutations = std::move(muts)}, guard, "finished view building step");
    co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);
}

future<> view_building_coordinator::abort_work(locator::host_id host, unsigned shard) {
    try {
        co_await ser::view_rpc_verbs::send_abort_view_building_work(&_messaging, host, shard);
    } catch (rpc::closed_error&) {
    } catch (...) {
        vbc_logger.warn("Error while aborting work on host {}, shard {}: {}", host, shard, std::current_exception());
    }
}

future<> view_building_coordinator::abort_previous_coordinator() {
    co_await coroutine::parallel_for_each(_topo_sm._topology.normal_nodes, [this] (auto& node) -> future<> {
        auto id = node.first;
        co_await coroutine::parallel_for_each(std::views::iota(0u, node.second.shard_count), [this, id] (auto shard) -> future<> {
            co_await abort_work(locator::host_id{id.uuid()}, shard);
        });
    });
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
            muts.emplace_back(std::move(mut));
        }
    }
    co_return muts;
}

future<std::vector<canonical_mutation>> view_building_coordinator::remove_view(const group0_guard& guard, const view_name& view_name) {
    vbc_logger.info("Unregister all remaining tasks for view: {}.{}", view_name.first, view_name.second);
    
    auto muts = co_await _sys_ks.make_vbc_remove_view_tasks_mutations(guard.write_timestamp(), view_name);
    co_return std::vector<canonical_mutation>{muts.begin(), muts.end()};
}

static bool contains_range(const dht::token_range_vector& ranges, const dht::token_range& range) {
    return std::find(ranges.cbegin(), ranges.cend(), range) != ranges.cend();
}

future<std::vector<mutation>> view_building_coordinator::get_migrate_tasks_mutations(const group0_guard& guard, table_id table_id, std::optional<locator::tablet_replica> abandoning_replica, std::optional<locator::tablet_replica> pending_replica, dht::token_range range) {
    auto state = co_await load_coordinator_state();
    if (!state.tasks.contains(table_id)) {
        co_return std::vector<mutation>();
    }

    std::vector<mutation> updates;
    for (auto& [view, tasks]: state.tasks[table_id]) {
        if (abandoning_replica && pending_replica) {
            view_building_target target {abandoning_replica->host, abandoning_replica->shard};
            if (tasks.contains(target) && contains_range(tasks[target], range)) {
                if (_per_host_processing_range.contains(target) && _per_host_processing_range[target] == range) {
                    co_await abort_work(target.host, target.shard);
                }

                auto del_mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target.host, target.shard, range);
                auto add_mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, pending_replica->host, pending_replica->shard, range);
                updates.push_back(std::move(del_mut));
                updates.push_back(std::move(add_mut));
                vbc_logger.info("Migrated task for view {}.{} with range {} from (host: {}, shard: {}) to (host: {}, shard: {})", view.first, view.second, range, target.host, target.shard, pending_replica->host, pending_replica->shard);
            }
        } else if (pending_replica) {
            auto mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, pending_replica->host, pending_replica->shard, range);
            updates.push_back(std::move(mut));
            vbc_logger.info("Added new task for view {}.{} with range {} on (host: {}, shard: {})", view.first, view.second, range, pending_replica->host, pending_replica->shard);
        } else if (abandoning_replica) {
            view_building_target target {abandoning_replica->host, abandoning_replica->shard};
            if (tasks.contains(target) && contains_range(tasks[target], range)) {
                if (_per_host_processing_range.contains(target) && _per_host_processing_range[target] == range) {
                    co_await abort_work(target.host, target.shard);
                }

                auto mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target.host, target.shard, range);
                updates.push_back(std::move(mut));
                vbc_logger.info("Deleted task for view {}.{} with range {} from (host: {}, shard: {})", view.first, view.second, range, target.host, target.shard);
            }
        }
    }
    co_return updates;
}

future<> view_building_coordinator::stop() {
    co_await coroutine::parallel_for_each(std::move(_rpc_handlers), [] (auto&& rpc_call) -> future<> {
        co_await std::move(rpc_call.second);
    });
}

}

}
