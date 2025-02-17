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
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/sleep.hh>
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
#include "seastar/core/on_internal_error.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/topology_coordinator.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "service/migration_manager.hh"
#include "replica/database.hh"
#include "view_info.hh"
#include "idl/view.dist.hh"

#include "service/view_building_coordinator.hh"

static logging::logger vbc_logger("view_building_coordinator");

namespace service {

view_building_coordinator::view_building_coordinator(abort_source& as, replica::database& db, raft::server& raft, raft_group0& group0, db::system_keyspace& sys_ks, netw::messaging_service& messaging, const topology_state_machine& topo_sm, const raft::term_t term) 
    : _db(db)
    , _raft(raft)
    , _group0(group0)
    , _sys_ks(sys_ks)
    , _messaging(messaging)
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
    auto currently_processed_base_table = co_await _sys_ks.get_vbc_processing_base();

    vbc_logger.debug("Loaded state: {}", tasks);
    vbc_logger.debug("Processing base: {}", currently_processed_base_table);

    co_return vbc_state {
        .tasks = std::move(tasks),
        .currently_processed_base_table = std::move(currently_processed_base_table),
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

bool view_building_coordinator::handle_error(std::exception_ptr eptr) noexcept {
    try {
        std::rethrow_exception(std::move(eptr));
    } catch (group0_concurrent_modification&) {
        vbc_logger.info("view building coordinator got group0_concurrent_modification");
    } catch (term_changed_error&) {
        vbc_logger.debug("view building coordinator got term_changed_error");
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

    // Since vbc_state is immutable, update currently processing base table only when there are no other changes to the state.
    if (cmuts.empty()) {
        if (state.currently_processed_base_table) {
            // Check if the base table should continue to be built. Maybe we should stop building
            // the base table if all views were dropped.
            auto base_id = *state.currently_processed_base_table;

            if (state.tasks.contains(base_id) && state.tasks.at(base_id).empty()) {
                vbc_logger.info("Base table {} no longer has any views to build.", base_id);
                auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
                cmuts.emplace_back(std::move(mut));
            }
        } else {
            // Select next base table to build if there is any.
            if (!state.tasks.empty()) {
                auto& base_id = state.tasks.cbegin()->first;
                vbc_logger.info("Start building views for base table: {}", base_id);

                auto mut = co_await _sys_ks.make_vbc_processing_base_mutation(guard.write_timestamp(), base_id);
                cmuts.emplace_back(std::move(mut));
            }
        }
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

static std::optional<dht::token_range> get_range_to_build(const locator::tablet_map& tablet_map, const dht::token_range_vector& ranges) {
    for (auto& range: ranges) {
        auto tid = tablet_map.get_tablet_id(range.end().value().value());
        auto trinfo = tablet_map.get_tablet_transition_info(tid);

        if (!trinfo) {
            return range;
        }
    }
    return std::nullopt;
}

static std::pair<std::vector<table_id>, dht::token_range> get_views_and_range_for_target(replica::database& db, table_id base_id, const base_building_tasks& base_tasks, const view_building_target& target) {
    std::vector<table_id> views;
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

    if (!state.tasks.contains(*state.currently_processed_base_table)) {
        on_internal_error(vbc_logger, "No tasks for currently processed base table");
    }
    auto& base_tasks = state.tasks[*state.currently_processed_base_table];

    for (auto& [id, replica_state]: _topo_sm._topology.normal_nodes) {
        locator::host_id host_id{id.uuid()};

        for (size_t shard = 0; shard < replica_state.shard_count; ++shard) {
            view_building_target target{host_id, shard};
            if (_remote_work_map.contains(target) && !_remote_work_map.at(target).available()) {
                vbc_logger.debug("Target {} is still processing request.", target);
                continue;
            }
            if (_remote_work_map.contains(target)) {
                co_await std::move(_remote_work_map.extract(target).mapped());
            }

            auto [views, range] = get_views_and_range_for_target(_db, *state.currently_processed_base_table, base_tasks, target);
            if (views.empty()) {
                vbc_logger.debug("No views to build for target {}", target);
                continue;
            }

            future<> rpc = send_task(target, *state.currently_processed_base_table, range, std::move(views));
            _remote_work_map.insert({target, std::move(rpc)});
        }
    }
}

future<> view_building_coordinator::send_task(view_building_target target, table_id base_id, dht::token_range range, std::vector<table_id> views) {
    vbc_logger.info("Sending view building task to node {}, shard {} (token range: {} | views: {})", target.host, target.shard, range, views);

    std::vector<table_id> rpc_result;
    try {
        rpc_result = co_await ser::view_rpc_verbs::send_build_views_range(&_messaging, target.host, _as, base_id, target.shard, range, std::move(views), _term);
    } catch (...) {
        vbc_logger.warn("Building views for base: {}, range: {} on node: {}, shard: {} failed: {}", base_id, range, target.host, target.shard, std::current_exception());
        _cond.broadcast();
        co_return;
    }

    int retries = 3;
    while (retries-- > 0) {
        bool sleep = false;
        try {
            co_await mark_task_completed(target, base_id, range, std::move(rpc_result));
            break;
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
    _cond.broadcast();
}

future<> view_building_coordinator::mark_task_completed(view_building_target target, table_id base_id, dht::token_range range, std::vector<table_id> views) {
    auto guard = co_await _group0.client().start_operation(_as);
    auto state = co_await load_coordinator_state();

    std::vector<canonical_mutation> muts;
    auto& base_tasks = state.tasks[base_id];
    for (auto& view: views) {
        // Mark token_range as completed (remove it from vb state)
        auto mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target, range);
        muts.emplace_back(std::move(mut));

        auto& ranges = base_tasks[view][target];
        std::erase(ranges, range);
        if (ranges.empty()) {
            base_tasks[view].erase(target);
        }
        vbc_logger.trace("Token range {} (view: {} | base_id: {}) was built on node {}, shard {}", range, view, base_id, target.host, target.shard);

        // Mark view as built if all tasks were completed
        if (base_tasks[view].empty()) {
            auto view_name = table_id_to_name(view);
            auto mut = co_await _sys_ks.make_mark_view_as_built_mutation(guard.write_timestamp(), view_name.first, view_name.second);
            muts.emplace_back(std::move(mut));

            base_tasks.erase(view);
            vbc_logger.info("View {}.{} was built", view_name.first, view_name.second);
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

future<> view_building_coordinator::abort_work(const view_building_target& target) {
    vbc_logger.trace("Sending abort request to {}", target);
    try {
        co_await ser::view_rpc_verbs::send_abort_view_building_work(&_messaging, target.host, target.shard, _term);
    } catch (rpc::closed_error&) {
    } catch (...) {
        vbc_logger.warn("Error while aborting work on host {}, shard {}: {}", target.host, target.shard, std::current_exception());
    }
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

future<> view_building_coordinator::stop() {
    co_await coroutine::parallel_for_each(std::move(_remote_work_map), [] (auto&& rpc_call) -> future<> {
        co_await std::move(rpc_call.second);
    });
}

}
