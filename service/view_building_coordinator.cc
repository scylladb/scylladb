/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <chrono>
#include <cstdlib>
#include <algorithm>
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
#include "db/view/view_build_status.hh"
#include "db/view/view_builder.hh"
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

static const int RPC_RESPONSE_RETRIES_NUM = 3;

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
    auto targets_with_staging_sstables = co_await _sys_ks.get_view_building_coordinator_staging_sstables_targets();
    auto status_map = co_await _sys_ks.get_view_build_status_map() | std::views::transform([this] (auto entry) {
        return std::make_pair(table_name_to_id(entry.first), entry.second);
    }) | std::ranges::to<view_build_status_map>();

    vbc_logger.debug("Loaded state: {}", tasks);
    vbc_logger.debug("Processing base: {}", currently_processed_base_table);
    vbc_logger.debug("Targets with staging sstables: {}", targets_with_staging_sstables);
    vbc_logger.debug("Status map: {}", status_map);

    co_return vbc_state {
        .tasks = std::move(tasks),
        .currently_processed_base_table = std::move(currently_processed_base_table),
        .targets_with_staging_sstables = std::move(targets_with_staging_sstables),
        .status_map = std::move(status_map)
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
            auto muts = co_await remove_view(guard, view_id, state);
            cmuts.insert(cmuts.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
        }
    }
    if (auto built_to_remove = get_built_views_to_remove(built_views, views); !built_to_remove.empty()) {
        for (auto& view_id: built_to_remove) {
            auto muts = co_await remove_built_view(guard, view_id);
            cmuts.insert(cmuts.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
        }
    }

    // Since vbc_state is immutable, update currently processing base table only when there are no other changes to the state.
    if (cmuts.empty()) {
        if (state.currently_processed_base_table) {
            // Check if the base table should continue to be built. Maybe we should stop building
            // the base table if all views were dropped.
            auto base_id = *state.currently_processed_base_table;

            if ((!state.tasks.contains(base_id) || state.tasks.at(base_id).empty()) && state.targets_with_staging_sstables.empty()) {
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
                auto status_muts = co_await mark_build_status_started_on_all_nodes(guard, state.tasks.at(base_id) | std::views::keys | std::ranges::to<std::vector>());
                cmuts.emplace_back(std::move(mut));
                cmuts.insert(cmuts.end(), std::make_move_iterator(status_muts.begin()), std::make_move_iterator(status_muts.end()));
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

static bool contains_range(const dht::token_range_vector& ranges, const dht::token_range& range) {
    return std::find(ranges.cbegin(), ranges.cend(), range) != ranges.cend();
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

static dht::token_range_vector get_staging_sstables_ranges_for_target(replica::database& db, table_id base_id, const base_building_tasks& base_tasks, const view_building_target& target, const dht::token_range_vector& ranges) {
    auto& tmap = db.get_token_metadata().tablets().get_tablet_map(base_id);

    auto is_range_built_for_all_views = [&base_tasks, &target] (const dht::token_range& range) {
        return std::ranges::all_of(base_tasks | std::views::values, [&] (const view_building_tasks& view_tasks) {
            return !contains_range(view_tasks.at(target), range);
        });
    };
    
    dht::token_range_vector sstables_to_register;
    for (auto& range: ranges) {
        // The range represents data within staging sstable.
        // Since the sstable is for tablet-based table, the data belongs to only one tablet.
        auto tid = tmap.get_tablet_id(range.end()->value());
        auto trinfo = tmap.get_tablet_transition_info(tid);

        if (!trinfo && is_range_built_for_all_views(range)) {
            sstables_to_register.push_back(range);
        }
    }
    return sstables_to_register;
}

future<> view_building_coordinator::build_view(vbc_state state) {
    if (!state.currently_processed_base_table) {
        vbc_logger.info("No view to process");
        co_return;
    }

    if (!state.tasks.contains(*state.currently_processed_base_table) && state.targets_with_staging_sstables.empty()) {
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

            std::optional<future<>> task_rpc_opt;
            if (auto [views, range] = get_views_and_range_for_target(_db, *state.currently_processed_base_table, base_tasks, target); !views.empty()) {
                task_rpc_opt = send_building_task(target, *state.currently_processed_base_table, range, std::move(views));
            } else if (auto sstables_to_register = get_staging_sstables_ranges_for_target(_db, *state.currently_processed_base_table, base_tasks, target, state.targets_with_staging_sstables[target]); !sstables_to_register.empty()) {
                task_rpc_opt = send_register_staging_task(target, *state.currently_processed_base_table, std::move(sstables_to_register));
            }

            if (task_rpc_opt) {
                _remote_work_map.insert({target, std::move(*task_rpc_opt)});
            } else {
                vbc_logger.debug("No work for target {}", target);
            }
        }
    }
}

future<> view_building_coordinator::send_building_task(view_building_target target, table_id base_id, dht::token_range range, std::vector<table_id> views) {
    vbc_logger.info("Sending view building task to node {}, shard {} (token range: {} | views: {})", target.host, target.shard, range, views);

    std::vector<table_id> rpc_result;
    try {
        _per_host_processing_range[target] = range;
        rpc_result = co_await ser::view_rpc_verbs::send_build_views_range(&_messaging, target.host, _as, base_id, target.shard, range, std::move(views), _term);
    } catch (...) {
        vbc_logger.warn("Building views for base: {}, range: {} on node: {}, shard: {} failed: {}", base_id, range, target.host, target.shard, std::current_exception());
        _per_host_processing_range.erase(target);
        _cond.broadcast();
        co_return;
    }

    int retries = RPC_RESPONSE_RETRIES_NUM;
    while (retries-- > 0) {
        bool sleep = false;
        try {
            co_await mark_building_task_completed(target, base_id, range, std::move(rpc_result));
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
    _per_host_processing_range.erase(target);
    _cond.broadcast();
}

future<> view_building_coordinator::send_register_staging_task(view_building_target target, table_id base_id, dht::token_range_vector ranges) {
    vbc_logger.info("Sending register staging sstables task to node {}, shard {} (ranges: {})", target.host, target.shard, ranges);

    try {
        co_await ser::view_rpc_verbs::send_register_staging_sstables(&_messaging, target.host, _as, base_id, target.shard, ranges);
    } catch (...) {
        vbc_logger.warn("Processing staging sstable on {} failed: {}", target, std::current_exception());
        _cond.broadcast();
        co_return;
    }

    int retires = RPC_RESPONSE_RETRIES_NUM;
    while (retires-- > 0) {
        bool sleep = false;
        try {
            co_await mark_staging_task_completed(target, base_id, ranges);
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

future<> view_building_coordinator::mark_building_task_completed(view_building_target target, table_id base_id, dht::token_range range, std::vector<table_id> views) {
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
            // This means the view was built on the target.
            base_tasks[view].erase(target);
            auto status_mut_opt = co_await maybe_mark_build_status_success(guard, base_tasks[view], state.targets_with_staging_sstables, view, target.host);
            if (status_mut_opt) {
                muts.emplace_back(std::move(*status_mut_opt));
            }
        }
        vbc_logger.trace("Token range {} (view: {} | base_id: {}) was built on node {}, shard {}", range, view, base_id, target.host, target.shard);

        // Mark view as built if all tasks were completed
        if (base_tasks[view].empty() && state.targets_with_staging_sstables.empty()) {
            auto view_name = table_id_to_name(view);
            auto mut = co_await _sys_ks.make_mark_view_as_built_mutation(guard.write_timestamp(), view_name.first, view_name.second);
            auto status_muts = co_await mark_build_status_success_on_remaining_nodes(guard, state, view);
            muts.emplace_back(std::move(mut));
            muts.insert(muts.end(), std::make_move_iterator(status_muts.begin()), std::make_move_iterator(status_muts.end()));

            base_tasks.erase(view);
            vbc_logger.info("View {}.{} was built", view_name.first, view_name.second);
        }
    }

    // Unset currently processing base if all views were built
    if (base_tasks.empty() && state.targets_with_staging_sstables.empty()) {
        auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
        muts.emplace_back(std::move(mut));
        vbc_logger.info("All views for base {} were built", base_id);
    }

    auto cmd = _group0.client().prepare_command(write_mutations{.mutations = std::move(muts)}, guard, "finished view building step");
    co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);
}

future<> view_building_coordinator::mark_staging_task_completed(view_building_target target, table_id base_id, dht::token_range_vector ranges) {
    auto guard = co_await _group0.client().start_operation(_as);
    auto state = co_await load_coordinator_state();

    std::vector<canonical_mutation> muts;
    for (auto& range: ranges) {
        auto mut = co_await _sys_ks.make_vbc_staging_sstable_done_mutation(guard.write_timestamp(), target.host, target.shard, range);
        muts.emplace_back(std::move(mut));
        std::erase(state.targets_with_staging_sstables[target], range);
        vbc_logger.trace("Staging sstable corresponding to range {} was processed on node {}, shard {}", range, target.host, target.shard);
    }

    bool no_views_to_build = !state.tasks.contains(base_id); // We may modity `state.tasks` locally by inserting empty objects. Save information if there are no views to build for this base_id for later.
    auto no_building_task_left = [&] (table_id view_id) {
        return no_views_to_build || !state.tasks[base_id].contains(view_id) || state.tasks[base_id][view_id].empty();
    };
    auto host_has_any_task_left = [&] (table_id view_id) {
        return !no_building_task_left(view_id) && std::any_of(state.tasks[base_id][view_id].begin(), state.tasks[base_id][view_id].end(), [&] (auto& e) {
            return e.first.host == target.host;
        });
    };

    if (state.targets_with_staging_sstables[target].empty()) {
        state.targets_with_staging_sstables.erase(target);

        // If all ranges were built, there is no entry in `state.tasks`, so check `state.status_map` instead.
        for (auto& [view_id, statuses]: state.status_map) {
            if (base_id != get_base_table_id(view_id) || host_has_any_task_left(view_id)) {
                continue;
            }

            // Mark build status `SUCCESS` if all staging sstables were processed and all view ranges were built on given host.
            //
            // `state.tasks[base_id][view_id]` may insert an objects in the maps but they will be empty entries and that's ok.
            auto status_mut_opt = co_await maybe_mark_build_status_success(guard, state.tasks[base_id][view_id], state.targets_with_staging_sstables, view_id, target.host);
            if (status_mut_opt) {
                muts.emplace_back(std::move(*status_mut_opt));
            }
            
            // If the view was built on all targets and there are no staging sstables to process on all targets, mark the view as built.
            if (no_building_task_left(view_id) && state.targets_with_staging_sstables.empty()) {
                auto view_name = table_id_to_name(view_id);
                auto mut = co_await _sys_ks.make_mark_view_as_built_mutation(guard.write_timestamp(), view_name.first, view_name.second);
                auto status_muts = co_await mark_build_status_success_on_remaining_nodes(guard, state, view_id);
                muts.emplace_back(std::move(mut));
                muts.insert(muts.end(), std::make_move_iterator(status_muts.begin()), std::make_move_iterator(status_muts.end()));
                vbc_logger.info("View {}.{} was built", view_name.first, view_name.second);
            }
        }
    }

    if (no_views_to_build && state.targets_with_staging_sstables.empty()) {
        auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
        muts.emplace_back(std::move(mut));
        vbc_logger.info("All views for base {} were built", base_id);
    }

    auto cmd = _group0.client().prepare_command(write_mutations{.mutations = std::move(muts)}, guard, "finished staging sstables processing step");
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

future<std::vector<canonical_mutation>> view_building_coordinator::mark_build_status_started_on_all_nodes(const group0_guard& guard, const std::vector<table_id>& views) {
    std::vector<canonical_mutation> muts;
    for (auto& [id, _]: _topo_sm._topology.normal_nodes) {
        locator::host_id host_id{id.uuid()};
        for (auto& view: views) {
            auto mut = co_await _sys_ks.make_view_build_status_mutation(guard.write_timestamp(), table_id_to_name(view), host_id, db::view::build_status::STARTED);
            muts.emplace_back(std::move(mut));
        }
    }
    co_return muts;
}

future<std::optional<mutation>> view_building_coordinator::maybe_mark_build_status_success(const group0_guard& guard, const view_building_tasks& view_tasks, const view_building_staging_sstables_map& staging_tasks, table_id view, locator::host_id host_id) {
    bool host_has_any_tasks = std::any_of(view_tasks.begin(), view_tasks.end(), [&] (auto& e) {
        return e.first.host == host_id;
    });
    if (host_has_any_tasks || !staging_tasks.empty()) {
        co_return std::nullopt;
    }
    co_return co_await _sys_ks.make_view_build_status_update_mutation(guard.write_timestamp(), table_id_to_name(view), host_id, db::view::build_status::SUCCESS);
}

future<std::vector<canonical_mutation>> view_building_coordinator::mark_build_status_success_on_remaining_nodes(const group0_guard& guard, vbc_state& state, table_id view) {
    std::vector<canonical_mutation> muts;
    for (auto& [id, _]: _topo_sm._topology.normal_nodes) {
        locator::host_id host_id{id.uuid()};
        
        if (!state.status_map[view].contains(host_id)) {
            auto mut = co_await _sys_ks.make_view_build_status_mutation(guard.write_timestamp(), table_id_to_name(view), host_id, db::view::build_status::SUCCESS);
            muts.emplace_back(std::move(mut));
        }
    }
    co_return muts;
}

std::set<table_id> view_building_coordinator::get_views_to_add(const vbc_state& state, const std::vector<table_id>& views, const std::vector<table_id>& built) {
    std::set<table_id> views_to_add;
    for (auto& view_id: views) {
        if (!state.status_map.contains(view_id) && std::find(built.begin(), built.end(), view_id) == built.end()) {
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
    auto status_muts = co_await mark_build_status_started_on_all_nodes(guard, {view_id});
    muts.insert(muts.end(), std::make_move_iterator(status_muts.begin()), std::make_move_iterator(status_muts.end()));
    co_return muts;
}

future<std::vector<canonical_mutation>> view_building_coordinator::remove_view(const group0_guard& guard, const table_id& view_id, const vbc_state& state) {
    auto view_name = table_id_to_name(view_id);
    vbc_logger.info("Unregister all remaining tasks for view: {}.{}", view_name.first, view_name.second);
    
    auto muts = co_await _sys_ks.make_vbc_remove_view_tasks_mutations(guard.write_timestamp(), view_id);
    std::vector<canonical_mutation> cmuts{muts.begin(), muts.end()};
    if (state.status_map.contains(view_id)) {
        auto status_mut = co_await _sys_ks.make_remove_view_build_status_mutation(guard.write_timestamp(), view_name);
        cmuts.emplace_back(std::move(status_mut));
    }
    co_return cmuts;
}

future<std::vector<canonical_mutation>> view_building_coordinator::remove_built_view(const group0_guard& guard, const table_id& view_id) {
    auto view_name = table_id_to_name(view_id);
    vbc_logger.info("Remove status for built view: {}.{}", view_name.first, view_name.second);

    auto built_mut = co_await _sys_ks.make_remove_built_view_mutation(guard.write_timestamp(), view_name.first, view_name.second);
    auto status_mut = co_await _sys_ks.make_remove_view_build_status_mutation(guard.write_timestamp(), view_name);

    co_return std::vector<canonical_mutation>{
        canonical_mutation(std::move(built_mut)), 
        canonical_mutation(std::move(status_mut))
    };
}

future<> view_building_coordinator::maybe_prepare_for_tablet_migration_start(const group0_guard& guard, table_id table_id, const locator::tablet_replica& abandoning_replica, const dht::token_range& range) {
    auto state = co_await load_coordinator_state();
    if (state.currently_processed_base_table != table_id) {
        co_return;
    }

    if (_per_host_processing_range.contains(abandoning_replica) && _per_host_processing_range[abandoning_replica] == range) {
        co_await abort_work(abandoning_replica);
    }
}

future<> view_building_coordinator::maybe_prepare_for_tablet_resize_start(const group0_guard& guard, table_id table_id) {
    auto state = co_await load_coordinator_state();
    if (state.currently_processed_base_table != table_id) {
        co_return;
    }

    for (auto& target: _remote_work_map | std::views::keys) {
        co_await abort_work(target);
    }
}

future<std::vector<mutation>> view_building_coordinator::get_migrate_tasks_mutations(const group0_guard& guard, table_id table_id, std::optional<locator::tablet_replica> abandoning_replica, std::optional<locator::tablet_replica> pending_replica, const dht::token_range& range) {
    auto state = co_await load_coordinator_state();
    if (!state.tasks.contains(table_id)) {
        co_return std::vector<mutation>();
    }

    std::vector<mutation> updates;
    for (auto& [view, tasks]: state.tasks[table_id]) {
        if (abandoning_replica && pending_replica) {
            if (tasks.contains(*abandoning_replica) && contains_range(tasks[*abandoning_replica], range)) {
                auto del_mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, *abandoning_replica, range);
                auto add_mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, *pending_replica, range);
                updates.push_back(std::move(del_mut));
                updates.push_back(std::move(add_mut));
                vbc_logger.info("Migrated task for view {} with range {} from (host: {}, shard: {}) to (host: {}, shard: {})", view, range, abandoning_replica->host, abandoning_replica->shard, pending_replica->host, pending_replica->shard);
            }
        } else if (pending_replica) {
            auto mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, *pending_replica, range);
            updates.push_back(std::move(mut));
            vbc_logger.info("Added new task for view {} with range {} on (host: {}, shard: {})", view, range, pending_replica->host, pending_replica->shard);
        } else if (abandoning_replica) {
            if (tasks.contains(*abandoning_replica) && contains_range(tasks[*abandoning_replica], range)) {
                auto mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, *abandoning_replica, range);
                updates.push_back(std::move(mut));
                vbc_logger.info("Deleted task for view {} with range {} from (host: {}, shard: {})", view, range, abandoning_replica->host, abandoning_replica->shard);
            }
        }
    }
    co_return updates;
}

future<std::vector<mutation>> view_building_coordinator::get_resize_tasks_mutations(const group0_guard& guard, table_id table_id, const locator::tablet_map& tablet_map, const locator::tablet_map& new_tablet_map) {
    auto state = co_await load_coordinator_state();
    if (!state.tasks.contains(table_id)) {
        co_return std::vector<mutation>();
    }

    std::vector<mutation> updates;
    for (auto& [view, view_tasks]: state.tasks[table_id]) {
        for (auto& [target, tasks]: view_tasks) {
            std::vector<mutation> muts;
            if (tablet_map.needs_split()) {
                muts = co_await get_split_mutations(guard, tablet_map, view, target, tasks);
            } else if (tablet_map.needs_merge()) {
                muts = co_await get_merge_mutations(guard, tablet_map, new_tablet_map, view, target, tasks);                
            }
            updates.insert(updates.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
        }
    }

    co_return updates;
}

future<std::vector<mutation>> view_building_coordinator::get_split_mutations(const group0_guard& guard, const locator::tablet_map& tablet_map, table_id view, const view_building_target& target, const std::vector<dht::token_range>& tasks) {
    std::vector<mutation> updates;
    updates.reserve(tasks.size() * 3);
    for (auto& range: tasks) {
        auto tid = tablet_map.get_tablet_id(range.end()->value());

        auto left_range = tablet_map.get_token_range_after_split(tablet_map.get_first_token(tid));
        auto right_range = tablet_map.get_token_range_after_split(tablet_map.get_last_token(tid));

        auto del_mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target, range);
        auto left_mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, target, left_range);
        auto right_mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, target, right_range);
        updates.push_back(std::move(del_mut));
        updates.push_back(std::move(left_mut));
        updates.push_back(std::move(right_mut));
    }
    co_return updates;
}

future<std::vector<mutation>> view_building_coordinator::get_merge_mutations(const group0_guard& guard, const locator::tablet_map& tablet_map, const locator::tablet_map& new_tablet_map, table_id view, const view_building_target& target, const std::vector<dht::token_range>& tasks) {
    std::vector<mutation> updates;
    std::unordered_set<locator::tablet_id> processed_ids;
    for (auto& range: tasks) {
        auto tid = tablet_map.get_tablet_id(range.end()->value());
        auto new_tid = locator::tablet_id(tid.value() >> 1);

        auto del_mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target, range);
        updates.push_back(std::move(del_mut));

        if (processed_ids.contains(new_tid)) {
            continue;
        }
        processed_ids.insert(new_tid);

        auto new_range = new_tablet_map.get_token_range(new_tid);
        auto add_mut = co_await _sys_ks.make_vbc_task_mutation(guard.write_timestamp(), view, target, new_range);
        updates.push_back(std::move(add_mut));
    }
    co_return updates;
}

future<> view_building_coordinator::stop() {
    co_await coroutine::parallel_for_each(std::move(_remote_work_map), [] (auto&& rpc_call) -> future<> {
        co_await std::move(rpc_call.second);
    });
}

}
