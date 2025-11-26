/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <algorithm>
#include <exception>
#include <ranges>
#include <seastar/core/abort_source.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/on_internal_error.hh>
#include "db/view/view_building_coordinator.hh"
#include "db/view/view_build_status.hh"
#include "locator/tablets.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "raft/raft.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "raft/server.hh"
#include "service/topology_coordinator.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"
#include "db/view/view_building_task_mutation_builder.hh"
#include "utils/assert.hh"
#include "idl/view.dist.hh"
#include "utils/error_injection.hh"
#include "utils/log.hh"

static logging::logger vbc_logger("view_building_coordinator");

namespace db {

namespace view {

view_building_coordinator::view_building_coordinator(replica::database& db, raft::server& raft, service::raft_group0& group0,
            db::system_keyspace& sys_ks, gms::gossiper& gossiper, netw::messaging_service& ms,
            view_building_state_machine& vb_sm, const service::topology_state_machine& topo_sm,
            raft::term_t term, abort_source& as)
    : _db(db)
    , _raft(raft)
    , _group0(group0)
    , _sys_ks(sys_ks)
    , _gossiper(gossiper)
    , _messaging(ms)
    , _vb_sm(vb_sm)
    , _topo_sm(topo_sm)
    , _term(term)
    , _as(as) {}

future<service::group0_guard> view_building_coordinator::start_operation() {
    auto guard = co_await _group0.client().start_operation(_as, service::raft_timeout{});
    vbc_logger.debug("starting new operation");
    if (_term != _raft.get_current_term()) {
        throw service::term_changed_error{};
    }
    co_return std::move(guard);
}

future<> view_building_coordinator::await_event() {
    _as.check();
    vbc_logger.debug("waiting for view building state machine event");
    co_await _vb_sm.event.when();
    vbc_logger.debug("event received");
}

future<> view_building_coordinator::commit_mutations(service::group0_guard guard, utils::chunked_vector<mutation> mutations, std::string_view description) {
    utils::chunked_vector<canonical_mutation> cmuts = {mutations.begin(), mutations.end()};
    auto cmd = _group0.client().prepare_command(service::write_mutations{
        .mutations{std::move(cmuts)}
    }, guard, description);
    co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);
}

void view_building_coordinator::handle_coordinator_error(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (service::group0_concurrent_modification&) {
        vbc_logger.info("view building coordinator got group0_concurrent_modification");
    } catch (abort_requested_exception&) {
        vbc_logger.debug("view building coordinator got abort_requested_exception");
    } catch (raft::request_aborted&) {
        vbc_logger.debug("view building coordinator got raft::request_aborted");
    } catch (service::term_changed_error&) {
        vbc_logger.debug("view building coordinator notices term change {} -> {}", _term, _raft.get_current_term());
    } catch (raft::commit_status_unknown&) {
        vbc_logger.warn("view building coordinator got raft::commit_status_unknown");
    } catch (...) {
        vbc_logger.error("view building coordinator got error: {}", std::current_exception());
    }
}

void view_building_coordinator::on_up(const gms::inet_address& endpoint, locator::host_id host_id) {
    _vb_sm.event.broadcast();
}

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _vb_sm.event.broadcast();
    });

    auto finished_tasks_gc_fiber = finished_task_gc_fiber();

    while (!_as.abort_requested()) {
        co_await utils::get_local_injector().inject("view_building_coordinator_pause_main_loop", utils::wait_for_message(std::chrono::minutes(2)));
        if (utils::get_local_injector().enter("view_building_coordinator_skip_main_loop")) {
            co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            continue;
        }

        bool sleep = false;
        try {
            auto guard_opt = co_await update_state(co_await start_operation());
            if (!guard_opt) {
                // If `update_state()` returned guard, this means it committed some mutations
                // and the state was changed.
                vbc_logger.debug("view building coordinator state was changed, do next iteration without waiting for event");
                continue;
            }

            co_await work_on_view_building(std::move(*guard_opt));
            co_await await_event();
        } catch (...) {
            handle_coordinator_error(std::current_exception());
            sleep = true;
        }

        if (sleep && !_as.abort_requested()) {
            try {
                vbc_logger.debug("Sleeping after exception.");
                co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                vbc_logger.warn("sleep failed: {}", std::current_exception());
            }
        }
    }

    co_await std::move(finished_tasks_gc_fiber);
}

future<> view_building_coordinator::finished_task_gc_fiber() {
    static auto task_gc_interval = 200ms;

    while (!_as.abort_requested()) {
        try {
            co_await clean_finished_tasks();
            co_await sleep_abortable(task_gc_interval, _as);
        } catch (abort_requested_exception&) {
            vbc_logger.debug("view_building_coordinator::finished_task_gc_fiber got abort_requested_exception");
        } catch (service::group0_concurrent_modification&) {
            vbc_logger.info("view_building_coordinator::finished_task_gc_fiber got group0_concurrent_modification");
        } catch (raft::request_aborted&) {
            vbc_logger.debug("view_building_coordinator::finished_task_gc_fiber got raft::request_aborted");
        } catch (service::term_changed_error&) {
            vbc_logger.debug("view_building_coordinator::finished_task_gc_fiber notices term change {} -> {}", _term, _raft.get_current_term());
        } catch (raft::commit_status_unknown&) {
            vbc_logger.warn("view_building_coordinator::finished_task_gc_fiber got raft::commit_status_unknown");
        } catch (...) {
            vbc_logger.error("view_building_coordinator::finished_task_gc_fiber got error: {}", std::current_exception());
        }
    }
}

future<> view_building_coordinator::clean_finished_tasks() {
    // Avoid acquiring a group0 operation if there are no tasks.
    if (_finished_tasks.empty()) {
        co_return;
    }

    auto guard = co_await start_operation();
    auto lock = co_await get_unique_lock(_mutex);

    if (!_vb_sm.building_state.currently_processed_base_table || std::ranges::all_of(_finished_tasks, [] (auto& e) { return e.second.empty(); })) {
        co_return;
    }

    view_building_task_mutation_builder builder(guard.write_timestamp());
    for (auto& [replica, tasks]: _finished_tasks) {
        for (auto& task_id: tasks) {
            // The task might be aborted in the meantime. In this case we cannot remove it because we need it to create a new task.
            //
            // TODO: When we're aborting a view building task (for instance due to tablet migration),
            //       we can look if we already finished it (check if it's in `_finished_tasks`).
            //       If yes, we can just remove it instead of aborting it.
            auto task_opt = _vb_sm.building_state.get_task(*_vb_sm.building_state.currently_processed_base_table, replica, task_id);
            if (task_opt && !task_opt->get().aborted) {
                builder.del_task(task_id);
                vbc_logger.debug("Removing finished task with ID: {}", task_id);
            }
        }
    }

    co_await commit_mutations(std::move(guard), {builder.build()}, "remove finished view building tasks");
    for (auto& [_, tasks_set]: _finished_tasks) {
        tasks_set.clear();
    }
}

future<std::optional<service::group0_guard>> view_building_coordinator::update_state(service::group0_guard guard) {
    vbc_logger.debug("Currently processed base table: {}", _vb_sm.building_state.currently_processed_base_table);

    utils::chunked_vector<mutation> muts;
    if (_vb_sm.building_state.currently_processed_base_table) {
        auto base_id = *_vb_sm.building_state.currently_processed_base_table;
        vbc_logger.debug("Tasks for currently processed base table: {}", _vb_sm.building_state.tasks_state.contains(base_id) ? _vb_sm.building_state.tasks_state.at(base_id) : base_table_tasks{});

        co_await update_views_statuses(guard, base_id, muts);
        if (!_vb_sm.building_state.tasks_state.contains(base_id)) {
            // If there are no more task for the base table, finish processing it.
            auto remove_processing_base_id_mut = co_await _sys_ks.make_remove_view_building_processing_base_id_mutation(guard.write_timestamp());
            muts.push_back(std::move(remove_processing_base_id_mut));
            vbc_logger.info("Finished processing base table {}", base_id);
        }
    } else {
        // `currently_processed_base_table` is not selected.
        if (_vb_sm.building_state.tasks_state.empty()) {
            // No task left, nothing to do.
            vbc_logger.info("No base table to process.");
            co_return std::move(guard);
        }

        // Select new base table to process.
        // Firstly, look for base table only with `process_staging` tasks. This means that views were build
        // but some staging sstables appeared.
        // If there is no such base table, select any other base table.

        // Look for base table with only `process_staging` tasks.
        auto base_table_only_with_staging = [this] -> std::optional<table_id> {
            for (auto& [base_id, tasks]: _vb_sm.building_state.tasks_state) {
                auto no_building_tasks = std::ranges::all_of(tasks, [] (const auto& replica_tasks) {
                    return replica_tasks.second.view_tasks.empty();
                });

                if (no_building_tasks) {
                    return base_id;
                }
            }
            return std::nullopt;
        };

        auto new_base_table_to_process = base_table_only_with_staging();
        if (!new_base_table_to_process) {
            new_base_table_to_process = _vb_sm.building_state.tasks_state.begin()->first;
        }
        vbc_logger.info("Selected new base table to process: {}", *new_base_table_to_process);
        auto mut = co_await _sys_ks.make_view_building_processing_base_id_mutation(guard.write_timestamp(), *new_base_table_to_process);
        muts.push_back(std::move(mut));

        co_await mark_all_remaining_view_build_statuses_started(guard, *new_base_table_to_process, muts);
    }


    if (muts.empty()) {
        vbc_logger.debug("vb state is up-to-date, no mutations to commit");
        co_return std::move(guard);
    } else {
        co_await commit_mutations(std::move(guard), std::move(muts), "update view building coordinator state");
        co_return std::nullopt;
    }

}

static std::pair<sstring, sstring> table_id_to_name(replica::database& db, table_id id) {
    auto schema = db.find_schema(id);
    return {schema->ks_name(), schema->cf_name()};
}

future<> view_building_coordinator::mark_view_build_status_started(const service::group0_guard& guard, table_id view_id, utils::chunked_vector<mutation>& out) {
    std::vector<locator::host_id> hosts_to_add;

    if (_vb_sm.views_state.status_map.contains(view_id)) {
        for (auto& [id, _]: _topo_sm._topology.normal_nodes) {
            locator::host_id host_id{id.uuid()};
            if (!_vb_sm.views_state.status_map.at(view_id).contains(host_id)) {
                hosts_to_add.push_back(host_id);
            }
        }
    } else {
        // The view is not present in status map, so mark it started on all hosts.
        hosts_to_add = _topo_sm._topology.normal_nodes | std::views::keys | std::views::transform([] (const raft::server_id& id) {
            return locator::host_id{id.uuid()};
        }) | std::ranges::to<std::vector>();
    }

    auto view_name = table_id_to_name(_db, view_id);
    for (auto& host_id: hosts_to_add) {
        auto mut = co_await _sys_ks.make_view_build_status_mutation(guard.write_timestamp(), view_name, host_id, db::view::build_status::STARTED);
        out.push_back(std::move(mut));
        vbc_logger.debug("Marking view build status STARTED for view {}({}.{}) on host {}", view_id, view_name.first, view_name.second, host_id);
    }
}

future<> view_building_coordinator::mark_all_remaining_view_build_statuses_started(const service::group0_guard& guard, table_id base_id, utils::chunked_vector<mutation>& out) {
    if (!_vb_sm.views_state.views_per_base.contains(base_id)) {
        co_return;
    }

    for (auto view_id: _vb_sm.views_state.views_per_base.at(base_id)) {
        co_await mark_view_build_status_started(guard, view_id, out);
    }
}

future<> view_building_coordinator::update_views_statuses(const service::group0_guard& guard, table_id base_id, utils::chunked_vector<mutation>& out) {
    if (!_vb_sm.views_state.views_per_base.contains(base_id)) {
        co_return;
    }

    auto no_tasks_left_for_view = [this, &base_id] (table_id view_id) -> bool {
        if (!_vb_sm.building_state.tasks_state.contains(base_id)) {
            return true;
        }

        auto& base_tasks = _vb_sm.building_state.tasks_state.at(base_id);
        if (std::ranges::any_of(base_tasks | std::views::values, [] (const replica_tasks& replica_tasks) {
            return !replica_tasks.staging_tasks.empty();
        })) {
            // If there are any staging tasks left, wait with marking any view as built.
            return false;
        }

        return std::ranges::all_of(base_tasks | std::views::values, [&view_id] (const replica_tasks& replica_tasks) {
            return !replica_tasks.view_tasks.contains(view_id);
        });
    };


    for (auto view_id: _vb_sm.views_state.views_per_base.at(base_id)) {
        if (_vb_sm.views_state.status_map.contains(view_id) && std::ranges::all_of(_vb_sm.views_state.status_map.at(view_id), [&] (const auto& e) {
            return e.second == db::view::build_status::SUCCESS;
        })) {
            // If the view is already marked as built, skip it.
            continue;
        }
        auto view_name = table_id_to_name(_db, view_id);

        // Check if view is built
        if (no_tasks_left_for_view(view_id)) {
            // Mark view build statuses as SUCCESS.
            // `system.view_build_status_v2` is partitioned by (ks_name, view_name, host_id),
            // so we can just insert entries with SUCCESS state for all hosts, 
            // instead of updating exiting ones.
            for (auto& [id, _]: _topo_sm._topology.normal_nodes) {
                locator::host_id host_id{id.uuid()};
                auto status_mut = co_await _sys_ks.make_view_build_status_mutation(guard.write_timestamp(), view_name, host_id, db::view::build_status::SUCCESS);
                out.push_back(std::move(status_mut));
                vbc_logger.debug("Marking view build status SUCCESS for view {}({}.{}) on host {}", view_id, view_name.first, view_name.second, host_id);
            }
            vbc_logger.info("View {}.{}({}) is built.", view_name.first, view_name.second, view_id);
        } else {
            // Ensure view build status is marked as STARTED on all nodes.
            co_await mark_view_build_status_started(guard, view_id, out);
        }
    }
}

future<> view_building_coordinator::work_on_view_building(service::group0_guard guard) {
    if (!_vb_sm.building_state.currently_processed_base_table) {
        vbc_logger.debug("No base table is selected, nothing to do.");
    }

    // Acquire unique lock of `_finished_tasks` to ensure each replica has its own entry in it
    // and to select tasks for them.
    auto lock = co_await get_unique_lock(_mutex);
    for (auto& replica: get_replicas_with_tasks()) {
        if (_remote_work.contains(replica)) {
            if (!_remote_work[replica].available()) {
                vbc_logger.debug("Replica {} is still doing work", replica);
                continue;
            }

            auto remote_results_opt = co_await _remote_work[replica].get_future();
            _remote_work.erase(replica);
        }

        const bool ignore_gossiper = utils::get_local_injector().enter("view_building_coordinator_ignore_gossiper");
        if (!_gossiper.is_alive(replica.host) && !ignore_gossiper) {
            vbc_logger.debug("Replica {} is dead", replica);
            continue;
        }

        if (!_finished_tasks.contains(replica)) {
            _finished_tasks.insert({replica, {}});
        }

        if (auto todo_ids = select_tasks_for_replica(replica); !todo_ids.empty()) {
            start_remote_worker(replica, std::move(todo_ids));
        } else {
            vbc_logger.debug("Nothing to do for replica {}", replica);
        }
    }
}

std::set<locator::tablet_replica> view_building_coordinator::get_replicas_with_tasks() {
    std::set<locator::tablet_replica> replicas;
    for (auto& [replica, _]: _vb_sm.building_state.tasks_state[*_vb_sm.building_state.currently_processed_base_table]) {
        replicas.insert(replica);
    }
    for (auto& [replica, _]: _remote_work) {
        replicas.insert(replica);
    }
    return replicas;
}

// Returns list of tasks which can be started together.
// A task can be started if corresponding tablet is not in migration process.
// All returned tasks can be executed together, meaning that their type, base_id, table_id and replica are the same.
std::vector<utils::UUID> view_building_coordinator::select_tasks_for_replica(locator::tablet_replica replica) {
    // At this point `replica` should have only tasks in `IDLE` state.

    // Select only building tasks and return theirs ids
    auto filter_building_tasks = [] (const std::vector<view_building_task>& tasks) -> std::vector<utils::UUID> {
        return tasks | std::views::filter([] (const view_building_task& t) {
            return t.type == view_building_task::task_type::build_range && !t.aborted;
        }) | std::views::transform([] (const view_building_task& t) {
            return t.id;
        }) | std::ranges::to<std::vector>();
    };

    auto& base_tasks = _vb_sm.building_state.tasks_state[*_vb_sm.building_state.currently_processed_base_table];
    if (!base_tasks.contains(replica)) {
        // No tasks for this replica
        vbc_logger.debug("No task for replica: {}", replica);
        return {};
    }

    auto& tablet_map = _db.get_token_metadata().tablets().get_tablet_map(*_vb_sm.building_state.currently_processed_base_table);
    auto tasks_by_last_token = _vb_sm.building_state.collect_tasks_by_last_token(*_vb_sm.building_state.currently_processed_base_table, replica);

    // Remove completed tasks in `_finished_tasks` from `tasks_by_last_token`
    auto it = tasks_by_last_token.begin();
    while (it != tasks_by_last_token.end()) {
        auto task_it = it->second.begin();
        while (task_it != it->second.end()) {
            if (_finished_tasks.at(replica).contains(task_it->id)) {
                task_it = it->second.erase(task_it);
            } else {
                ++task_it;
            }
        }

        // Remove the entry from `tasks_by_last_token` if its vector is empty
        if (it->second.empty()) {
            it = tasks_by_last_token.erase(it);
        } else {
            ++it;
        }
    }

    for (auto& [token, tasks]: tasks_by_last_token) {
        auto tid = tablet_map.get_tablet_id(token);
        if (tablet_map.get_tablet_transition_info(tid)) {
            vbc_logger.debug("Tablet {} on replica {} is in transition.", tid, replica);
            continue;
        }

        auto building_tasks = filter_building_tasks(tasks);
        if (!building_tasks.empty()) {
            return building_tasks;
        } else {
            return tasks | std::views::filter([] (const view_building_task& t) {
                return !t.aborted;
            }) | std::views::transform([] (const view_building_task& t) {
                return t.id;
            }) | std::ranges::to<std::vector>();
        }
    }
    vbc_logger.debug("No tasks for replica {} can be started now.", replica);
    return {};
}

void view_building_coordinator::start_remote_worker(const locator::tablet_replica& replica, std::vector<utils::UUID> tasks) {
    vbc_logger.debug("Attaching to started tasks {} on replica {}", tasks, replica);
    shared_future<std::optional<std::vector<utils::UUID>>> work = work_on_tasks(replica, std::move(tasks));
    _remote_work.insert({replica, std::move(work)});
}

future<std::optional<std::vector<utils::UUID>>> view_building_coordinator::work_on_tasks(locator::tablet_replica replica, std::vector<utils::UUID> tasks) {
    constexpr auto backoff_duration = std::chrono::seconds(1);
    static thread_local logger::rate_limit rate_limit{backoff_duration};

    std::vector<utils::UUID> remote_results;
    bool rpc_failed = false;

    try {
        remote_results = co_await ser::view_rpc_verbs::send_work_on_view_building_tasks(&_messaging, replica.host, _as, _term, replica.shard, tasks);
    } catch (...) {
        vbc_logger.log(log_level::warn, rate_limit, "Work on tasks {} on replica {}, failed with error: {}",
                tasks, replica, std::current_exception());
        rpc_failed = true;
    }

    if (rpc_failed) {
        co_await seastar::sleep(backoff_duration);
        _vb_sm.event.broadcast();
        co_return std::nullopt;
    }

    // In `view_building_coordinator::work_on_view_building()` we made sure that,
    // each replica has its own entry in the `_finished_tasks`, so now we can just take a shared lock
    // and insert its of finished tasks to this replica bucket as there is at most one instance of this method for each replica.
    auto lock = co_await get_shared_lock(_mutex);
    _finished_tasks.at(replica).insert_range(remote_results);

    _vb_sm.event.broadcast();
    co_return remote_results;
}

future<> view_building_coordinator::stop() {
    co_await coroutine::parallel_for_each(std::move(_remote_work), [] (auto&& remote_work) -> future<> {
        co_await remote_work.second.get_future();
    });
}

void view_building_coordinator::generate_tablet_migration_updates(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, const locator::tablet_map& tmap, locator::global_tablet_id gid, const locator::tablet_transition_info& trinfo) {
    vbc_logger.debug("Generating updates for tablet migration for table {}", gid.table);
    
    if (!_vb_sm.building_state.tasks_state.contains(gid.table)) {
        vbc_logger.debug("No view building tasks for table {} - skipping tablet migration updates generation", gid.table);
        return;
    }

    auto& tinfo = tmap.get_tablet_info(gid.tablet);
    auto leaving_replica = locator::get_leaving_replica(tinfo, trinfo);

    if (!leaving_replica && !trinfo.pending_replica) {
        return;
    }

    auto last_token = tmap.get_last_token(gid.tablet);
    view_building_task_mutation_builder builder(guard.write_timestamp());

    auto create_task_copy_on_pending_replica = [&] (const view_building_task& task) {
        auto new_id = builder.new_id();
        builder.set_type(new_id, task.type)
                .set_aborted(new_id, false)
                .set_base_id(new_id, task.base_id)
                .set_last_token(new_id, task.last_token)
                .set_replica(new_id, *trinfo.pending_replica);
        if (task.view_id) {
            builder.set_view_id(new_id, *task.view_id);
        }
    };

    if (leaving_replica && trinfo.pending_replica) {
        // tablet migration
        auto tasks_to_migrate = _vb_sm.building_state.collect_tasks_by_last_token(gid.table, *leaving_replica)[last_token];
        for (auto& task: tasks_to_migrate) {
            create_task_copy_on_pending_replica(task);
            builder.del_task(task.id);
            vbc_logger.debug("Task {} was migrated from {} to {}.", task.id, task.replica, *trinfo.pending_replica);
        }        
        
    } else if (leaving_replica) {
        // RF decrease
        auto tasks_to_abort = _vb_sm.building_state.collect_tasks_by_last_token(gid.table, *leaving_replica)[last_token];
        for (auto& task: tasks_to_abort) {
            builder.del_task(task.id);
            vbc_logger.debug("Aborting task {} for abandoning replica {}", task.id, task.replica);
        } 

    } else if (trinfo.pending_replica) {
        // RF increase
        // Filter out staging tasks and group by remaining by view_id.
        // If a view has any unfinished task for this tablet id, create a task for each new replica.
        // TODO:
        // This might be optimized out depending on how data on the new replicas is built.
        // If all tablet replicas are built for the view, we're sure new view's replicas will also get correct data.
        std::unordered_map<::table_id, std::vector<view_building_task>> tasks_per_view;
        auto tasks_for_tablet = _vb_sm.building_state.collect_tasks_by_last_token(gid.table)[last_token];
        for (auto& t: tasks_for_tablet | std::views::filter([] (const view_building_task& t) {
            return t.type == view_building_task::task_type::build_range;
        })) {
            tasks_per_view[*t.view_id].push_back(t);
        }

        for (auto& [_, tasks_for_view]: tasks_per_view) {
            auto task = tasks_for_view.front();
            create_task_copy_on_pending_replica(task);
            vbc_logger.debug("Creating new task for pending replica {}", *trinfo.pending_replica);
        }
    }

    out.emplace_back(builder.build());
}

void view_building_coordinator::generate_tablet_resize_updates(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id, const locator::tablet_map& old_tmap, const locator::tablet_map& new_tmap) {
    vbc_logger.debug("Generating updates for tablet resize for table {}", table_id);
    if (!_vb_sm.building_state.tasks_state.contains(table_id)) {
        vbc_logger.debug("No view building tasks for table {} - skipping tablet migration updates generation", table_id);
        return;
    }

    if (old_tmap.tablet_count() == new_tmap.tablet_count()) {
        vbc_logger.debug("Tablet map size wasn't changed - skipping tablet migration updates generation");
        return;
    }
    bool is_split = old_tmap.tablet_count() < new_tmap.tablet_count();
    view_building_task_mutation_builder builder(guard.write_timestamp());

    auto create_task_copy = [&] (const view_building_task& task, dht::token last_token) -> utils::UUID {
        auto new_id = builder.new_id();
        builder.set_type(new_id, task.type)
                .set_aborted(new_id, false)
                .set_base_id(new_id, task.base_id)
                .set_last_token(new_id, last_token)
                .set_replica(new_id, task.replica);
        if (task.view_id) {
            builder.set_view_id(new_id, *task.view_id);
        }
        return new_id;
    };

    // Task with tablet id `n` is split into 2 tasks with tablet ids `2n` and `2n+1`
    auto split_task = [&] (const view_building_task& task) {
        auto new_tid = locator::tablet_id{old_tmap.get_tablet_id(task.last_token).id * 2};

        auto new_id = create_task_copy(task, new_tmap.get_last_token(new_tid));
        auto new_id2 = create_task_copy(task, new_tmap.get_last_token(locator::tablet_id{new_tid.id + 1}));
        builder.del_task(task.id);

        vbc_logger.debug("Task {} was split into task {} and task {}", task.id, new_id, new_id2);
    };

    // Task with tablet id `n` is updated to new task with tablet id `n/2` (integer division).
    // If task with tablet id `n/2` is already created (information is stored in `created_tablet_ids`), only old task is removed.
    auto merge_task = [&] (std::unordered_set<locator::tablet_id>& created_tablet_ids, const view_building_task& task) {
        builder.del_task(task.id);

        auto new_tid = locator::tablet_id(old_tmap.get_tablet_id(task.last_token).id / 2);
        if (!created_tablet_ids.contains(new_tid)) {
            created_tablet_ids.insert(new_tid);
            auto new_id = create_task_copy(task, new_tmap.get_last_token(new_tid));
            vbc_logger.debug("Task {} was merged into task {} ", task.id, new_id);
        } else {
            vbc_logger.debug("Task {} was removed during tablet merge. Task ending at token {} was already created.", task.id, task.last_token);
        }
    };

    auto resize_task_map = [&] (const task_map& task_map) {
        std::unordered_set<locator::tablet_id> new_tasks_tablet_ids;
        for (auto& [_, task]: task_map) {
            if (is_split) {
                split_task(task);
            } else {
                merge_task(new_tasks_tablet_ids, task);
            }
        }
    };

    for (auto& [_, replica_tasks]: _vb_sm.building_state.tasks_state.at(table_id)) {
        // Resize build_range tasks
        for (auto& [_, view_tasks]: replica_tasks.view_tasks) {
            resize_task_map(view_tasks);
        }
        // Migrate process_staging tasks
        resize_task_map(replica_tasks.staging_tasks);
    }

    out.emplace_back(builder.build());
}

void view_building_coordinator::abort_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id) {
    if (!_vb_sm.building_state.tasks_state.contains(table_id)) {
        return;
    }
    vbc_logger.debug("Generating abort mutations for tasks for table {}", table_id);

    view_building_task_mutation_builder builder(guard.write_timestamp());
    auto abort_task_map = [&] (const task_map& task_map) {
        for (auto& [id, _]: task_map) {
            vbc_logger.debug("Aborting task {}", id);
            builder.set_aborted(id, true);
        }
    };

    for (auto& [_, replica_tasks]: _vb_sm.building_state.tasks_state.at(table_id)) {
        for (auto& [_, building_task_map]: replica_tasks.view_tasks) {
            abort_task_map(building_task_map);
        }
        abort_task_map(replica_tasks.staging_tasks);
    }

    out.emplace_back(builder.build());
}

void view_building_coordinator::abort_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id, locator::tablet_replica replica, dht::token last_token) {
    return abort_view_building_tasks(_vb_sm, out, guard.write_timestamp(), table_id, replica, last_token);
}

void abort_view_building_tasks(const view_building_state_machine& vb_sm,
        utils::chunked_vector<canonical_mutation>& out, api::timestamp_type write_timestamp, table_id table_id, const locator::tablet_replica& replica, dht::token last_token) {
    if (!vb_sm.building_state.tasks_state.contains(table_id) || !vb_sm.building_state.tasks_state.at(table_id).contains(replica)) {
        return;
    }
    vbc_logger.debug("Generating abort mutations for tasks for table {} on replica {} and last token {}", table_id, replica, last_token);

    view_building_task_mutation_builder builder(write_timestamp);
    auto abort_task_map = [&] (const task_map& task_map) {
        for (auto& [id, task]: task_map) {
            if (task.last_token == last_token) {
                vbc_logger.debug("Aborting task {}", id);
                builder.set_aborted(id, true);
            }
        }
    };

    auto& replica_tasks = vb_sm.building_state.tasks_state.at(table_id).at(replica);
    for (auto& [_, building_task_map]: replica_tasks.view_tasks) {
        abort_task_map(building_task_map);
    }
    abort_task_map(replica_tasks.staging_tasks);

    out.emplace_back(builder.build());
}

static void rollback_task_map(view_building_task_mutation_builder& builder, const task_map& task_map) {
    for (auto& [id, task]: task_map) {
        if (task.aborted) {
            auto new_id = builder.new_id();
            builder.set_type(new_id, task.type)
                .set_aborted(new_id, false)
                .set_base_id(new_id, task.base_id)
                .set_last_token(new_id, task.last_token)
                .set_replica(new_id, task.replica);
            if (task.view_id) {
                builder.set_view_id(new_id, *task.view_id);
            }
            builder.del_task(task.id);
            vbc_logger.debug("Aborted task {} was recreated with new id {}", task.id, new_id);
        }
    }
}

void view_building_coordinator::rollback_aborted_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id) {
    if (!_vb_sm.building_state.tasks_state.contains(table_id)) {
        return;
    }

    view_building_task_mutation_builder builder(guard.write_timestamp());
    auto& base_tasks = _vb_sm.building_state.tasks_state.at(table_id);
    for (auto& [_, replica_tasks]: base_tasks) {
        for (auto& [_, building_task_map]: replica_tasks.view_tasks) {
            rollback_task_map(builder, building_task_map);
        }
        rollback_task_map(builder, replica_tasks.staging_tasks);
    }

    out.emplace_back(builder.build());
}

void view_building_coordinator::rollback_aborted_tasks(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, table_id table_id, locator::tablet_replica replica, dht::token last_token) {
    if (!_vb_sm.building_state.tasks_state.contains(table_id) || !_vb_sm.building_state.tasks_state.at(table_id).contains(replica)) {
        return;
    }

    view_building_task_mutation_builder builder(guard.write_timestamp());
    auto& replica_tasks = _vb_sm.building_state.tasks_state.at(table_id).at(replica);
    for (auto& [_, building_task_map]: replica_tasks.view_tasks) {
        rollback_task_map(builder, building_task_map);
    }
    rollback_task_map(builder, replica_tasks.staging_tasks);

    out.emplace_back(builder.build());
}

future<> view_building_coordinator::mark_view_build_statuses_on_node_join(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, locator::host_id host_id) {
    // View builder coordinator marks statuses (STARTED/SUCCESS) on all nodes at once,
    // so copy the status to the new node.
    // Ignore views which are not started yet.
    vbc_logger.debug("Marking view build statuses for joined node {}", host_id);
    for (const auto& [view_id, statuses]: _vb_sm.views_state.status_map) {
        if (!statuses.contains(host_id)) {
            auto view_name = table_id_to_name(_db, view_id);
            auto mut = co_await _sys_ks.make_view_build_status_mutation(guard.write_timestamp(), view_name, host_id, statuses.begin()->second);
            out.emplace_back(std::move(mut));
            vbc_logger.debug("Marking view {} on node {} as: {}", view_name, host_id, db::view::build_status_to_sstring(statuses.begin()->second));
        }
    }
}

future<> view_building_coordinator::remove_view_build_statuses_on_left_node(utils::chunked_vector<canonical_mutation>& out, const service::group0_guard& guard, locator::host_id host_id) {
    vbc_logger.debug("Removing view build statuses for leaving node {}", host_id);
    for (const auto& [view_id, statuses]: _vb_sm.views_state.status_map) {
        if (statuses.contains(host_id)) {
            auto view_name = table_id_to_name(_db, view_id);
            auto mut = co_await _sys_ks.make_remove_view_build_status_on_host_mutation(guard.write_timestamp(), view_name, host_id);
            out.emplace_back(std::move(mut));
            vbc_logger.debug("Removed view build status for view {} on node {}", view_name, host_id);
        }
    }
}

}

}
