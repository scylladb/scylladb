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
#include "service/view_building_coordinator.hh"
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
#include "service/view_building_state.hh"
#include "utils/assert.hh"
#include "idl/view.dist.hh"

static logging::logger vbc_logger("view_building_coordinator");

namespace service {

namespace view_building {

view_building_coordinator::view_building_coordinator(replica::database& db, raft::server& raft, raft_group0& group0,
            db::system_keyspace& sys_ks, netw::messaging_service& ms,
            view_building_state_machine& vb_sm, const topology_state_machine& topo_sm,
            raft::term_t term, abort_source& as)
    : _db(db)
    , _raft(raft)
    , _group0(group0)
    , _sys_ks(sys_ks)
    , _messaging(ms)
    , _vb_sm(vb_sm)
    , _topo_sm(topo_sm)
    , _term(term)
    , _as(as) {}

future<group0_guard> view_building_coordinator::start_operation() {
    auto guard = co_await _group0.client().start_operation(_as, raft_timeout{});
    vbc_logger.debug("starting new operation");
    if (_term != _raft.get_current_term()) {
        throw term_changed_error{};
    }
    co_return std::move(guard);
}

future<> view_building_coordinator::await_event() {
    _as.check();
    vbc_logger.debug("waiting for view building state machine event");
    co_await _vb_sm.event.when();
    vbc_logger.debug("event received");
}

future<> view_building_coordinator::commit_mutations(group0_guard guard, std::vector<mutation> mutations, std::string_view description) {
    std::vector<canonical_mutation> cmuts = {mutations.begin(), mutations.end()};
    auto cmd = _group0.client().prepare_command(write_mutations{
        .mutations{std::move(cmuts)}
    }, guard, description);
    co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);
}

void view_building_coordinator::handle_coordinator_error(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (group0_concurrent_modification&) {
        vbc_logger.info("view building coordinator got group0_concurrent_modification");
    } catch (abort_requested_exception&) {
        vbc_logger.debug("view building coordinator got abort_requested_exception");
    } catch (raft::request_aborted&) {
        vbc_logger.debug("view building coordinator got raft::request_aborted");
    } catch (term_changed_error&) {
        vbc_logger.debug("view building coordinator notices term change {} -> {}", _term, _raft.get_current_term());
    } catch (raft::commit_status_unknown&) {
        vbc_logger.warn("view building coordinator got raft::commit_status_unknown");
    } catch (...) {
        vbc_logger.error("view building coordinator got error: {}", std::current_exception());
    }
}

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _vb_sm.event.broadcast();
    });

    while (!_as.abort_requested()) {
        bool sleep = false;
        try {
            auto guard_opt = co_await update_state(co_await start_operation());
            if (!guard_opt) {
                // If `update_state()` returned guard, this means it committed some mutations
                // and the state was changed.
                vbc_logger.debug("view building coordinator state was changed, do next iteration without waiting for event");
                continue;
            }

            auto started_new_work = co_await work_on_view_building(std::move(*guard_opt));
            if (started_new_work) {
                // If any tasks were started, do another iteration, so the coordinator can attach itself to the tasks (via RPC)
                vbc_logger.debug("view building coordinator started new tasks, do next iteration without waiting for event");
                continue;
            }
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
}

future<std::optional<group0_guard>> view_building_coordinator::update_state(group0_guard guard) {
    vbc_logger.debug("Currently processed base table: {}", _vb_sm.building_state.currently_processed_base_table);

    std::vector<mutation> muts;
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

future<> view_building_coordinator::mark_view_build_status_started(const group0_guard& guard, table_id view_id, std::vector<mutation>& out) {
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

future<> view_building_coordinator::mark_all_remaining_view_build_statuses_started(const group0_guard& guard, table_id base_id, std::vector<mutation>& out) {
    if (!_vb_sm.views_state.views_per_base.contains(base_id)) {
        co_return;
    }

    for (auto view_id: _vb_sm.views_state.views_per_base.at(base_id)) {
        co_await mark_view_build_status_started(guard, view_id, out);
    }
}

future<> view_building_coordinator::update_views_statuses(const group0_guard& guard, table_id base_id, std::vector<mutation>& out) {
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

future<bool> view_building_coordinator::work_on_view_building(group0_guard guard) {
    if (!_vb_sm.building_state.currently_processed_base_table) {
        vbc_logger.debug("No base table is selected, nothing to do.");
        co_return false;
    }

    std::vector<mutation> muts;
    for (auto& replica: get_replicas_with_tasks()) {
        // Check whether the coordinator already waits for the remote work on the replica to be finished.
        // If so: check if the work is done and and remove the shared_future, skip this replica otherwise.
        if (_remote_work.contains(replica)) {
            if (_remote_work[replica].available()) {
                co_await _remote_work[replica].get_future();
                _remote_work.erase(replica);
            } else {
                vbc_logger.debug("Replica {} is still doing work", replica);
                continue;
            }
        }
        
        if (auto already_started_ids = get_started_tasks(replica); !already_started_ids.empty()) {
            // If the replica has any task in `STARTED` state, attach the coordinator to the work.
            attach_to_started_tasks(replica, std::move(already_started_ids));
        } else if (auto todo_ids = select_tasks_for_replica(replica); !todo_ids.empty()) {
            // If the replica has no started tasks and there are tasks to do, mark them as started.
            // The coordinator will attach itself to the work in next iteration.
            auto new_mutations = co_await start_tasks(guard, std::move(todo_ids));
            muts.insert(muts.end(), std::make_move_iterator(new_mutations.begin()), std::make_move_iterator(new_mutations.end()));
        } else {
            vbc_logger.debug("Nothing to do for replica {}", replica);
        }
    }

    if (!muts.empty()) {
        co_await commit_mutations(std::move(guard), std::move(muts), "start view building tasks");
        co_return true;
    }
    co_return false;
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

// Returns all tasks for `_vb_sm.building_state.currently_processed_base_table` and `replica` with `STARTED` state.
std::vector<utils::UUID> view_building_coordinator::get_started_tasks(locator::tablet_replica replica) {
    auto& base_tasks = _vb_sm.building_state.tasks_state[*_vb_sm.building_state.currently_processed_base_table];
    if (!base_tasks.contains(replica)) {
        vbc_logger.debug("No started tasks for replica: {}", replica);
        // No tasks for this replica
        return {};
    }

    std::vector<view_building_task> tasks;
    auto& replica_tasks = base_tasks[replica];
    for (auto& [_, view_tasks]: replica_tasks.view_tasks) {
        for (auto& [_, task]: view_tasks) {
            if (task.state == view_building_task::task_state::started) {
                tasks.push_back(task);
            }
        }
    }
    for (auto& [_, task]: replica_tasks.staging_tasks) {
        if (task.state == view_building_task::task_state::started) {
            tasks.push_back(task);
        }
    }

    // All collected tasks should have the same: type, base_id and last_token,
    // so they can be executed in the same view_building_worker::batch.
#ifdef SEASTAR_DEBUG
    if (!tasks.empty()) {
        auto& task = tasks.front();
        for (auto& t: tasks) {
            SCYLLA_ASSERT(task.type == t.type);
            SCYLLA_ASSERT(task.base_id == t.base_id);
            SCYLLA_ASSERT(task.last_token == t.last_token);
        }
    }
#endif

    return tasks | std::views::transform([] (const view_building_task& t) {
        return t.id;
    }) | std::ranges::to<std::vector>();
}

static std::map<dht::token, std::vector<view_building_task>> collect_tasks_by_last_token(const replica_tasks& replica_tasks) {
    std::map<dht::token, std::vector<view_building_task>> tasks;
    for (auto& [_, view_tasks]: replica_tasks.view_tasks) {
        for (auto& [_, task]: view_tasks) {
            tasks[task.last_token].push_back(task);
        }
    }
    for (auto& [_, task]: replica_tasks.staging_tasks) {
        tasks[task.last_token].push_back(task);
    }
    return tasks;
}

// Returns list of tasks which can be started together.
// A task can be started if corresponding tablet is not in migration process.
// All returned tasks can be executed together, meaning that their type, base_id, table_id and replica are the same.
std::vector<utils::UUID> view_building_coordinator::select_tasks_for_replica(locator::tablet_replica replica) {
    // At this point `replica` should have only tasks in `IDLE` state.

    // Select only building tasks and return theirs ids
    auto filter_building_tasks = [] (const std::vector<view_building_task>& tasks) -> std::vector<utils::UUID> {
        return tasks | std::views::filter([] (const view_building_task& t) {
            return t.type == view_building_task::task_type::build_range;
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
    for (auto& [token, tasks]: collect_tasks_by_last_token(base_tasks[replica])) {
        auto tid = tablet_map.get_tablet_id(token);
        if (tablet_map.get_tablet_transition_info(tid)) {
            vbc_logger.debug("Tablet {} on replica {} is in transition.", tid, replica);
            continue;
        }

        auto building_tasks = filter_building_tasks(tasks);
        if (!building_tasks.empty()) {
            return building_tasks;
        } else {
            return tasks | std::views::transform([] (const view_building_task& t) {
                return t.id;
            }) | std::ranges::to<std::vector>();
        }
    }
    vbc_logger.debug("No tasks for replica {} can be started now.", replica);
    return {};
}

future<std::vector<mutation>> view_building_coordinator::start_tasks(const group0_guard& guard, std::vector<utils::UUID> tasks) {
    vbc_logger.info("Starting tasks {}", tasks);

    std::vector<mutation> muts;
    for (auto& t: tasks) {
        auto mut = co_await _sys_ks.make_update_view_building_task_state_mutation(guard.write_timestamp(), t, service::view_building::view_building_task::task_state::started);
        muts.push_back(std::move(mut));
    }
    co_return muts;
}

void view_building_coordinator::attach_to_started_tasks(const locator::tablet_replica& replica, std::vector<utils::UUID> tasks) {
    vbc_logger.debug("Attaching to started tasks {} on replica {}", tasks, replica);
    shared_future<> work = work_on_tasks(replica, std::move(tasks));
    _remote_work.insert({replica, std::move(work)});
}

future<> view_building_coordinator::work_on_tasks(locator::tablet_replica replica, std::vector<utils::UUID> tasks) {
    std::vector<view_task_result> results;
    try {
        results = co_await ser::view_rpc_verbs::send_work_on_view_building_tasks(&_messaging, replica.host, _as, tasks);
    } catch (...) {
        vbc_logger.warn("Work on tasks {} on replica {}, failed with error: {}", tasks, replica, std::current_exception());
        _vb_sm.event.broadcast();
        co_return;
    }

    if (tasks.size() != results.size()) {
        on_internal_error(vbc_logger, fmt::format("Number of tasks ({}) and results ({}) do not match for replica {}", tasks.size(), results.size(), replica));
    }
    try {
        co_await update_state_after_work_is_done(replica, std::move(tasks), std::move(results));
    } catch (...) {
        handle_coordinator_error(std::current_exception());
    }
    _vb_sm.event.broadcast();
}

// Mark finished task as done (remove them from the table).
// Retry failed tasks if possible (if failed tasks wasn't aborted).
future<> view_building_coordinator::update_state_after_work_is_done(const locator::tablet_replica& replica, std::vector<utils::UUID> tasks, std::vector<view_task_result> results) {
    auto guard = co_await start_operation();
    vbc_logger.debug("Got results for tasks {}: {}", tasks, results);
    std::vector<mutation> muts;
    for (size_t i = 0; i < tasks.size(); ++i) {
        vbc_logger.info("Task {} was finished with result: {}", tasks[i], results[i]);
        auto delete_mut = co_await _sys_ks.make_remove_view_building_task_mutation(guard.write_timestamp(), tasks[i]);
        muts.push_back(std::move(delete_mut));

        if (results[i].status == view_task_result::command_status::fail && _vb_sm.building_state.currently_processed_base_table) {
            auto task_opt = _vb_sm.building_state.get_task(*_vb_sm.building_state.currently_processed_base_table, replica, tasks[i]);
            if (task_opt) {
                auto task = task_opt->get();
                task.id = utils::UUID_gen::get_time_UUID();
                task.state = view_building_task::task_state::idle;
                auto retry_mut = co_await _sys_ks.make_view_building_task_mutation(guard.write_timestamp(), task);
                muts.push_back(std::move(retry_mut));
                vbc_logger.debug("Task {} will be retried with new id: {}", tasks[i], task.id);
            }
            // If above if statement is false, this means the task was aborted and we should not retry it.
        }
    }

    if (!muts.empty()) {
        co_await commit_mutations(std::move(guard), std::move(muts), "update vb state after task are finished");
    }
}

future<> view_building_coordinator::stop() {
    co_await coroutine::parallel_for_each(std::move(_remote_work), [] (auto&& remote_work) -> future<> {
        co_await remote_work.second.get_future();
    });
}

}

}
