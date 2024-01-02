/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tasks/task_handler.hh"
#include "utils/overloaded_functor.hh"

namespace tasks {

static future<task_status> get_task_status(task_manager::task_ptr task) {
    auto& tm = task->get_module()->get_task_manager();
    std::vector<task_identity> children_ids{task->get_children().size()};
    boost::transform(task->get_children(), children_ids.begin(), [&tm] (const auto& child) {
        return task_identity{
            .node = tm.get_broadcast_address(),
            .task_id = child->id()
        };
    });
    auto local_task_status = task->get_status();
    auto status = task_status{
        .task_id = local_task_status.id,
        .type = task->type(),
        .kind = task_kind::node,
        .scope = local_task_status.scope,
        .state = local_task_status.state,
        .is_abortable = task->is_abortable(),
        .start_time = local_task_status.start_time,
        .end_time = local_task_status.end_time,
        .error = local_task_status.error,
        .parent_id = task->get_parent_id(),
        .sequence_number = local_task_status.sequence_number,
        .shard = local_task_status.shard,
        .keyspace = local_task_status.keyspace,
        .table = local_task_status.table,
        .entity = local_task_status.entity,
        .progress_units = local_task_status.progress_units,
        .progress = co_await task->get_progress(),
        .children = std::move(children_ids)
    };
    co_return status;
}

static future<task_status> get_foreign_task_status(task_manager::foreign_task_ptr& task) {
    co_return co_await smp::submit_to(task.get_owner_shard(), [t = co_await task.copy()] () mutable {
        return get_task_status(t.release());
    });
}

template<typename T>
static T get_virtual_task_info(task_id id, std::optional<T> info) {
    // A service associated with the virtual task may have stopped tracking the operation
    // before its info got retrieved.
    if (!info) {
        throw task_manager::task_not_found(id);
    }
    return *info;
}

// Prolongs task life to make sure all its children will be accessible.
struct status_helper {
    task_status status;
    task_manager::foreign_task_ptr task;
};

future<status_helper> task_handler::get_status_helper() {
    return task_manager::invoke_on_task(_tm.container(), _id, std::function(
            [id = _id] (task_manager::task_variant task_v) -> future<status_helper> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) -> future<status_helper> {
                if (task->is_complete()) {
                    task->unregister_task();
                }
                co_return status_helper{
                    .status = co_await get_task_status(task),
                    .task = task
                };
            },
            [id] (task_manager::virtual_task_ptr task) -> future<status_helper> {
                auto status = co_await task->get_status(id);
                co_return status_helper{
                    .status = get_virtual_task_info(id, status),
                    .task = nullptr
                };
            }
        }, task_v);
    }));
}

future<task_status> task_handler::get_status() {
    auto s = co_await get_status_helper();
    co_return s.status;
}

future<task_status> task_handler::wait_for_task() {
    return task_manager::invoke_on_task(_tm.container(), _id, std::function([id = _id] (task_manager::task_variant task_v) -> future<task_status> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) {
                return task->done().then_wrapped([task] (auto f) {
                    task->unregister_task();
                    // done() is called only because we want the task to be complete before getting its status.
                    // The future should be ignored here as the result does not matter.
                    f.ignore_ready_future();
                    return get_task_status(std::move(task));
                });
            },
            [id] (task_manager::virtual_task_ptr task) -> future<task_status> {
                auto status = co_await task->get_status(id);
                co_return get_virtual_task_info(id, status);
            }
        }, task_v);
    }));
}

future<utils::chunked_vector<task_status>> task_handler::get_status_recursively(bool local) {
    auto sh = co_await get_status_helper();

    std::queue<task_manager::foreign_task_ptr> q;
    utils::chunked_vector<task_status> res;
    if (sh.task) {  // task
        q.push(co_await sh.task.copy());   // Task cannot be moved since we need it to be alive during whole loop execution.
    } else {        // virtual task
        res.push_back(sh.status);
        for (auto ident: sh.status.children) {
            if (ident.node != _tm.get_broadcast_address()) {
                // FIXME: add non-local version
                continue;
            }

            try {
                auto child = co_await _tm.lookup_task_on_all_shards(_tm.container(), ident.task_id);
                q.push(std::move(child));
            } catch (const task_manager::task_not_found& e) {
                continue; // Virtual task's children may get unregistered.
            }
        }
    }

    // Push children's statuses in BFS order.
    while (!q.empty()) {
        auto& current = q.front();
        res.push_back(co_await get_foreign_task_status(current));
        for (auto& child: current->get_children()) {
            q.push(co_await child.copy());
        }
        q.pop();
    }

    co_return res;
}

future<> task_handler::abort() {
    co_await task_manager::invoke_on_task(_tm.container(), _id, [id = _id] (task_manager::task_variant task_v) -> future<> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) -> future<> {
                if (!task->is_abortable()) {
                    co_await coroutine::return_exception(std::runtime_error("Requested task cannot be aborted"));
                }
                co_await task->abort();
            },
            [id] (task_manager::virtual_task_ptr task) -> future<> {
                return task->abort(id);
            }
        }, task_v);
    });
}

}
