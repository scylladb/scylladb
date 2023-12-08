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
    std::vector<task_id> children_ids{task->get_children().size()};
    boost::transform(task->get_children(), children_ids.begin(), [] (const auto& child) {
        return child->id();
    });
    auto local_task_status = task->get_status();
    auto status = task_status{
        .task_id = local_task_status.id,
        .type = task->type(),
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

future<task_status> task_handler::get_status() {
    auto task = co_await task_manager::invoke_on_task(_tm.container(), _id, std::function([id = _id] (task_manager::task_variant task_v) -> future<task_manager::foreign_task_ptr> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) -> future<task_manager::foreign_task_ptr> {
                if (task->is_complete()) {
                    task->unregister_task();
                }
                co_return std::move(task);
            },
            [id] (task_manager::virtual_task_ptr task) -> future<task_manager::foreign_task_ptr> {
                throw task_manager::task_not_found(id);    // API does not support virtual tasks yet.
            }
        }, task_v);
    }));
    co_return co_await get_foreign_task_status(task);
}

future<task_status> task_handler::wait_for_task() {
    auto task = co_await task_manager::invoke_on_task(_tm.container(), _id, std::function([id = _id] (task_manager::task_variant task_v) -> future<task_manager::foreign_task_ptr> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) {
                return task->done().then_wrapped([task] (auto f) {
                    task->unregister_task();
                    // done() is called only because we want the task to be complete before getting its status.
                    // The future should be ignored here as the result does not matter.
                    f.ignore_ready_future();
                    return make_foreign(task);
                });
            },
            [id] (task_manager::virtual_task_ptr task) -> future<task_manager::foreign_task_ptr> {
                throw task_manager::task_not_found(id);    // API does not support virtual tasks yet.
            }
        }, task_v);
    }));
    co_return co_await get_foreign_task_status(task);
}

future<utils::chunked_vector<task_status>> task_handler::get_status_recursively(bool local) {
    std::queue<task_manager::foreign_task_ptr> q;
    utils::chunked_vector<task_status> res;

    auto task = co_await task_manager::invoke_on_task(_tm.container(), _id, std::function([id = _id] (task_manager::task_variant task_v) -> future<task_manager::foreign_task_ptr> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) -> future<task_manager::foreign_task_ptr> {
                if (task->is_complete()) {
                    task->unregister_task();
                }
                co_return task;
            },
            [id] (task_manager::virtual_task_ptr task) -> future<task_manager::foreign_task_ptr> {
                throw task_manager::task_not_found(id);    // API does not support virtual tasks yet.
            }
        }, task_v);
    }));

    // Push children's statuses in BFS order.
    q.push(co_await task.copy());   // Task cannot be moved since we need it to be alive during whole loop execution.
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
                throw task_manager::task_not_found(id);    // API does not support virtual tasks yet.
            }
        }, task_v);
    });
}

}
