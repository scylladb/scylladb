/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tasks/task_handler.hh"
#include "utils/overloaded_functor.hh"

#include <queue>

namespace tasks {

using task_status_variant = std::variant<tasks::task_manager::foreign_task_ptr, tasks::task_manager::task::task_essentials>;

static future<task_status> get_task_status(task_manager::task_ptr task) {
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
        .children = co_await task->get_children().map_each_task<task_id>(
            [] (const task_manager::foreign_task_ptr& task) { return task->id(); },
            [] (const task_manager::task::task_essentials& task) { return task.task_status.id; })
    };
    co_return status;
}

static future<task_status> get_foreign_task_status(const task_manager::foreign_task_ptr& task) {
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
    std::queue<task_status_variant> q;
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
        co_await std::visit(overloaded_functor {
            [&] (const tasks::task_manager::foreign_task_ptr& task) -> future<> {
                res.push_back(co_await get_foreign_task_status(task));
                co_await task->get_children().for_each_task([&q] (const tasks::task_manager::foreign_task_ptr& child) -> future<> {
                    q.push(co_await child.copy());
                }, [&] (const tasks::task_manager::task::task_essentials& child) {
                    q.push(child);
                    return make_ready_future();
                });
            },
            [&] (const tasks::task_manager::task::task_essentials& task) -> future<> {
                auto status = task_status{
                    .task_id = task.task_status.id,
                    .type = task.type,
                    .scope = task.task_status.scope,
                    .state = task.task_status.state,
                    .is_abortable = task.abortable,
                    .start_time = task.task_status.start_time,
                    .end_time = task.task_status.end_time,
                    .error = task.task_status.error,
                    .parent_id = task.parent_id,
                    .sequence_number = task.task_status.sequence_number,
                    .shard = task.task_status.shard,
                    .keyspace = task.task_status.keyspace,
                    .table = task.task_status.table,
                    .entity = task.task_status.entity,
                    .progress_units = task.task_status.progress_units,
                    .progress = task.task_progress,
                    .children = boost::copy_range<std::vector<task_id>>(task.failed_children | boost::adaptors::transformed([] (auto& child) {
                        return child.task_status.id;
                    }))
                };
                res.push_back(status);

                for (auto& child: task.failed_children) {
                    q.push(child);
                }
                return make_ready_future();
            }
        }, current);
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
                task->abort();
            },
            [id] (task_manager::virtual_task_ptr task) -> future<> {
                throw task_manager::task_not_found(id);    // API does not support virtual tasks yet.
            }
        }, task_v);
    });
}

}
