/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/timeout_clock.hh"
#include "tasks/task_handler.hh"
#include "tasks/virtual_task_hint.hh"
#include "utils/overloaded_functor.hh"

#include <seastar/core/with_timeout.hh>

#include <queue>

namespace tasks {

using task_status_variant = std::variant<tasks::task_manager::foreign_task_ptr, tasks::task_manager::task::task_essentials>;

static future<task_status> get_task_status(task_manager::task_ptr task) {
    auto host_id = task->get_module()->get_task_manager().get_host_id();
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
        .children = co_await task->get_children().map_each_task<task_identity>(
            [host_id] (const task_manager::foreign_task_ptr& task) {
                // There is no race because id does not change for the whole task lifetime.
                return task_identity{
                    .host_id = host_id,
                    .task_id = task->id()
                };
            },
            [host_id] (const task_manager::task::task_essentials& task) {
                return task_identity{
                    .host_id = host_id,
                    .task_id = task.task_status.id
                };
            })
    };
    co_return status;
}

static future<task_status> get_foreign_task_status(const task_manager::foreign_task_ptr& task) {
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
            [id = _id] (task_manager::task_variant task_v, tasks::virtual_task_hint hint) -> future<status_helper> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) -> future<status_helper> {
                co_return status_helper{
                    .status = co_await get_task_status(task),
                    .task = task
                };
            },
            [id, hint = std::move(hint)] (task_manager::virtual_task_ptr task) -> future<status_helper> {
                auto id_ = id;
                auto status = co_await task->get_status(id_, std::move(hint));
                co_return status_helper{
                    .status = get_virtual_task_info(id_, status),
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

future<task_status> task_handler::wait_for_task(std::optional<std::chrono::seconds> timeout) {
    auto wait = task_manager::invoke_on_task(_tm.container(), _id, std::function([id = _id] (task_manager::task_variant task_v, tasks::virtual_task_hint hint) -> future<task_status> {
        return std::visit(overloaded_functor{
            [] (task_manager::task_ptr task) {
                return task->done().then_wrapped([task] (auto f) {
                    // done() is called only because we want the task to be complete before getting its status.
                    // The future should be ignored here as the result does not matter.
                    f.ignore_ready_future();
                    return get_task_status(std::move(task));
                });
            },
            [id, hint = std::move(hint)] (task_manager::virtual_task_ptr task) -> future<task_status> {
                auto id_ = id;
                auto status = co_await task->wait(id_, std::move(hint));
                co_return get_virtual_task_info(id_, status);
            }
        }, task_v);
    }));
    return with_timeout(timeout ? db::timeout_clock::now() + timeout.value() : db::no_timeout, std::move(wait));
}

future<utils::chunked_vector<task_status>> task_handler::get_status_recursively(bool local) {
    auto sh = co_await get_status_helper();

    std::queue<task_status_variant> q;
    utils::chunked_vector<task_status> res;
    if (sh.task) {  // task
        q.push(co_await sh.task.copy());   // Task cannot be moved since we need it to be alive during whole loop execution.
    } else {        // virtual task
        res.push_back(sh.status);
        for (auto ident: sh.status.children) {
            if (ident.host_id != _tm.get_host_id()) {
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
                    .kind = task_kind::node,
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
                    .children = task.failed_children | std::views::transform([&tm = _tm] (auto& child) {
                        return task_identity{
                            .host_id = tm.get_host_id(),
                            .task_id = child.task_status.id
                        };
                    }) | std::ranges::to<std::vector<task_identity>>()
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
    co_await task_manager::invoke_on_task(_tm.container(), _id, [id = _id] (task_manager::task_variant task_v, tasks::virtual_task_hint hint) -> future<> {
        return std::visit(overloaded_functor{
            [id] (task_manager::task_ptr task) -> future<> {
                if (!task->is_abortable()) {
                    co_await coroutine::return_exception(task_not_abortable(id));
                }
                task->abort();
            },
            [id, hint = std::move(hint)] (task_manager::virtual_task_ptr task) -> future<> {
                auto id_ = id;
                auto hint_ = std::move(hint);
                if (!co_await task->is_abortable(hint_)) {
                    co_await coroutine::return_exception(task_not_abortable(id_));
                }
                co_await task->abort(id_, std::move(hint_));
            }
        }, task_v);
    });
}

}
