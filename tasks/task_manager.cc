/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "task_manager.hh"
#include "test_module.hh"

namespace tasks {

logging::logger tmlogger("task_manager");

task_manager::module_ptr task_manager::make_module(std::string name) {
    auto m = seastar::make_shared<task_manager::module>(*this, name);
    register_module(std::move(name), m);
    return m;
}

task_manager::module_ptr task_manager::find_module(std::string module_name) {
    auto it = _modules.find(module_name);
    if (it == _modules.end()) {
        throw std::runtime_error(format("module {} not found", module_name));
    }
    return it->second;
}

future<> task_manager::stop() noexcept {
    if (!_modules.empty()) {
        on_internal_error(tmlogger, "Tried to stop task manager while some modules were not unregistered");
    }
    return make_ready_future<>();
}

future<task_manager::foreign_task_ptr> task_manager::lookup_task_on_all_shards(sharded<task_manager>& tm, task_id tid) {
    return task_manager::invoke_on_task(tm, tid, std::function([] (task_ptr task) {
        return make_ready_future<task_manager::foreign_task_ptr>(make_foreign(task));
    }));
}

future<> task_manager::invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<> (task_manager::task_ptr)> func) {
    co_await task_manager::invoke_on_task(tm, id, std::function([func = std::move(func)] (task_manager::task_ptr task) -> future<bool> {
        co_await func(task);
        co_return true;
    }));
}

template<typename T>
future<T> task_manager::invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<T> (task_manager::task_ptr)> func) {
    std::optional<T> res;
    co_await coroutine::parallel_for_each(boost::irange(0u, smp::count), [&tm, id, &res, &func] (unsigned shard) -> future<> {
        auto local_res = co_await tm.invoke_on(shard, [id, func] (const task_manager& local_tm) -> future<std::optional<T>> {
            const auto& all_tasks = local_tm.get_all_tasks();
            if (auto it = all_tasks.find(id); it != all_tasks.end()) {
                co_return co_await func(it->second);
            }
            co_return std::nullopt;
        });
        if (!res) {
            res = std::move(local_res);
        } else if (local_res) {
            on_internal_error(tmlogger, format("task_id {} found on more than one shard", id));
        }
    });
    if (!res) {
        co_await coroutine::return_exception(task_manager::task_not_found(id));
    }
    co_return std::move(res.value());
}

void task_manager::register_module(std::string name, module_ptr module) {
    _modules[name] = module;
    tmlogger.info("Registered module {}", name);
}

void task_manager::unregister_module(std::string name) noexcept {
    _modules.erase(name);
    tmlogger.info("Unregistered module {}", name);
}

void task_manager::register_task(task_ptr task) {
    _all_tasks[task->id()] = task;
}

void task_manager::unregister_task(task_id id) noexcept {
    _all_tasks.erase(id);
}

}
