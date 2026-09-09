/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

#include <unordered_map>

#include <seastar/core/shared_ptr.hh>

#include "task_manager.hh"

namespace tasks {

// The state a test steers a task through: the task's action waits on the
// promise until the test resolves it.
struct task_finalization {
    promise<> finish_run;
    bool finished = false;
};

using finalization_ptr = seastar::shared_ptr<task_finalization>;

// The module owns the finalization state of its tasks, keyed by task id,
// so that a test which only knows a task's id can finish it.
class test_module : public task_manager::module {
private:
    std::unordered_map<task_id, finalization_ptr> _finalizations;
public:
    test_module(task_manager& tm) noexcept : module(tm, "test") {}

    void register_finalization(task_id id, finalization_ptr finalization) {
        _finalizations[id] = std::move(finalization);
    }

    finalization_ptr get_finalization(task_id id) const noexcept {
        auto it = _finalizations.find(id);
        return it != _finalizations.end() ? it->second : nullptr;
    }

    void erase_finalization(task_id id) noexcept {
        _finalizations.erase(id);
    }
};

class test_task {
private:
    task_manager::task_ptr _task;
    // Null once the task has finished and its finalizer erased the state.
    finalization_ptr _finalization;
public:
    test_task(task_manager::task_ptr task) noexcept
        : _task(task)
        , _finalization(dynamic_pointer_cast<test_module>(task->get_module())->get_finalization(task->id()))
    {}

    future<> finish() noexcept {
        if (_finalization && !_finalization->finished) {
            _finalization->finish_run.set_value();
            _finalization->finished = true;
        }
        return _task->done();
    }

    future<> finish_failed(std::exception_ptr ex) {
        if (_finalization && !_finalization->finished) {
            _finalization->finish_run.set_exception(std::move(ex));
            _finalization->finished = true;
        }
        return _task->done().then_wrapped([] (auto&& f) {
            f.ignore_ready_future();
        });
    }

    void register_task() {
        _task->register_task();
    }

    future<> unregister_task() noexcept {
        co_await finish();
        _task->unregister_task();
    }
};

}

#endif
