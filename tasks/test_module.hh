/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#pragma once

#include "task_manager.hh"

namespace tasks {

class test_module : public task_manager::module {
public:
    test_module(task_manager& tm) noexcept : module(tm, "test") {}
};

class test_task_impl : public task_manager::task::impl {
private:
    promise<> _finish_run;
    bool _finished = false;
public:
    test_task_impl(task_manager::module_ptr module, task_id id, uint64_t sequence_number = 0, std::string keyspace = "", std::string table = "", std::string entity = "", task_id parent_id = task_id::create_null_id()) noexcept
        : task_manager::task::impl(module, id, sequence_number, "test", std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {}

    virtual std::string type() const override {
        return "test";
    }

    future<> run() override {
        return _finish_run.get_future();
    }

    friend class test_task;
};

class test_task {
private:
    task_manager::task_ptr _task;
public:
    test_task(task_manager::task_ptr task) noexcept : _task(task) {}

    future<> finish() noexcept {
        auto& task_impl = dynamic_cast<test_task_impl&>(*_task->_impl);
        if (!task_impl._finished) {
            task_impl._finish_run.set_value();
            task_impl._finished = true;
        }
        return _task->done();
    }

    future<> finish_failed(std::exception_ptr ex) {
        auto& task_impl = dynamic_cast<test_task_impl&>(*_task->_impl);
        if (!task_impl._finished) {
            task_impl._finish_run.set_exception(ex);
            task_impl._finished = true;
        }
        return _task->done().then_wrapped([] (auto&& f) {
            f.ignore_ready_future();
        });
    }

    void register_task() {
        _task->register_task();
    }

    future<> unregister_task() noexcept {
        auto& task_impl = dynamic_cast<test_task_impl&>(*_task->_impl);
        co_await finish();
        co_await task_impl._done.get_shared_future();
        _task->unregister_task();
    }
};

}

#endif
