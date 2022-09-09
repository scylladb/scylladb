/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "repair/repair.hh"
#include "tasks/task_manager.hh"

class repair_task_impl : public tasks::task_manager::task::impl {
public:
    repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, unsigned sequence_number = 0, std::string keyspace = "", std::string table = "", std::string type = "", std::string entity = "", tasks::task_id parent_id = tasks::task_id::create_null_id())
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(type), std::move(entity), parent_id) {
        _status.progress_units = "ranges";
    }

    virtual future<> run() override = 0;
};

class repair_module : public tasks::task_manager::module {
private:
    repair_service& _rs;
public:
    repair_module(tasks::task_manager& tm, repair_service& rs) noexcept : module(tm, "repair"), _rs(rs) {}

    repair_service& repair_service() noexcept {
        return _rs;
    }
};
