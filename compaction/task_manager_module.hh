/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/condition-variable.hh>

#include "replica/database_fwd.hh"
#include "table_state.hh"
#include "tasks/task_manager.hh"
#include "utils/api.hh"

namespace compaction {

class compaction_task_impl : public tasks::task_manager::task::impl {
public:
    compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {
        // FIXME: add progress units
    }

    virtual std::string type() const override = 0;
protected:
    virtual future<> run() override = 0;
};

class major_compaction_task_impl : public compaction_task_impl {
public:
    major_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {
        // FIXME: add progress units
    }

    virtual std::string type() const override {
        return "major compaction";
    }
protected:
    virtual future<> run() override = 0;
};

class major_keyspace_compaction_task_impl : public major_compaction_task_impl {
private:
    sharded<replica::database>& _db;
    std::vector<api::table_info> _table_infos;
public:
    major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            sharded<replica::database>& db,
            std::vector<api::table_info> table_infos) noexcept
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _table_infos(std::move(table_infos))
    {}
protected:
    virtual future<> run() override;
};

class shard_major_keyspace_compaction_task_impl : public major_compaction_task_impl {
private:
    replica::database& _db;
    std::vector<api::table_info> _local_tables;
public:
    shard_major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            tasks::task_id parent_id,
            replica::database& db,
            std::vector<api::table_info> local_tables) noexcept
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _local_tables(std::move(local_tables))
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class table_major_keyspace_compaction_task_impl : public major_compaction_task_impl {
private:
    replica::database& _db;
    api::table_info _ti;
    seastar::condition_variable& _cv;
    tasks::task_manager::task_ptr& _current_task;
public:
    table_major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            replica::database& db,
            api::table_info ti,
            seastar::condition_variable& cv,
            tasks::task_manager::task_ptr& current_task) noexcept
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

template<typename T>
requires requires (T task) {
    {task->perform()};
}
class compaction_group_major_keyspace_compaction_task_impl : public major_compaction_task_impl {
private:
    table_state& _table_state;
    T _compaction_manager_task;
public:
    compaction_group_major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            table_state& table_state,
            T compaction_manager_task) noexcept
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), std::move(table), "", parent_id)
        , _table_state(table_state)
        , _compaction_manager_task(std::move(compaction_manager_task))
    {}

    tasks::is_internal is_internal() const noexcept {
        return tasks::is_internal::yes;
    }
protected:
    future<> run() {
        return _compaction_manager_task->perform();
    }

    friend class ::compaction_manager;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "compaction") {}
};

}
