/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "compaction/compaction.hh"
#include "replica/database_fwd.hh"
#include "schema/schema_fwd.hh"
#include "tasks/task_manager.hh"

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
    std::vector<table_info> _table_infos;
public:
    major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            sharded<replica::database>& db,
            std::vector<table_info> table_infos) noexcept
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
    std::vector<table_info> _local_tables;
public:
    shard_major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            tasks::task_id parent_id,
            replica::database& db,
            std::vector<table_info> local_tables) noexcept
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, std::move(keyspace), "", "", parent_id)
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
    table_info _ti;
    seastar::condition_variable& _cv;
    tasks::task_manager::task_ptr& _current_task;
public:
    table_major_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            replica::database& db,
            table_info ti,
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


class cleanup_compaction_task_impl : public compaction_task_impl {
public:
    cleanup_compaction_task_impl(tasks::task_manager::module_ptr module,
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
        return "cleanup compaction";
    }
protected:
    virtual future<> run() override = 0;
};

class cleanup_keyspace_compaction_task_impl : public cleanup_compaction_task_impl {
private:
    sharded<replica::database>& _db;
    std::vector<table_info> _table_infos;
public:
    cleanup_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            sharded<replica::database>& db,
            std::vector<table_info> table_infos) noexcept
        : cleanup_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _table_infos(std::move(table_infos))
    {}
protected:
    virtual future<> run() override;
};

class shard_cleanup_keyspace_compaction_task_impl : public cleanup_compaction_task_impl {
private:
    replica::database& _db;
    std::vector<table_info> _local_tables;
public:
    shard_cleanup_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            tasks::task_id parent_id,
            replica::database& db,
            std::vector<table_info> local_tables) noexcept
        : cleanup_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _local_tables(std::move(local_tables))
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class table_cleanup_keyspace_compaction_task_impl : public cleanup_compaction_task_impl {
private:
    replica::database& _db;
    table_info _ti;
    seastar::condition_variable& _cv;
    tasks::task_manager::task_ptr& _current_task;
public:
    table_cleanup_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            replica::database& db,
            table_info ti,
            seastar::condition_variable& cv,
            tasks::task_manager::task_ptr& current_task) noexcept
        : cleanup_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class offstrategy_compaction_task_impl : public compaction_task_impl {
public:
    offstrategy_compaction_task_impl(tasks::task_manager::module_ptr module,
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
        return "offstrategy compaction";
    }
protected:
    virtual future<> run() override = 0;
};

class offstrategy_keyspace_compaction_task_impl : public offstrategy_compaction_task_impl {
private:
    sharded<replica::database>& _db;
    std::vector<table_info> _table_infos;
    bool& _needed;
public:
    offstrategy_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            sharded<replica::database>& db,
            std::vector<table_info> table_infos,
            bool& needed) noexcept
        : offstrategy_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _needed(needed)
    {}
protected:
    virtual future<> run() override;
};

class shard_offstrategy_keyspace_compaction_task_impl : public offstrategy_compaction_task_impl {
private:
    replica::database& _db;
    std::vector<table_info> _table_infos;
    bool& _needed;
public:
    shard_offstrategy_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            tasks::task_id parent_id,
            replica::database& db,
            std::vector<table_info> table_infos,
            bool& needed) noexcept
        : offstrategy_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _needed(needed)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class table_offstrategy_keyspace_compaction_task_impl : public offstrategy_compaction_task_impl {
private:
    replica::database& _db;
    table_info _ti;
    seastar::condition_variable& _cv;
    tasks::task_manager::task_ptr& _current_task;
    bool& _needed;
public:
    table_offstrategy_keyspace_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            replica::database& db,
            table_info ti,
            seastar::condition_variable& cv,
            tasks::task_manager::task_ptr& current_task,
            bool& needed) noexcept
        : offstrategy_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
        , _needed(needed)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class sstables_compaction_task_impl : public compaction_task_impl {
public:
    sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
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
        return "rewrite sstables compaction";
    }
protected:
    virtual future<> run() override = 0;
};

class upgrade_sstables_compaction_task_impl : public sstables_compaction_task_impl {
private:
    sharded<replica::database>& _db;
    std::vector<table_info> _table_infos;
    bool _exclude_current_version;
public:
    upgrade_sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            sharded<replica::database>& db,
            std::vector<table_info> table_infos,
            bool exclude_current_version) noexcept
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _exclude_current_version(exclude_current_version)
    {}
protected:
    virtual future<> run() override;
};

class shard_upgrade_sstables_compaction_task_impl : public sstables_compaction_task_impl {
private:
    replica::database& _db;
    std::vector<table_info> _table_infos;
    bool _exclude_current_version;
public:
    shard_upgrade_sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            tasks::task_id parent_id,
            replica::database& db,
            std::vector<table_info> table_infos,
            bool exclude_current_version) noexcept
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _exclude_current_version(exclude_current_version)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class scrub_sstables_compaction_task_impl : public sstables_compaction_task_impl {
private:
    sharded<replica::database>& _db;
    std::vector<sstring> _column_families;
    sstables::compaction_type_options::scrub _opts;
    sstables::compaction_stats& _stats;
public:
    scrub_sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            sharded<replica::database>& db,
            std::vector<sstring> column_families,
            sstables::compaction_type_options::scrub opts,
            sstables::compaction_stats& stats) noexcept
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _column_families(std::move(column_families))
        , _opts(opts)
        , _stats(stats)
    {}
protected:
    virtual future<> run() override;
};

class shard_scrub_sstables_compaction_task_impl : public sstables_compaction_task_impl {
private:
    replica::database& _db;
    std::vector<sstring> _column_families;
    sstables::compaction_type_options::scrub _opts;
    sstables::compaction_stats& _stats;
public:
    shard_scrub_sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            tasks::task_id parent_id,
            replica::database& db,
            std::vector<sstring> column_families,
            sstables::compaction_type_options::scrub opts,
            sstables::compaction_stats& stats) noexcept
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _column_families(std::move(column_families))
        , _opts(opts)
        , _stats(stats)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class table_scrub_sstables_compaction_task_impl : public sstables_compaction_task_impl {
private:
    replica::database& _db;
    sstables::compaction_type_options::scrub _opts;
    sstables::compaction_stats& _stats;
public:
    table_scrub_sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            replica::database& db,
            sstables::compaction_type_options::scrub opts,
            sstables::compaction_stats& stats) noexcept
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _opts(opts)
        , _stats(stats)
    {}

    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "compaction") {}
};

}
