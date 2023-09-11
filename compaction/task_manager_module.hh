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

namespace sstables {
class sstable_directory;
}

namespace replica {
class reshard_shard_descriptor;
}

namespace compaction {

class compaction_task_impl : public tasks::task_manager::task::impl {
public:
    compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
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
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
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
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "keyspace", std::move(keyspace), "", "", tasks::task_id::create_null_id())
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
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _local_tables(std::move(local_tables))
    {}
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
        : major_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "table", std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
    {}
protected:
    virtual future<> run() override;
};


class cleanup_compaction_task_impl : public compaction_task_impl {
public:
    cleanup_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
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
        : cleanup_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "keyspace", std::move(keyspace), "", "", tasks::task_id::create_null_id())
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
        : cleanup_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _local_tables(std::move(local_tables))
    {}
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
        : cleanup_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "table", std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
    {}
protected:
    virtual future<> run() override;
};

class offstrategy_compaction_task_impl : public compaction_task_impl {
public:
    offstrategy_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
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
        : offstrategy_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "keyspace", std::move(keyspace), "", "", tasks::task_id::create_null_id())
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
        : offstrategy_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _needed(needed)
    {}
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
        : offstrategy_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "table", std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
        , _needed(needed)
    {}
protected:
    virtual future<> run() override;
};

class sstables_compaction_task_impl : public compaction_task_impl {
public:
    sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {
        // FIXME: add progress units
    }

    virtual std::string type() const override {
        return "sstables compaction";
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
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "keyspace", std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _exclude_current_version(exclude_current_version)
    {}

    virtual std::string type() const override {
        return "upgrade " + sstables_compaction_task_impl::type();
    }
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
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _table_infos(std::move(table_infos))
        , _exclude_current_version(exclude_current_version)
    {}

    virtual std::string type() const override {
        return "upgrade " + sstables_compaction_task_impl::type();
    }
protected:
    virtual future<> run() override;
};

class table_upgrade_sstables_compaction_task_impl : public sstables_compaction_task_impl {
private:
    replica::database& _db;
    table_info _ti;
    seastar::condition_variable& _cv;
    tasks::task_manager::task_ptr& _current_task;
    bool _exclude_current_version;
public:
    table_upgrade_sstables_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            replica::database& db,
            table_info ti,
            seastar::condition_variable& cv,
            tasks::task_manager::task_ptr& current_task,
            bool exclude_current_version) noexcept
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "table", std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _ti(std::move(ti))
        , _cv(cv)
        , _current_task(current_task)
        , _exclude_current_version(exclude_current_version)
    {}

    virtual std::string type() const override {
        return "upgrade " + sstables_compaction_task_impl::type();
    }
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
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "keyspace", std::move(keyspace), "", "", tasks::task_id::create_null_id())
        , _db(db)
        , _column_families(std::move(column_families))
        , _opts(opts)
        , _stats(stats)
    {}

    virtual std::string type() const override {
        return "scrub " + sstables_compaction_task_impl::type();
    }
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
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), "", "", parent_id)
        , _db(db)
        , _column_families(std::move(column_families))
        , _opts(opts)
        , _stats(stats)
    {}

    virtual std::string type() const override {
        return "scrub " + sstables_compaction_task_impl::type();
    }
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
        : sstables_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "table", std::move(keyspace), std::move(table), "", parent_id)
        , _db(db)
        , _opts(opts)
        , _stats(stats)
    {}

    virtual std::string type() const override {
        return "scrub " + sstables_compaction_task_impl::type();
    }
protected:
    virtual future<> run() override;
};

class reshaping_compaction_task_impl : public compaction_task_impl {
public:
    reshaping_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {
        // FIXME: add progress units
    }

    virtual std::string type() const override {
        return "reshaping compaction";
    }
protected:
    virtual future<> run() override = 0;
};

class table_reshaping_compaction_task_impl : public reshaping_compaction_task_impl {
private:
    sharded<sstables::sstable_directory>& _dir;
    sharded<replica::database>& _db;
    sstables::reshape_mode _mode;
    sstables::compaction_sstable_creator_fn _creator;
    std::function<bool (const sstables::shared_sstable&)> _filter;
public:
    table_reshaping_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            sharded<sstables::sstable_directory>& dir,
            sharded<replica::database>& db,
            sstables::reshape_mode mode,
            sstables::compaction_sstable_creator_fn creator,
            std::function<bool (const sstables::shared_sstable&)> filter) noexcept
        : reshaping_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "table", std::move(keyspace), std::move(table), "", tasks::task_id::create_null_id())
        , _dir(dir)
        , _db(db)
        , _mode(mode)
        , _creator(std::move(creator))
        , _filter(std::move(filter))
    {}
protected:
    virtual future<> run() override;
};

class shard_reshaping_compaction_task_impl : public reshaping_compaction_task_impl {
private:
    sstables::sstable_directory& _dir;
    sharded<replica::database>& _db;
    sstables::reshape_mode _mode;
    sstables::compaction_sstable_creator_fn _creator;
    std::function<bool (const sstables::shared_sstable&)> _filter;
    uint64_t& _total_shard_size;
public:
    shard_reshaping_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            sstables::sstable_directory& dir,
            sharded<replica::database>& db,
            sstables::reshape_mode mode,
            sstables::compaction_sstable_creator_fn creator,
            std::function<bool (const sstables::shared_sstable&)> filter,
            uint64_t& total_shard_size) noexcept
        : reshaping_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), std::move(table), "", parent_id)
        , _dir(dir)
        , _db(db)
        , _mode(mode)
        , _creator(std::move(creator))
        , _filter(std::move(filter))
        , _total_shard_size(total_shard_size)
    {}
protected:
    virtual future<> run() override;
};


class resharding_compaction_task_impl : public compaction_task_impl {
public:
    resharding_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {
        // FIXME: add progress units
    }

    virtual std::string type() const override {
        return "resharding compaction";
    }
protected:
    virtual future<> run() override = 0;
};

class table_resharding_compaction_task_impl : public resharding_compaction_task_impl {
private:
    sharded<sstables::sstable_directory>& _dir;
    sharded<replica::database>& _db;
    sstables::compaction_sstable_creator_fn _creator;
    compaction::owned_ranges_ptr _owned_ranges_ptr;
public:
    table_resharding_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            sharded<sstables::sstable_directory>& dir,
            sharded<replica::database>& db,
            sstables::compaction_sstable_creator_fn creator,
            compaction::owned_ranges_ptr owned_ranges_ptr) noexcept
        : resharding_compaction_task_impl(module, tasks::task_id::create_random_id(), module->new_sequence_number(), "table", std::move(keyspace), std::move(table), "", tasks::task_id::create_null_id())
        , _dir(dir)
        , _db(db)
        , _creator(std::move(creator))
        , _owned_ranges_ptr(std::move(owned_ranges_ptr))
    {}
protected:
    virtual future<> run() override;
};

class shard_resharding_compaction_task_impl : public resharding_compaction_task_impl {
private:
    sharded<sstables::sstable_directory>& _dir;
    replica::database& _db;
    sstables::compaction_sstable_creator_fn _creator;
    compaction::owned_ranges_ptr _local_owned_ranges_ptr;
    std::vector<replica::reshard_shard_descriptor>& _destinations;
public:
    shard_resharding_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            sharded<sstables::sstable_directory>& dir,
            replica::database& db,
            sstables::compaction_sstable_creator_fn creator,
            compaction::owned_ranges_ptr local_owned_ranges_ptr,
            std::vector<replica::reshard_shard_descriptor>& destinations) noexcept
        : resharding_compaction_task_impl(module, tasks::task_id::create_random_id(), 0, "shard", std::move(keyspace), std::move(table), "", parent_id)
        , _dir(dir)
        , _db(db)
        , _creator(std::move(creator))
        , _local_owned_ranges_ptr(std::move(local_owned_ranges_ptr))
        , _destinations(destinations)
    {}
protected:
    virtual future<> run() override;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "compaction") {}
};

class regular_compaction_task_impl : public compaction_task_impl {
public:
    regular_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id) noexcept
        : compaction_task_impl(module, id, sequence_number, "compaction group", std::move(keyspace), std::move(table), std::move(entity), parent_id)
    {
        // FIXME: add progress units
    }

    virtual std::string type() const override {
        return "regular compaction";
    }
protected:
    virtual future<> run() override = 0;
};

}
