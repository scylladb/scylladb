/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "compaction/compaction.hh"
#include "replica/database_fwd.hh"
#include "schema/schema_fwd.hh"
#include "tasks/task_manager.hh"

#include <optional>

namespace sstables {
class sstable_directory;
}

namespace replica {
class reshard_shard_descriptor;
}

namespace compaction {

class compaction_task_impl : public tasks::task_manager::task::impl {
protected:
    mutable std::optional<uint64_t> _expected_workload;
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
        _status.progress_units = "bytes";
    }

    virtual std::string type() const override = 0;
    virtual tasks::is_abortable is_abortable() const noexcept override;
protected:
    virtual future<> run() override = 0;
    future<tasks::task_manager::task::progress> get_progress(const compaction_data& cdata, const compaction_progress_monitor& progress_monitor) const;
};

using current_task_type = tasks::task_manager::task_ptr;

// The state through which a task waits for its turn among its siblings.
struct compaction_turn {
    seastar::condition_variable& cv;
    current_task_type& current_task;
};

enum class flush_mode {
    skip,               // Skip flushing.  Useful when application explicitly flushes all tables prior to compaction
    compacted_tables,   // Flush only the compacted keyspace/tables
    all_tables          // Flush all tables in the database prior to compaction
};

inline constexpr auto major_compaction_task_type = "major compaction";

class major_compaction_task_impl : public compaction_task_impl {
public:
    major_compaction_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string keyspace,
            std::string table,
            std::string entity,
            tasks::task_id parent_id,
            flush_mode fm = flush_mode::compacted_tables,
            bool consider_only_existing_data = false) noexcept
        : compaction_task_impl(module, id, sequence_number, std::move(scope), std::move(keyspace), std::move(table), std::move(entity), parent_id)
        , _flush_mode(fm)
        , _consider_only_existing_data(consider_only_existing_data)
    {}

    virtual std::string type() const override {
        return major_compaction_task_type;
    }

protected:
    flush_mode _flush_mode;
    bool _consider_only_existing_data;

    virtual future<> run() override = 0;
};

inline constexpr auto cleanup_compaction_task_type = "cleanup compaction";

inline constexpr auto global_cleanup_compaction_task_type = "global cleanup compaction";

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
    {}

    virtual std::string type() const override {
        return cleanup_compaction_task_type;
    }
protected:
    virtual future<> run() override = 0;
};

inline constexpr auto offstrategy_compaction_task_type = "offstrategy compaction";

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
    {}

    virtual std::string type() const override {
        return offstrategy_compaction_task_type;
    }
protected:
    virtual future<> run() override = 0;
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
    {}

    virtual std::string type() const override {
        return "sstables compaction";
    }
protected:
    virtual future<> run() override = 0;
};

inline constexpr auto upgrade_sstables_compaction_task_type = "upgrade sstables compaction";

inline constexpr auto scrub_sstables_compaction_task_type = "scrub sstables compaction";

inline constexpr auto reshaping_compaction_task_type = "reshaping compaction";

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
    {}

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
    compaction_sstable_creator_fn _creator;
    compaction::owned_ranges_ptr _owned_ranges_ptr;
    bool _vnodes_resharding;
public:
    table_resharding_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            sharded<sstables::sstable_directory>& dir,
            sharded<replica::database>& db,
            compaction_sstable_creator_fn creator,
            compaction::owned_ranges_ptr owned_ranges_ptr,
            bool vnodes_resharding) noexcept
        : resharding_compaction_task_impl(module, tasks::task_id::create_random_id(), parent_id ? 0 : module->new_sequence_number(), "table", std::move(keyspace), std::move(table), "", parent_id)
        , _dir(dir)
        , _db(db)
        , _creator(std::move(creator))
        , _owned_ranges_ptr(std::move(owned_ranges_ptr))
        , _vnodes_resharding(vnodes_resharding)
    {}
protected:
    virtual future<> run() override;
    virtual future<std::optional<double>> expected_total_workload() const override;
};

class shard_resharding_compaction_task_impl : public resharding_compaction_task_impl {
private:
    sharded<sstables::sstable_directory>& _dir;
    replica::database& _db;
    compaction_sstable_creator_fn _creator;
    compaction::owned_ranges_ptr _local_owned_ranges_ptr;
    bool _vnodes_resharding;
    std::vector<replica::reshard_shard_descriptor>& _destinations;
public:
    shard_resharding_compaction_task_impl(tasks::task_manager::module_ptr module,
            std::string keyspace,
            std::string table,
            tasks::task_id parent_id,
            sharded<sstables::sstable_directory>& dir,
            replica::database& db,
            compaction_sstable_creator_fn creator,
            compaction::owned_ranges_ptr local_owned_ranges_ptr,
            bool vnodes_resharding,
            std::vector<replica::reshard_shard_descriptor>& destinations) noexcept;
protected:
    virtual future<> run() override;
    virtual future<std::optional<double>> expected_total_workload() const override;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "compaction") {}

    // Starts a major compaction of all the tables on the node.
    future<tasks::task_manager::task_ptr> start_global_major_compaction(sharded<replica::database>& db, std::optional<flush_mode> fm, bool consider_only_existing_data);

    // Starts a major compaction of the given tables of a keyspace on all the shards.
    future<tasks::task_manager::task_ptr> start_major_keyspace_compaction(sharded<replica::database>& db, std::string keyspace, std::vector<table_info> table_infos, std::optional<flush_mode> fm, bool consider_only_existing_data, compaction_turn* turn = nullptr, tasks::task_info parent_info = tasks::make_empty_task_info());

    // Starts a major compaction of the given tables of a keyspace on this shard.
    future<tasks::task_manager::task_ptr> start_shard_major_compaction(replica::database& db, std::string keyspace, const std::vector<table_info>& table_infos, flush_mode fm, bool consider_only_existing_data, tasks::task_info parent_info);

    // Starts a major compaction of a single table on this shard, once the turn is taken by the created task.
    future<tasks::task_manager::task_ptr> start_table_major_compaction(replica::database& db, std::string keyspace, const table_info& info, compaction_turn& turn, flush_mode fm, bool consider_only_existing_data, tasks::task_info parent_info);

    // Starts a cleanup compaction of all the vnode based keyspaces on the node.
    future<tasks::task_manager::task_ptr> start_global_cleanup_compaction(sharded<replica::database>& db);

    // Starts a cleanup compaction of the given tables of a keyspace on all the shards.
    future<tasks::task_manager::task_ptr> start_cleanup_keyspace_compaction(sharded<replica::database>& db, std::string keyspace, const std::vector<table_info>& table_infos, flush_mode fm, tasks::is_user_task is_user_task);

    // Starts a cleanup compaction of the given tables of a keyspace on this shard.
    future<tasks::task_manager::task_ptr> start_shard_cleanup_compaction(replica::database& db, std::string keyspace, const std::vector<table_info>& table_infos, tasks::task_info parent_info);

    // Starts a cleanup compaction of a single table on this shard, once the turn is taken by the created task.
    future<tasks::task_manager::task_ptr> start_table_cleanup_compaction(replica::database& db, std::string keyspace, const table_info& info, compaction_turn& turn, tasks::task_info parent_info);

    // Starts an offstrategy compaction of the given tables of a keyspace on all the shards.
    // If needed is set, it receives whether any table had sstables to compact.
    future<tasks::task_manager::task_ptr> start_offstrategy_keyspace_compaction(sharded<replica::database>& db, std::string keyspace, std::vector<table_info> table_infos, bool* needed);

    // Starts an offstrategy compaction of the given tables of a keyspace on this shard.
    // needed is set if any table had sstables to compact.
    future<tasks::task_manager::task_ptr> start_shard_offstrategy_compaction(replica::database& db, std::string keyspace, const std::vector<table_info>& table_infos, bool& needed, tasks::task_info parent_info);

    // Starts an offstrategy compaction of a single table on this shard, once the turn is taken by the created task.
    // needed is set if the table had sstables to compact.
    future<tasks::task_manager::task_ptr> start_table_offstrategy_compaction(replica::database& db, std::string keyspace, const table_info& info, compaction_turn& turn, bool& needed, tasks::task_info parent_info);

    // Starts an sstable upgrade of the given tables of a keyspace on all the shards.
    future<tasks::task_manager::task_ptr> start_upgrade_sstables_keyspace_compaction(sharded<replica::database>& db, std::string keyspace, std::vector<table_info> table_infos, bool exclude_current_version);

    // Starts an sstable upgrade of the given tables of a keyspace on this shard.
    future<tasks::task_manager::task_ptr> start_shard_upgrade_sstables_compaction(replica::database& db, std::string keyspace, const std::vector<table_info>& table_infos, bool exclude_current_version, tasks::task_info parent_info);

    // Starts an sstable upgrade of a single table on this shard, once the turn is taken by the created task.
    future<tasks::task_manager::task_ptr> start_table_upgrade_sstables_compaction(replica::database& db, std::string keyspace, const table_info& info, compaction_turn& turn, bool exclude_current_version, tasks::task_info parent_info);

    // Starts a scrub of the given tables of a keyspace on all the shards.
    // If stats is set, it receives the scrub's result.
    future<tasks::task_manager::task_ptr> start_scrub_sstables_keyspace_compaction(sharded<replica::database>& db, std::string keyspace, std::vector<sstring> column_families, compaction_type_options::scrub opts, compaction_stats* stats);

    // Starts a scrub of the given tables of a keyspace on this shard.
    // stats receives the scrub's result.
    future<tasks::task_manager::task_ptr> start_shard_scrub_sstables_compaction(replica::database& db, std::string keyspace, const std::vector<sstring>& column_families, compaction_type_options::scrub opts, compaction_stats& stats, tasks::task_info parent_info);

    // Starts a scrub of a single table on this shard.
    // The scrub's result is added to stats.
    future<tasks::task_manager::task_ptr> start_table_scrub_sstables_compaction(replica::database& db, std::string keyspace, std::string table, compaction_type_options::scrub opts, compaction_stats& stats, tasks::task_info parent_info);

    // Starts a reshape of a table's sstables collected by the directory, on all the shards.
    future<tasks::task_manager::task_ptr> start_table_reshaping_compaction(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, std::string keyspace, std::string table, reshape_mode mode, compaction_sstable_creator_fn creator, std::function<bool (const sstables::shared_sstable&)> filter);

    // Starts a reshape of a table's sstables collected by the directory, on this shard.
    // total_shard_size is increased by the size of the reshaped sstables.
    future<tasks::task_manager::task_ptr> start_shard_reshaping_compaction(sstables::sstable_directory& dir, sharded<replica::database>& db, std::string keyspace, std::string table, reshape_mode mode, compaction_sstable_creator_fn creator, std::function<bool (const sstables::shared_sstable&)> filter, uint64_t& total_shard_size, tasks::task_info parent_info);
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
    {}

    virtual std::string type() const override {
        return "regular compaction";
    }

    virtual tasks::is_internal is_internal() const noexcept override {
        return tasks::is_internal::yes;
    }
protected:
    virtual future<> run() override = 0;
};

} // namespace compaction

template <>
struct fmt::formatter<compaction::flush_mode> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(compaction::flush_mode, fmt::format_context& ctx) const -> decltype(ctx.out());
};
