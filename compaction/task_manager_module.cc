/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "compaction/task_manager_module.hh"
#include "compaction/compaction_manager.hh"
#include "replica/database.hh"

namespace compaction {

// Run on all tables, skipping dropped tables
future<> run_on_existing_tables(sstring op, replica::database& db, std::string keyspace, const std::vector<table_info> local_tables, std::function<future<> (replica::table&)> func) {
    std::exception_ptr ex;
    for (const auto& ti : local_tables) {
        tasks::tmlogger.debug("Starting {} on {}.{}", op, keyspace, ti.name);
        try {
            co_await func(db.find_column_family(ti.id));
        } catch (const replica::no_such_column_family& e) {
            tasks::tmlogger.warn("Skipping {} of {}.{}: {}", op, keyspace, ti.name, e.what());
        } catch (...) {
            ex = std::current_exception();
            tasks::tmlogger.error("Failed {} of {}.{}: {}", op, keyspace, ti.name, ex);
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }
}

future<> major_keyspace_compaction_task_impl::run() {
    co_await _db.invoke_on_all([&] (replica::database& db) -> future<> {
        tasks::task_info parent_info{_status.id, _status.shard};
        auto& module = db.get_compaction_manager().get_task_manager_module();
        auto task = co_await module.make_and_start_task<shard_major_keyspace_compaction_task_impl>(parent_info, _status.keyspace, _status.id, db, _table_infos);
        co_await task->done();
    });
}

tasks::is_internal shard_major_keyspace_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> shard_major_keyspace_compaction_task_impl::run() {
    // Major compact smaller tables first, to increase chances of success if low on space.
    std::ranges::sort(_local_tables, std::less<>(), [&] (const table_info& ti) {
        try {
            return _db.find_column_family(ti.id).get_stats().live_disk_space_used;
        } catch (const replica::no_such_column_family& e) {
            return int64_t(-1);
        }
    });
    co_await run_on_existing_tables("force_keyspace_compaction", _db, _status.keyspace, _local_tables, [] (replica::table& t) {
        return t.compact_all_sstables();
    });
}

future<> cleanup_keyspace_compaction_task_impl::run() {
    co_await _db.invoke_on_all([&] (replica::database& db) -> future<> {
        auto& module = db.get_compaction_manager().get_task_manager_module();
        auto task = co_await module.make_and_start_task<shard_cleanup_keyspace_compaction_task_impl>({_status.id, _status.shard}, _status.keyspace, _status.id, db, _table_infos);
        co_await task->done();
    });
}

tasks::is_internal shard_cleanup_keyspace_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> shard_cleanup_keyspace_compaction_task_impl::run() {
    // Cleanup smaller tables first, to increase chances of success if low on space.
    std::ranges::sort(_local_tables, std::less<>(), [&] (const table_info& ti) {
        try {
            return _db.find_column_family(ti.id).get_stats().live_disk_space_used;
        } catch (const replica::no_such_column_family& e) {
            return int64_t(-1);
        }
    });
    auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(_db.get_keyspace_local_ranges(_status.keyspace));
    co_await run_on_existing_tables("force_keyspace_cleanup", _db, _status.keyspace, _local_tables, [&] (replica::table& t) {
        return t.perform_cleanup_compaction(owned_ranges_ptr);
    });
}

future<> offstrategy_keyspace_compaction_task_impl::run() {
    _needed = co_await _db.map_reduce0([&] (replica::database& db) -> future<bool> {
        bool needed = false;
        tasks::task_info parent_info{_status.id, _status.shard};
        auto& module = db.get_compaction_manager().get_task_manager_module();
        auto task = co_await module.make_and_start_task<shard_offstrategy_keyspace_compaction_task_impl>(parent_info, _status.keyspace, _status.id, db, _table_infos, needed);
        co_await task->done();
        co_return needed;
    }, false, std::plus<bool>());
}

tasks::is_internal shard_offstrategy_keyspace_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> shard_offstrategy_keyspace_compaction_task_impl::run() {
    co_await run_on_existing_tables("perform_keyspace_offstrategy_compaction", _db, _status.keyspace, _table_infos, [this] (replica::table& t) -> future<> {
        _needed |= co_await t.perform_offstrategy_compaction();
    });
}

future<> upgrade_sstables_compaction_task_impl::run() {
    co_await _db.invoke_on_all([&] (replica::database& db) -> future<> {
        tasks::task_info parent_info{_status.id, _status.shard};
        auto& compaction_module = db.get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<shard_upgrade_sstables_compaction_task_impl>(parent_info, _status.keyspace, _status.id, db, _table_infos, _exclude_current_version);
        co_await task->done();
    });
}

tasks::is_internal shard_upgrade_sstables_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> shard_upgrade_sstables_compaction_task_impl::run() {
    auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(_db.get_keyspace_local_ranges(_status.keyspace));
    co_await run_on_existing_tables("upgrade_sstables", _db, _status.keyspace, _table_infos, [&] (replica::table& t) -> future<> {
        return t.parallel_foreach_table_state([&] (compaction::table_state& ts) -> future<> {
            return t.get_compaction_manager().perform_sstable_upgrade(owned_ranges_ptr, ts, _exclude_current_version);
        });
    });
}

future<> scrub_sstables_compaction_task_impl::run() {
    _stats = co_await _db.map_reduce0([&] (replica::database& db) -> future<sstables::compaction_stats> {
        sstables::compaction_stats stats;
        tasks::task_info parent_info{_status.id, _status.shard};
        auto& compaction_module = db.get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<shard_scrub_sstables_compaction_task_impl>(parent_info, _status.keyspace, _status.id, db, _column_families, _opts, stats);
        co_await task->done();
        co_return stats;
    }, sstables::compaction_stats{}, std::plus<sstables::compaction_stats>());
}

tasks::is_internal shard_scrub_sstables_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> shard_scrub_sstables_compaction_task_impl::run() {
    _stats = co_await map_reduce(_column_families, [&] (sstring cfname) -> future<sstables::compaction_stats> {
        sstables::compaction_stats stats{};
        tasks::task_info parent_info{_status.id, _status.shard};
        auto& compaction_module = _db.get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<table_scrub_sstables_compaction_task_impl>(parent_info, _status.keyspace, cfname, _status.id, _db, _opts, stats);
        co_await task->done();
        co_return stats;
    }, sstables::compaction_stats{}, std::plus<sstables::compaction_stats>());
}

tasks::is_internal table_scrub_sstables_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> table_scrub_sstables_compaction_task_impl::run() {
    auto& cm = _db.get_compaction_manager();
    auto& cf = _db.find_column_family(_status.keyspace, _status.table);
    co_await cf.parallel_foreach_table_state([&] (compaction::table_state& ts) mutable -> future<> {
        auto r = co_await cm.perform_sstable_scrub(ts, _opts);
        _stats += r.value_or(sstables::compaction_stats{});
    });
}


}
