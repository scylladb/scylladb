/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "compaction/task_manager_module.hh"
#include "replica/database.hh"

namespace compaction {

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

struct table_tasks_info {
    tasks::task_manager::task_ptr task;
    api::table_info ti;

    table_tasks_info(tasks::task_manager::task_ptr t, api::table_info info)
        : task(t)
        , ti(info)
    {}
};

future<> shard_major_keyspace_compaction_task_impl::run() {
    std::exception_ptr ex;
    seastar::condition_variable cv;
    tasks::task_manager::task_ptr current_task;
    tasks::task_info parent_info{_status.id, _status.shard};
    std::vector<table_tasks_info> table_tasks;
    for (auto& ti : _local_tables) {
        table_tasks.emplace_back(co_await _module->make_and_start_task<table_major_keyspace_compaction_task_impl>(parent_info, _status.keyspace, ti.name, _status.id, _db, ti, cv, current_task), ti);
    }

    // While compaction is run on one table, the size of tables may significantly change.
    // Thus, they are sorted before each invidual compaction and the smallest table is chosen.
    while (!table_tasks.empty()) {
        try {
    // FIXME: fix indentation
    // Major compact smaller tables first, to increase chances of success if low on space.
    // Tables will be kept in descending order.
    std::ranges::sort(table_tasks, std::greater<>(), [&] (const table_tasks_info& tti) {
        try {
            return _db.find_column_family(tti.ti.id).get_stats().live_disk_space_used;
        } catch (const replica::no_such_column_family& e) {
            return int64_t(-1);
        }
    });
            // Task responsible for the smallest table.
            current_task = table_tasks.back().task;
            table_tasks.pop_back();
            cv.broadcast();
            co_await current_task->done();
        } catch (...) {
            ex = std::current_exception();
            current_task = nullptr;
            cv.broken(ex);
            break;
        }
    }

    if (ex) {
        // Wait for all tasks even on failure.
        for (auto& tti: table_tasks) {
            co_await tti.task->done();
        }
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

tasks::is_internal table_major_keyspace_compaction_task_impl::is_internal() const noexcept {
    return tasks::is_internal::yes;
}

future<> table_major_keyspace_compaction_task_impl::run() {
    co_await _cv.wait([&] {
        return _current_task && _current_task->id() == _status.id;
    });
    co_await run_on_table("force_keyspace_compaction", _db, _status.keyspace, _ti, [] (replica::table& t) {
        return t.compact_all_sstables();
    });
}

}
