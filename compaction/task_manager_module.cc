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

future<> shard_major_keyspace_compaction_task_impl::run() {
    // major compact smaller tables first, to increase chances of success if low on space.
    std::ranges::sort(_local_tables, std::less<>(), [&] (const api::table_info& ti) {
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

}
