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
        auto local_tables = _table_infos;
        // major compact smaller tables first, to increase chances of success if low on space.
        std::ranges::sort(local_tables, std::less<>(), [&] (const api::table_info& ti) {
            try {
                return db.find_column_family(ti.id).get_stats().live_disk_space_used;
            } catch (const replica::no_such_column_family& e) {
                return int64_t(-1);
            }
        });
        co_await run_on_existing_tables("force_keyspace_compaction", db, _status.keyspace, local_tables, [] (replica::table& t) {
            return t.compact_all_sstables();
        });
    });
}

}
