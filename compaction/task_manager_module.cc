/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "compaction/task_manager_module.hh"
#include "replica/database.hh"

namespace compaction {

// Run on all tables, skipping dropped tables
future<> run_on_existing_tables(sstring op, replica::database& db, std::string_view keyspace, const std::vector<table_id> local_tables, std::function<future<> (replica::table&)> func) {
    std::exception_ptr ex;
    for (const auto& ti : local_tables) {
        tasks::tmlogger.debug("Starting {} on {}.{}", op, keyspace, ti);
        try {
            co_await func(db.find_column_family(ti));
        } catch (const replica::no_such_column_family& e) {
            tasks::tmlogger.warn("Skipping {} of {}.{}: {}", op, keyspace, ti, e.what());
        } catch (...) {
            ex = std::current_exception();
            tasks::tmlogger.error("Failed {} of {}.{}: {}", op, keyspace, ti, ex);
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }
}

}
