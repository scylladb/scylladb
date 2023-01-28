/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/database.hh"
#include "utils/api.hh"

logging::logger apilog("api");

namespace std {

std::ostream& operator<<(std::ostream& os, const api::table_info& ti) {
    return os << "table{name=" << ti.name << ", id=" << ti.id << "}";
}

} // namespace std

namespace api {

// Run on all tables, skipping dropped tables
future<> run_on_existing_tables(sstring op, replica::database& db, std::string_view keyspace, const std::vector<table_info> local_tables, std::function<future<> (replica::table&)> func) {
    std::exception_ptr ex;
    for (const auto& ti : local_tables) {
        apilog.debug("Starting {} on {}.{}", op, keyspace, ti);
        try {
            co_await func(db.find_column_family(ti.id));
        } catch (const replica::no_such_column_family& e) {
            apilog.warn("Skipping {} of {}.{}: {}", op, keyspace, ti, e.what());
        } catch (...) {
            ex = std::current_exception();
            apilog.error("Failed {} of {}.{}: {}", op, keyspace, ti, ex);
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }
}

}
