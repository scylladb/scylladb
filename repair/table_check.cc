/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/database.hh"
#include "repair/table_check.hh"
#include "service/migration_manager.hh"

namespace repair {

future<table_dropped> table_sync_and_check(replica::database& db, service::migration_manager& mm, const table_id& uuid) {
    if (mm.use_raft()) {
        // Trigger read barrier to synchronize schema.
        co_await mm.get_group0_barrier().trigger(mm.get_abort_source());
    }

    co_return !db.column_family_exists(uuid);
}

future<table_dropped> with_table_drop_silenced(replica::database& db, service::migration_manager& mm, const table_id& uuid,
        std::function<future<>(const table_id&)> f) {
    std::exception_ptr ex = nullptr;
    try {
        co_await f(uuid);
        co_return table_dropped::no;
    } catch (replica::no_such_column_family&) {
        // No need to synchronize while we know the table was dropped.
    } catch (...) {
        // This node may still see a table while it is dropped on the remote node
        // and so the remote node returns an error. In that case we want to skip
        // that table and continue with the operation.
        //
        // But since RPC does not enable returning the exception type, the cause
        // of the failure cannot be determined. Synchronize schema to see the latest
        // changes and determine whether the table was dropped.
        ex = std::current_exception();
    }

    if (ex) {
        auto dropped = co_await table_sync_and_check(db, mm, uuid);
        if (!dropped) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }
    co_return table_dropped::yes;
}

}
