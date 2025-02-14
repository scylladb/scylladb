/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/util/bool_class.hh>

#include "schema/schema_fwd.hh"

using table_dropped = bool_class<class table_dropped_tag>;

namespace raft {
class server;
}

namespace replica {
class database;
}

namespace service {
class migration_manager;
}

namespace repair {

class database;

future<table_dropped> table_sync_and_check(replica::database& db, service::migration_manager& mm, const table_id& uuid);

// Runs function f on given table. If f throws and the table is dropped, the exception is swallowed.
// Function is aimed to handle no_such_column_family on remote node or different shard, as it synchronizes
// schema before checking the table. Prefer standard error handling whenever possible.
future<table_dropped> with_table_drop_silenced(replica::database& db, service::migration_manager& mm, const table_id& uuid,
        std::function<future<>(const table_id&)> f);

}
