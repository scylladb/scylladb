/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>
#include <seastar/core/future.hh>

#include "tasks/task_manager.hh"
#include "db/snapshot_types.hh"
#include "schema/schema_fwd.hh"

namespace db {
class snapshot_ctl;
}

namespace service {
class storage_proxy;
}

namespace replica {
    class table;
}

namespace db::snapshot {

std::string sstables_location(std::string_view prefix, const replica::table&, std::string_view snapshot_name);
std::string snapshot_meta_location(std::string_view prefix, const replica::table&, std::string_view snapshot_name);

future<tasks::task_id> 
start_global_backup(db::snapshot_ctl&, tasks::task_manager::module_ptr, std::string snapshot_name, std::unordered_multimap<sstring, sstring> ks_tables, std::unordered_map<sstring, snapshot_dc_location> locations, bool move_files);

future<>
backup_sstables(db::snapshot_ctl&, table_id table_id, std::string tag, std::string endpoint, std::string bucket, std::string prefix, dht::token first_token, dht::token last_token, utils::chunked_vector<sstables::sstable_id> sstable_ids, bool use_move);

}
