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
#include "locator/host_id.hh"

namespace db {
class snapshot_ctl;
}

namespace service {
class storage_proxy;
}

namespace replica {
    class database;
    class table;
}

namespace message {
    class messaging_service;
}

namespace cql3 {
    class query_processor;
}

namespace db::snapshot {

std::string sstables_location(std::string_view prefix, const replica::table&, std::string_view snapshot_name);
std::string snapshot_meta_location(std::string_view prefix, const replica::table&, std::string_view snapshot_name);

future<tasks::task_id> 
start_global_backup(db::snapshot_ctl&, tasks::task_manager::module_ptr, std::string snapshot_name, std::unordered_multimap<sstring, sstring> ks_tables, std::unordered_map<sstring, snapshot_dc_location> locations, bool move_files);

using send_backup_rpc_func = std::function<future<>(
    locator::host_id id, table_id, sstring, sstring, sstring, sstring, dht::token, dht::token, utils::chunked_vector<sstables::sstable_id>, bool
)>;

future<> 
run_global_backup(cql3::query_processor&, std::string snapshot_name, std::unordered_multimap<sstring, sstring> ks_tables, std::unordered_map<sstring, db::snapshot_dc_location> locations, bool move_files, send_backup_rpc_func f, tasks::progress_sink&);

future<>
backup_sstables(cql3::query_processor&, table_id table_id, std::string tag, std::string endpoint, std::string bucket, std::string prefix, dht::token first_token, dht::token last_token, utils::chunked_vector<sstables::sstable_id> sstable_ids, bool use_move, seastar::abort_source* as = nullptr);

}
