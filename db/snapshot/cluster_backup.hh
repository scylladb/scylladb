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
#include "utils/chunked_vector.hh"

namespace db {
class snapshot_ctl;
class snapshot_table_helper;
}

namespace service {
class storage_proxy;
}

namespace replica {
    class database;
    class table;
}

namespace locator {
class token_metadata;
}

namespace db::snapshot {

std::string sstables_location(std::string_view prefix);
std::string snapshot_meta_location(std::string_view prefix, const replica::table&, std::string_view snapshot_name);
std::string snapshot_prefix_from_manifest(std::string_view manifest_path);

// Releases the references and deletes the catalog rows written under
// `name`, the catalog name of one snapshot attempt. `tables` are the names on
// the schema rows of `name`, written before the attempt's RPCs, so they cover
// tables dropped since. A reference may be a compacted-away sstable's last
// claim, so it is released, not deleted, and its components go with it. Local
// tables have no references, their residue is the rows alone. The schema rows
// and the attempt row of `name` are left to the caller.
future<> collect_snapshot_residue(replica::database& db, const locator::token_metadata& tm, const sstring& name,
        const utils::chunked_vector<std::pair<sstring, sstring>>& tables, db::snapshot_table_helper& sth);

future<tasks::task_id> 
start_global_backup(db::snapshot_ctl&, tasks::task_manager::module_ptr, std::string snapshot_name, std::unordered_multimap<sstring, sstring> ks_tables, std::unordered_map<sstring, snapshot_dc_location> locations, bool move_files);

future<>
backup_sstables(db::snapshot_ctl&, table_id table_id, std::string tag, std::string endpoint, std::string bucket, std::string prefix, dht::token first_token, dht::token last_token, utils::chunked_vector<sstables::sstable_id> sstable_ids, bool use_move);

}
