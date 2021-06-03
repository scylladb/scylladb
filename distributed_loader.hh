/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once


#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include <vector>
#include <functional>
#include <filesystem>
#include "seastarx.hh"
#include "sstables/compaction_descriptor.hh"

class database;
class table;
using column_family = table;
namespace db {
class system_distributed_keyspace;
namespace view {
class view_update_generator;
}
}

namespace sstables {

class entry_descriptor;
class foreign_sstable_open_info;
class sstable_directory;

}

namespace service {

class storage_proxy;
class migration_manager;

}

class distributed_loader {
public:
    static future<> reshape(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstables::reshape_mode mode,
            sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator);
    static future<> reshard(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator);
    static future<> process_sstable_dir(sharded<sstables::sstable_directory>& dir, bool sort_sstables_according_to_owner = true);
    static future<> lock_table(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring cf_name);

    static future<> verify_owner_and_mode(std::filesystem::path path);

    static future<size_t> make_sstables_available(sstables::sstable_directory& dir,
            sharded<database>& db, sharded<db::view::view_update_generator>& view_update_generator,
            std::filesystem::path datadir, sstring ks, sstring cf);
    static future<> process_upload_dir(distributed<database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks,
            distributed<db::view::view_update_generator>& view_update_generator, sstring ks_name, sstring cf_name);
    // Scan sstables under upload directory. Return a vector with smp::count entries.
    // Each entry with index of idx should be accessed on shard idx only.
    // Each entry contains a vector of sstables for this shard.
    // The table UUID is returned too.
    static future<std::tuple<utils::UUID, std::vector<std::vector<sstables::shared_sstable>>>>
            get_sstables_from_upload_dir(distributed<database>& db, sstring ks, sstring cf);
    static future<> populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf);
    static future<> populate_keyspace(distributed<database>& db, sstring datadir, sstring ks_name);
    static future<> init_system_keyspace(distributed<database>& db);
    static future<> ensure_system_table_directories(distributed<database>& db);
    static future<> init_non_system_keyspaces(distributed<database>& db, distributed<service::storage_proxy>& proxy, distributed<service::migration_manager>& mm);
    /**
     * Marks a keyspace (by name) as "prioritized" on bootstrap.
     * This will effectively let it bypass concurrency control.
     * The only real use for this is to avoid certain chicken and
     * egg issues.
     *
     * May only be called pre-bootstrap on main shard.
     */
    static void mark_keyspace_as_load_prio(const sstring&);
private:
    static future<> cleanup_column_family_temp_sst_dirs(sstring sstdir);
    static future<> handle_sstables_pending_delete(sstring pending_deletes_dir);
};
