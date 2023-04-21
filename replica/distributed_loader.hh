/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once


#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include <seastar/util/bool_class.hh>
#include <vector>
#include <functional>
#include <filesystem>
#include "seastarx.hh"
#include "compaction/compaction_descriptor.hh"

namespace replica {
class database;
class table;
using column_family = table;
}

namespace db {
class config;
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
class storage_service;

}

namespace gms {
class gossiper;
}

class distributed_loader_for_tests;

namespace replica {

class table_population_metadata;

class distributed_loader {
    friend class ::distributed_loader_for_tests;
    friend class table_population_metadata;

    static future<> reshape(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstables::reshape_mode mode,
            sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator, std::function<bool (const sstables::shared_sstable&)> filter);
    static future<> reshard(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator);
    static future<> process_sstable_dir(sharded<sstables::sstable_directory>& dir, bool sort_sstables_according_to_owner = true);
    static future<> lock_table(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstring ks_name, sstring cf_name);
    static future<size_t> make_sstables_available(sstables::sstable_directory& dir,
            sharded<replica::database>& db, sharded<db::view::view_update_generator>& view_update_generator,
            std::filesystem::path datadir, sstring ks, sstring cf);
    using allow_offstrategy_compaction = bool_class<struct allow_offstrategy_compaction_tag>;
    using must_exist = bool_class<struct must_exist_tag>;
    static future<> populate_column_family(table_population_metadata& metadata, sstring subdir, allow_offstrategy_compaction, must_exist = must_exist::yes);
    static future<> populate_keyspace(distributed<replica::database>& db, sstring datadir, sstring ks_name);
    static future<> cleanup_column_family_temp_sst_dirs(sstring sstdir);
    static future<> handle_sstables_pending_delete(sstring pending_deletes_dir);

public:
    static future<> init_system_keyspace(distributed<replica::database>& db, distributed<service::storage_service>& ss, sharded<gms::gossiper>& g, db::config& cfg);
    static future<> init_non_system_keyspaces(distributed<replica::database>& db, distributed<service::storage_proxy>& proxy);
    static future<> ensure_system_table_directories(distributed<replica::database>& db);

    // Scan sstables under upload directory. Return a vector with smp::count entries.
    // Each entry with index of idx should be accessed on shard idx only.
    // Each entry contains a vector of sstables for this shard.
    // The table UUID is returned too.
    static future<std::tuple<utils::UUID, std::vector<std::vector<sstables::shared_sstable>>>>
            get_sstables_from_upload_dir(distributed<replica::database>& db, sstring ks, sstring cf);
    static future<> process_upload_dir(distributed<replica::database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks,
            distributed<db::view::view_update_generator>& view_update_generator, sstring ks_name, sstring cf_name);
};

}
