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
#include "db/system_keyspace.hh"
#include "sstables/sstable_directory.hh"

namespace replica {
class database;
class table;
using column_family = table;
}

namespace db {
class config;
class system_keyspace;
namespace view {
class view_builder;
}
}

namespace sstables {

class entry_descriptor;
class foreign_sstable_open_info;

}

namespace service {

class storage_proxy;

}

namespace locator {
class effective_replication_map_factory;
}

class distributed_loader_for_tests;

namespace replica {

class table_populator;
class global_table_ptr;

class distributed_loader {
    friend class ::distributed_loader_for_tests;
    friend class table_populator;

    static future<> reshape(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstables::reshape_mode mode,
            sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator, std::function<bool (const sstables::shared_sstable&)> filter);
    static future<> reshard(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator,
            compaction::owned_ranges_ptr owned_ranges_ptr = nullptr);
    static future<> process_sstable_dir(sharded<sstables::sstable_directory>& dir, sstables::sstable_directory::process_flags flags);
    static future<> lock_table(global_table_ptr&, sharded<sstables::sstable_directory>& dir);
    static future<size_t> make_sstables_available(sstables::sstable_directory& dir,
            sharded<replica::database>& db, sharded<db::view::view_builder>& vb,
            bool needs_view_update, sstring ks, sstring cf);
    static future<> populate_keyspace(distributed<replica::database>& db, sharded<db::system_keyspace>& sys_ks, keyspace& ks, sstring ks_name);
    static future<std::tuple<table_id, std::vector<std::vector<sstables::shared_sstable>>>>
        get_sstables_from(distributed<replica::database>& db, sstring ks, sstring cf, sstables::sstable_open_config cfg,
        noncopyable_function<future<>(global_table_ptr&, sharded<sstables::sstable_directory>&)> start_dir);

public:
    static future<> init_system_keyspace(sharded<db::system_keyspace>&, distributed<locator::effective_replication_map_factory>&, distributed<replica::database>&);
    static future<> init_non_system_keyspaces(distributed<replica::database>& db, distributed<service::storage_proxy>& proxy, sharded<db::system_keyspace>& sys_ks);

    // Scan sstables under upload directory. Return a vector with smp::count entries.
    // Each entry with index of idx should be accessed on shard idx only.
    // Each entry contains a vector of sstables for this shard.
    // The table UUID is returned too.
    static future<std::tuple<table_id, std::vector<std::vector<sstables::shared_sstable>>>>
            get_sstables_from_upload_dir(distributed<replica::database>& db, sstring ks, sstring cf, sstables::sstable_open_config cfg);
    static future<std::tuple<table_id, std::vector<std::vector<sstables::shared_sstable>>>>
            get_sstables_from_object_store(distributed<replica::database>& db, sstring ks, sstring cf, sstring endpoint, sstring bucket, sstring prefix, sstables::sstable_open_config cfg);
    static future<> process_upload_dir(distributed<replica::database>& db, sharded<db::view::view_builder>& vb, sstring ks_name, sstring cf_name);
};

}
