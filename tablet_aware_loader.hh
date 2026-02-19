/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "tasks/task_manager.hh"

#include <seastar/core/sharded.hh>

namespace db {
class system_distributed_keyspace;
}
namespace sstables {
class storage_manager;
}
namespace netw {
class messaging_service;
}

namespace replica {
class database;
}

class tablet_aware_loader : public peering_sharded_service<tablet_aware_loader> {
    sharded<service::storage_service>& _storage_service;
    sstables::storage_manager& _storage_manager;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<replica::database>& _db;
    shared_ptr<tasks::task_manager::module> _task_manager_module;

public:
    tablet_aware_loader(sharded<service::storage_service>& ss,
                        sstables::storage_manager& storage_manager,
                        sharded<db::system_distributed_keyspace>& sys_dist_ks,
                        sharded<replica::database>& db,
                        tasks::task_manager& tm);

    future<tasks::task_id> start_loading_task(
        std::string snapshot, std::string data_center, std::string rack, std::string keyspace, std::string table, std::string endpoint, std::string bucket);
    sharded<service::storage_service>& get_storage_service() const { return _storage_service; }
    sstables::storage_manager& get_storage_manager() const { return _storage_manager; }
    sharded<db::system_distributed_keyspace>& get_systesm_distributed_keyspace() const { return _sys_dist_ks; }
    sharded<replica::database>& get_db() const { return _db; }
    future<> stop();
};
