/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "tablet_aware_loader.hh"
#include "tablet_aware_download_task.hh"

tablet_aware_loader::tablet_aware_loader(sharded<service::storage_service>& ss,
                                         sstables::storage_manager& storage_manager,
                                         sharded<db::system_distributed_keyspace>& sys_dist_ks,
                                         sharded<replica::database>& db,
                                         tasks::task_manager& tm)
    :_storage_service(ss), _storage_manager(storage_manager)
    , _sys_dist_ks(sys_dist_ks)
    , _db(db)
    , _task_manager_module(make_shared<tasks::task_manager::module>(tm, "tablet_aware_loader")) {
    tm.register_module("tablet_aware_loader", _task_manager_module);
}

future<tasks::task_id> tablet_aware_loader::start_loading_task(
    std::string snapshot, std::string data_center, std::string rack, std::string keyspace, std::string table, std::string endpoint, std::string bucket) {
    auto task = co_await _task_manager_module->make_and_start_task<tablet_aware_download_task_impl>(
        {}, *this, snapshot, data_center, rack, keyspace, table, endpoint, bucket);
    co_return task->id();
}

future<> tablet_aware_loader::stop() {
    co_await _task_manager_module->stop();
}
