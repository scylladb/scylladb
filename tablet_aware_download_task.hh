/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "tasks/task_manager.hh"


class tablet_aware_loader;
class sstables_tablet_aware_loader;
namespace db {
class system_distributed_keyspace;
}
namespace sstables {
class storage_manager;
}
namespace replica {
class database;
}
class tablet_aware_download_task_impl : public tasks::task_manager::task::impl {
    std::string _snapshot;
    std::string _data_center;
    std::string _rack;
    std::string _keyspace;
    std::string _table;

    std::string _endpoint;
    std::string _bucket;

    tasks::task_manager::task::progress _total_progress;
    tablet_aware_loader& _tablet_aware_loader;

protected:
    future<> run() override;

public:
    tablet_aware_download_task_impl(tasks::task_manager::module_ptr module,
                                    tablet_aware_loader& tablet_aware_loader,
                                    std::string snapshot,
                                    std::string data_center,
                                    std::string rack,
                                    std::string keyspace,
                                    std::string table,
                                    std::string endpoint,
                                    std::string bucket) noexcept;

    std::string type() const override { return "tablet_aware_download"; }

    tasks::is_internal is_internal() const noexcept override { return tasks::is_internal::no; }

    tasks::is_user_task is_user_task() const noexcept override { return tasks::is_user_task::yes; }

    tasks::is_abortable is_abortable() const noexcept override { return tasks::is_abortable::yes; }

    future<tasks::task_manager::task::progress> get_progress() const override;
};
