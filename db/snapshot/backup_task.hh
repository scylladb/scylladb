/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include "utils/s3/client_fwd.hh"
#include "tasks/task_manager.hh"

namespace db {
class snapshot_ctl;

namespace snapshot {

class backup_task_impl : public tasks::task_manager::task::impl {
    snapshot_ctl& _snap_ctl;
    shared_ptr<s3::client> _client;
    sstring _bucket;
    sstring _prefix;
    std::filesystem::path _snapshot_dir;
    bool _remove_on_uploaded;
    s3::upload_progress _progress = {};

    future<> do_backup();
    future<> upload_component(sstring name);

protected:
    virtual future<> run() override;

public:
    backup_task_impl(tasks::task_manager::module_ptr module,
                     snapshot_ctl& ctl,
                     shared_ptr<s3::client> cln,
                     sstring bucket,
                     sstring prefix,
                     sstring ks,
                     std::filesystem::path snapshot_dir,
                     bool move_files) noexcept;

    virtual std::string type() const override;
    virtual tasks::is_internal is_internal() const noexcept override;
    virtual tasks::is_abortable is_abortable() const noexcept override;
    virtual future<tasks::task_manager::task::progress> get_progress() const override;
    virtual tasks::is_user_task is_user_task() const noexcept override;
};

} // snapshot namespace
} // db namespace
