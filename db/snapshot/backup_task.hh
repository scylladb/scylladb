/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "tasks/task_manager.hh"

namespace s3 { class client; }

namespace db {
class snapshot_ctl;

namespace snapshot {

class backup_task_impl : public tasks::task_manager::task::impl {
    snapshot_ctl& _snap_ctl;
    shared_ptr<s3::client> _client;
    sstring _bucket;
    sstring _ks;
    sstring _snapshot_name;
    std::exception_ptr _ex;

    future<> run(sstring data_dir);

protected:
    virtual future<> run() override;

public:
    backup_task_impl(tasks::task_manager::module_ptr module, snapshot_ctl& ctl, shared_ptr<s3::client> cln, sstring bucket, sstring ks, sstring snapshot_name) noexcept;

    virtual std::string type() const override;
    virtual tasks::is_internal is_internal() const noexcept override;
    virtual tasks::is_abortable is_abortable() const noexcept override;
};

} // snapshot namespace
} // db namespace
