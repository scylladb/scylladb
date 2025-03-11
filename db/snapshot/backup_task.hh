/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include <exception>
#include <vector>
#include <unordered_map>

#include <seastar/core/gate.hh>

#include "utils/s3/client_fwd.hh"
#include "utils/small_vector.hh"
#include "tasks/task_manager.hh"
#include "sstables/component_type.hh"
#include "sstables/generation_type.hh"

namespace db {
class snapshot_ctl;

namespace snapshot {

class backup_task_impl : public tasks::task_manager::task::impl {
    snapshot_ctl& _snap_ctl;
    shared_ptr<s3::client> _client;
    sstring _bucket;
    sstring _prefix;
    std::filesystem::path _snapshot_dir;
    table_id _table_id;
    bool _remove_on_uploaded;
    tasks::task_manager::task::progress _total_progress;
    s3::upload_progress _progress;

    std::exception_ptr _ex;
    std::vector<sstring> _files;
    using comps_vector = utils::small_vector<std::string, sstables::num_component_types>;
    using comps_map = std::unordered_map<sstables::generation_type, comps_vector>;
    comps_map _sstable_comps;
    std::vector<sstables::generation_type> _deleted_sstables;

    future<> do_backup();
    future<> upload_component(sstring name);
    future<> process_snapshot_dir();
    future<> uploads_worker();
    struct upload_permit {
        gate::holder gh;
    };
    future<> backup_file(sstring name, upload_permit permit);
    // Returns a disengaged optional when done
    std::optional<std::string> dequeue();
    void dequeue_sstable();

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
                     table_id tid,
                     bool move_files) noexcept;

    virtual std::string type() const override;
    virtual tasks::is_internal is_internal() const noexcept override;
    virtual tasks::is_abortable is_abortable() const noexcept override;
    virtual future<tasks::task_manager::task::progress> get_progress() const override;
    virtual tasks::is_user_task is_user_task() const noexcept override;
};

} // snapshot namespace
} // db namespace
