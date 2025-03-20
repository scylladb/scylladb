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
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>

#include "utils/s3/client_fwd.hh"
#include "utils/small_vector.hh"
#include "tasks/task_manager.hh"
#include "sstables/component_type.hh"
#include "sstables/sstables_manager_subscription.hh"

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
    s3::upload_progress _progress = {};

    std::exception_ptr _ex;
    gate _uploads;
    struct comp_desc {
        sstring name;
        uint64_t size;
    };
    using comps_vector = utils::small_vector<comp_desc, sstables::num_component_types>;
    using comps_map = std::unordered_map<sstables::generation_type, comps_vector>;
    comps_map _sstable_comps;
    size_t _num_sstable_comps = 0;
    comps_vector _non_sstable_files;
    // FIXME: this should be configurable, or derived dynamically from
    // the concurrency level of S3 uploads
    constexpr static size_t max_concurrency = 32 << 10;
    semaphore _concurrency_sem{max_concurrency};
    shard_id _backup_shard;
    sharded<foreign_ptr<shared_ptr<sstables::manager_signal_connection_type>>> _subscriptions;
    std::vector<sstables::generation_type> _unlinked_sstables;

    future<> do_backup();
    future<> upload_component(sstring name);
    future<> list_snapshot_dir();
    // Leaves a background task, covered by _uploads gate
    future<> backup_file(const comp_desc& desc);
    future<> backup_sstable(sstables::generation_type gen, const comps_vector& comps);
    void on_unlink(sstables::generation_type gen);

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
