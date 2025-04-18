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
#include <unordered_set>

#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>

#include "utils/s3/client_fwd.hh"
#include "utils/small_vector.hh"
#include "tasks/task_manager.hh"
#include "sstables/component_type.hh"
#include "sstables/generation_type.hh"
#include "sstables/sstables_manager_subscription.hh"

namespace replica {
    class database;
}

namespace sstables {
class sstables_manager;
class storage_manager;
}

namespace db {
class snapshot_ctl;

namespace snapshot {

class backup_task_impl : public tasks::task_manager::task::impl {
    snapshot_ctl& _snap_ctl;
    sharded<sstables::storage_manager>& _sstm;
    sstring _endpoint;
    sstring _bucket;
    sstring _prefix;
    std::filesystem::path _snapshot_dir;
    table_id _table_id;
    bool _remove_on_uploaded;
    tasks::task_manager::task::progress _total_progress;

    std::exception_ptr _ex;
    std::vector<sstring> _files;
    using comps_vector = utils::small_vector<std::string, sstables::num_component_types>;
    using comps_map = std::unordered_map<sstables::generation_type, comps_vector>;
    comps_map _sstable_comps;   // Keeps all sstable components to back up, extract entries once queued for upload
    std::unordered_set<sstables::generation_type> _sstables_in_snapshot; // Keeps all sstable generations in snapshot
    std::vector<sstables::generation_type> _deleted_sstables;
    shard_id _backup_shard;

    class worker : sstables::sstables_manager_event_handler {
        sstables::sstables_manager& _manager;
        backup_task_impl& _task;
        shared_ptr<s3::client> _client;
        abort_source _as;
        std::exception_ptr _ex;

    public:
        worker(const replica::database& db, table_id t, backup_task_impl& task);

        future<> start_uploading();

        void abort();

        sstables::sstables_manager& manager() const noexcept {
            return _manager;
        }

    private:
        virtual future<> deleted_sstable(sstables::generation_type gen) const override;
        struct upload_permit {
            gate::holder gh;
            semaphore_units<> units;
        };
        future<> backup_file(sstring name, upload_permit permit);
        future<> upload_component(sstring name);
    };
    sharded<worker> _sharded_worker;
    std::vector<s3::upload_progress> _progress_per_shard{smp::count};

    future<> do_backup();
    future<> process_snapshot_dir();
    // Returns a disengaged optional when done
    std::optional<std::string> dequeue();
    void dequeue_sstable();
    void on_sstable_deletion(sstables::generation_type gen);

protected:
    virtual future<> run() override;

public:
    backup_task_impl(tasks::task_manager::module_ptr module,
                     snapshot_ctl& ctl,
                     sharded<sstables::storage_manager>& sstm,
                     sstring endpoint,
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
