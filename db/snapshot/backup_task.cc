/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/lister.hh"
#include "utils/s3/client.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "schema/schema_fwd.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/component_type.hh"
#include "utils/error_injection.hh"

extern logging::logger snap_log;

namespace db::snapshot {

backup_task_impl::backup_task_impl(tasks::task_manager::module_ptr module,
                                   snapshot_ctl& ctl,
                                   shared_ptr<s3::client> client,
                                   sstring bucket,
                                   sstring prefix,
                                   sstring ks,
                                   std::filesystem::path snapshot_dir,
                                   table_id tid,
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _client(std::move(client))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
    , _table_id(tid)
    , _remove_on_uploaded(move_files) {
    _status.progress_units = "bytes ('total' may grow along the way)";
}

std::string backup_task_impl::type() const {
    return "backup";
}

tasks::is_internal backup_task_impl::is_internal() const noexcept {
    return tasks::is_internal::no;
}

tasks::is_abortable backup_task_impl::is_abortable() const noexcept {
    return tasks::is_abortable::yes;
}

future<tasks::task_manager::task::progress> backup_task_impl::get_progress() const {
    co_return tasks::task_manager::task::progress {
        .completed = _progress.uploaded,
        .total = _progress.total,
    };
}

tasks::is_user_task backup_task_impl::is_user_task() const noexcept {
    return tasks::is_user_task::yes;
}

future<> backup_task_impl::upload_component(sstring name) {
    auto component_name = _snapshot_dir / name;
    auto destination = fmt::format("/{}/{}/{}", _bucket, _prefix, name);
    snap_log.trace("Upload {} to {}", component_name.native(), destination);

    // Start uploading in the background. The caller waits for these fibers
    // with the uploads gate.
    // Parallelism is implicitly controlled in two ways:
    //  - s3::client::claim_memory semaphore
    //  - http::client::max_connections limitation
    try {
        co_await _client->upload_file(component_name, destination, _progress, &_as);
    } catch (const abort_requested_exception&) {
        snap_log.info("Upload aborted per requested: {}", component_name.native());
        throw;
    } catch (...) {
        snap_log.error("Error uploading {}: {}", component_name.native(), std::current_exception());
        throw;
    }

    if (!_remove_on_uploaded) {
        co_return;
    }

    // Delete the uploaded component to:
    // 1. Free up disk space immediately
    // 2. Avoid costly S3 existence checks on future backup attempts
    try {
        co_await remove_file(component_name.native());
    } catch (...) {
        // If deletion of an uploaded file fails, the backup process will continue.
        // While this doesn't halt the backup, it may indicate filesystem permissions
        // issues or system constraints that should be investigated.
        snap_log.warn("Failed to remove {}: {}", component_name, std::current_exception());
    }
}

future<> backup_task_impl::list_snapshot_dir() {
    auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());

    try {
        snap_log.debug("backup_task: listing {}", _snapshot_dir.native());
        while (auto component_ent = co_await snapshot_dir_lister.get()) {
            const auto& name = component_ent->name;
            auto file_path = _snapshot_dir / name;
            auto st = co_await file_stat(file_path.native());
            try {
                auto desc = sstables::parse_path(file_path, "", "");
                _sstable_comps[desc.generation].emplace_back(name, st.size);
                ++_num_sstable_comps;

                if (desc.component == sstables::component_type::Data) {
                    // If the sstable is already unlinked after the snapshot was taken
                    // track its generation in the unlinked_sstables list
                    // so it can be prioritized for backup
                    if (st.number_of_links == 1) {
                        snap_log.trace("do_backup: sstable with gen={} is already unlinked", desc.generation);
                        _unlinked_sstables.push_back(desc.generation);
                    }
                }
            } catch (const sstables::malformed_sstable_exception&) {
                _non_sstable_files.emplace_back(name, st.size);
            }
        }
        snap_log.debug("backup_task: found {} SSTables consisting of {} component files, and {} non-sstable files",
            _sstable_comps.size(), _num_sstable_comps, _non_sstable_files.size());
    } catch (...) {
        _ex = std::current_exception();
        snap_log.error("backup_task: listing {} failed: {}", _snapshot_dir.native(), _ex);
    }

    co_await snapshot_dir_lister.close();
    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

future<> backup_task_impl::backup_file(const comp_desc& desc) {
    _as.check();

    auto gh = _uploads.hold();
    uint64_t required = std::min(desc.size, max_concurrency);
    auto units = co_await get_units(_concurrency_sem, required, _as);

    // okay to drop future since uploads is always closed before exiting the function
    std::ignore = upload_component(desc.name).handle_exception([this] (std::exception_ptr e) {
        // keep the first exception
        if (!_ex) {
            _ex = std::move(e);
        }
    }).finally([gh = std::move(gh), units = std::move(units)] {});
};

future<> backup_task_impl::backup_sstable(sstables::generation_type gen, const comps_vector& comps) {
    snap_log.debug("Backing up SSTable generation {}", gen);
    for (auto it = comps.begin(); it != comps.end() && !_ex; ++it) {
        // Pre-upload break point. For testing abort in actual s3 client usage.
        co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

        const auto& desc = *it;
        co_await backup_file(desc);

        co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
    }
};

void backup_task_impl::on_unlink(sstables::generation_type gen) {
    if (_sstable_comps.contains(gen)) {
        _unlinked_sstables.push_back(gen);
    }
}

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    _backup_shard = this_shard_id();

    co_await _subscriptions.start();
    co_await _snap_ctl.db().invoke_on_all([&] (replica::database& db) {
        auto schema = db.find_schema(_table_id);
        auto sub = db.get_sstables_manager(*schema).subscribe([&] (sstables::generation_type gen, sstables::manager_event_type event) -> future<> {
            switch (event) {
            case sstables::manager_event_type::add:
                break;
            case sstables::manager_event_type::unlink:
                if (_sstable_comps.contains(gen)) {
                    return smp::submit_to(_backup_shard, [this, gen] {
                        on_unlink(gen);
                    });
                }
            }
            return make_ready_future();
        });
        _subscriptions.local() = make_foreign(seastar::make_shared(std::move(sub)));
    });

    co_await list_snapshot_dir();

    try {
        while (!_sstable_comps.empty() && !_ex) {
            auto to_backup = _sstable_comps.begin();
            // Prioritize unlinked sstables to free-up their disk space earlier.
            // This is particularly important when running backup at high utilization levels (e.g. over 90%)
            if (!_unlinked_sstables.empty()) {
                auto gen = _unlinked_sstables.back();
                _unlinked_sstables.pop_back();
                if (auto it = _sstable_comps.find(gen); it != _sstable_comps.end()) {
                    snap_log.debug("Prioritizing unlinked sstable gen={}", gen);
                    to_backup = it;
                } else {
                    snap_log.trace("Unlinked sstable gen={} was not found", gen);
                }
            }
            auto ent = _sstable_comps.extract(to_backup);
            co_await backup_sstable(ent.key(), ent.mapped());
        }

        for (auto it = _non_sstable_files.begin(); it != _non_sstable_files.end() && !_ex; ++it) {
            co_await backup_file(*it);
        }
    } catch (...) {
        _ex = std::current_exception();
    }

    co_await _uploads.close();
    co_await _subscriptions.stop();
    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

future<> backup_task_impl::run() {
    // do_backup() removes a file once it is fully uploaded, so we are actually
    // mutating snapshots.
    co_await _snap_ctl.run_snapshot_modify_operation([this] {
        return do_backup();
    });
    snap_log.info("Finished backup");
}

} // db::snapshot namespace
