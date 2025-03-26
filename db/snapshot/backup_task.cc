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
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/component_type.hh"
#include "utils/error_injection.hh"

extern logging::logger snap_log;

namespace db::snapshot {

backup_task_impl::backup_task_impl(tasks::task_manager::module_ptr module,
                                   snapshot_ctl& ctl,
                                   sharded<sstables::storage_manager>& sstm,
                                   sstring endpoint,
                                   sstring bucket,
                                   sstring prefix,
                                   sstring ks,
                                   std::filesystem::path snapshot_dir,
                                   table_id tid,
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _sstm(sstm)
    , _endpoint(std::move(endpoint))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
    , _table_id(tid)
    , _remove_on_uploaded(move_files) {
    _status.progress_units = "bytes";
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
    auto p = co_await _sstm.map_reduce0(
        [this](const auto&) {
            return _progress_per_shard[this_shard_id()];
        }, s3::upload_progress(), std::plus<>());
    co_return tasks::task_manager::task::progress{
        .completed = p.uploaded,
        .total = p.total,
    };
}

tasks::is_user_task backup_task_impl::is_user_task() const noexcept {
    return tasks::is_user_task::yes;
}

future<> backup_task_impl::worker::upload_component(sstring name) {
    auto component_name = _task._snapshot_dir / name;
    auto destination = fmt::format("/{}/{}/{}", _task._bucket, _task._prefix, name);
    snap_log.trace("Upload {} to {}", component_name.native(), destination);

    // Start uploading in the background. The caller waits for these fibers
    // with the uploads gate.
    // Parallelism is implicitly controlled in two ways:
    //  - s3::client::claim_memory semaphore
    //  - http::client::max_connections limitation
    try {
        co_await _client->upload_file(component_name, destination, _task._progress_per_shard[this_shard_id()], &_as);
    } catch (const abort_requested_exception&) {
        snap_log.info("Upload aborted per requested: {}", component_name.native());
        throw;
    } catch (...) {
        snap_log.error("Error uploading {}: {}", component_name.native(), std::current_exception());
        throw;
    }

    if (!_task._remove_on_uploaded) {
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

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    co_await process_snapshot_dir();

    _backup_shard = this_shard_id();
    co_await _sharded_worker.start(std::ref(_snap_ctl.db()), _table_id, std::ref(*this));

    gate abort_gate;
    auto abort_sub = _as.subscribe([&] () noexcept {
        if (auto gh = abort_gate.try_hold()) {
            std::ignore = _sharded_worker.invoke_on_all([] (worker& m) {
                m.abort();
            }).finally([gh = std::move(gh)] {});
        }
    });

    try {
        co_await _sharded_worker.invoke_on_all([](worker& m) {
            return m.start_uploading();
        });
    } catch (...) {
        _ex = std::current_exception();
    }

    co_await abort_gate.close();
    abort_sub = {};
    co_await _sharded_worker.stop();

    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

future<> backup_task_impl::process_snapshot_dir() {
    auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    size_t num_sstable_comps = 0;

    try {
        snap_log.debug("backup_task: listing {}", _snapshot_dir.native());
        size_t total = 0;
        while (auto component_ent = co_await snapshot_dir_lister.get()) {
            const auto& name = component_ent->name;
            auto file_path = _snapshot_dir / name;
            auto st = co_await file_stat(file_path.native());
            total += st.size;
            try {
                auto desc = sstables::parse_path(file_path, "", "");
                const auto& gen = desc.generation;
                _sstable_comps[gen].emplace_back(name);
                _sstables_in_snapshot.insert(desc.generation);
                ++num_sstable_comps;

                // When the SSTable is only linked-to by the snapshot directory,
                // it is already deleted from the table's base directory, and
                // therefore it better be uploaded earlier to free-up its capacity.
                if (desc.component == sstables::component_type::Data && st.number_of_links == 1) {
                    snap_log.debug("backup_task: SSTable with generation {} is already deleted from the table", gen);
                    _deleted_sstables.push_back(gen);
                }
            } catch (const sstables::malformed_sstable_exception&) {
                _files.emplace_back(name);
            }
        }
        _total_progress.total = total;
        snap_log.debug("backup_task: found {} SSTables consisting of {} component files, and {} non-sstable files",
            _sstable_comps.size(), num_sstable_comps, _files.size());
    } catch (...) {
        _ex = std::current_exception();
        snap_log.error("backup_task: listing {} failed: {}", _snapshot_dir.native(), _ex);
    }

    co_await snapshot_dir_lister.close();
    if (_ex) {
        co_await coroutine::return_exception_ptr(_ex);
    }
}

future<> backup_task_impl::worker::start_uploading() {
    named_gate uploads(format("do_backup::uploads({})", _task._snapshot_dir));

    try {
        while (!_ex) {
            auto gh = uploads.hold();
            auto units = co_await _manager.dir_semaphore().get_units(1, _as);

            // Pre-upload break point. For testing abort in actual s3 client usage.
            co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

            auto name_opt = co_await smp::submit_to(_task._backup_shard, [this] () {
                return _task.dequeue();
            });
            if (!name_opt) {
                break;
            }
            // okay to drop future since async_gate is always closed before stopping
            std::ignore =
                backup_file(std::move(*name_opt), upload_permit(std::move(gh), std::move(units)));
            co_await coroutine::maybe_yield();
            co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
            if (_as.abort_requested()) {
                _ex = _as.abort_requested_exception_ptr();
                break;
            }
        }
    } catch (...) {
        _ex = std::current_exception();
    }

    co_await uploads.close();

    if (_ex) {
        co_await coroutine::return_exception_ptr(_ex);
    }
}

future<> backup_task_impl::worker::backup_file(sstring name, upload_permit permit) {
    try {
        co_await upload_component(name);
    } catch (...) {
        snap_log.debug("backup_file {} failed: {}", name, std::current_exception());
        // keep the first exception
        if (!_ex) {
            _ex = std::current_exception();
        }
    }
}

std::optional<std::string> backup_task_impl::dequeue() {
    if (_files.empty()) {
        dequeue_sstable();
    }
    if (_files.empty()) {
        return std::nullopt;
    }
    auto ret = std::move(_files.back());
    _files.pop_back();
    return ret;
}

void backup_task_impl::dequeue_sstable() {
    auto to_backup = _sstable_comps.begin();
    if (to_backup == _sstable_comps.end()) {
        return;
    }
    // Prioritize stables that were already deleted
    // for the table, to free up their capacity earlier.
    while (!_deleted_sstables.empty()) {
        auto gen = _deleted_sstables.back();
        _deleted_sstables.pop_back();
        auto it = _sstable_comps.find(gen);
        // It is possible that the sstable was already backed up
        // so silently skip this generation
        // and keep looking for another candidate
        if (it != _sstable_comps.end()) {
            to_backup = it;
            break;
        }
    }
    auto ent = _sstable_comps.extract(to_backup);
    snap_log.debug("Backing up SSTable generation {}", ent.key());
    for (auto& name : ent.mapped()) {
        _files.emplace_back(std::move(name));
    }
}

void backup_task_impl::on_sstable_deletion(sstables::generation_type gen) {
    if (_sstable_comps.contains(gen)) {
        _deleted_sstables.push_back(gen);
    }
}

backup_task_impl::worker::worker(const replica::database& db, table_id t, backup_task_impl& task)
    : _manager(db.get_sstables_manager(*db.find_schema(t)))
    , _task(task)
    , _client(task._sstm.local().get_endpoint_client(task._endpoint))
{
    _manager.subscribe(*this);
}

void backup_task_impl::worker::abort() {
    _as.request_abort();
}

future<> backup_task_impl::worker::deleted_sstable(sstables::generation_type gen) const {
    // The notification is called for any sstable, so `gen` may belong
    // to another table, or to an sstable that was created after the snapshot
    // was taken.
    // To avoid needless call to submit_to on another shard (which is expensive),
    // check if `gen` was included in the snapshot.
    //
    // Note: looking up gen in `_sstables_in_snapshot` is safe, although it was
    // created on `backup_shard`, since it is immutable after `process_snapshot_dir` is done.
    if (_task._sstables_in_snapshot.contains(gen)) {
        snap_log.debug("SSTable with generation {} was deleted from the table", gen);
        return smp::submit_to(_task._backup_shard, [this, gen] {
            _task.on_sstable_deletion(gen);
        });
    }
    return make_ready_future();
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
