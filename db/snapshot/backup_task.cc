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
#include "sstables/sstables.hh"
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
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _client(std::move(client))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
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
    auto ret = _total_progress;
    ret.completed = _progress.uploaded;
    co_return ret;
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

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    co_await process_snapshot_dir();

    co_await uploads_worker();

    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

future<> backup_task_impl::process_snapshot_dir() {
    auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());

    try {
        size_t total = 0;
        while (auto component_ent = co_await snapshot_dir_lister.get()) {
            const auto& name = component_ent->name;
            auto file_path = _snapshot_dir / name;
            auto st = co_await file_stat(file_path.native());
            total += st.size;
            _files.emplace_back(name);
        }
        _total_progress.total = total;
    } catch (...) {
        _ex = std::current_exception();
    }

    co_await snapshot_dir_lister.close();
    if (_ex) {
        co_await coroutine::return_exception_ptr(_ex);
    }
}

future<> backup_task_impl::uploads_worker() {
    gate uploads;

    try {
        for (auto it = _files.begin(); it != _files.end() && !_ex; ++it) {
            auto gh = uploads.hold();

            // Pre-upload break point. For testing abort in actual s3 client usage.
            co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

            std::ignore = upload_component(*it).handle_exception([this] (std::exception_ptr e) {
                // keep the first exception
                if (!_ex) {
                    _ex = std::move(e);
                }
            }).finally([gh = std::move(gh)] {});
            co_await coroutine::maybe_yield();
            co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
            if (impl::_as.abort_requested()) {
                _ex = impl::_as.abort_requested_exception_ptr();
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

future<> backup_task_impl::run() {
    // do_backup() removes a file once it is fully uploaded, so we are actually
    // mutating snapshots.
    co_await _snap_ctl.run_snapshot_modify_operation([this] {
        return do_backup();
    });
    snap_log.info("Finished backup");
}

} // db::snapshot namespace
