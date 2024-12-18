/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

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
                                   std::filesystem::path snapshot_dir) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _client(std::move(client))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir)) {
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

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    std::exception_ptr ex;
    gate uploads;
    auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());

    for (;;) {
        std::optional<directory_entry> component_ent;
        try {
            component_ent = co_await snapshot_dir_lister.get();
        } catch (...) {
            if (!ex) {
                ex = std::current_exception();
                break;
            }
        }
        if (!component_ent.has_value()) {
            break;
        }
        auto gh = uploads.hold();
        auto component_name = _snapshot_dir / component_ent->name;
        auto destination = fmt::format("/{}/{}/{}", _bucket, _prefix, component_ent->name);
        snap_log.trace("Upload {} to {}", component_name.native(), destination);

        // Pre-upload break point. For testing abort in actual s3 client usage.
        co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

        // Start uploading in the background. The caller waits for these fibers
        // with the uploads gate.
        // Parallelism is implicitly controlled in two ways:
        //  - s3::client::claim_memory semaphore
        //  - http::client::max_connections limitation
        std::ignore = _client->upload_file(component_name, destination, _progress, &_as).handle_exception([comp = component_name, &ex] (std::exception_ptr e) {
            snap_log.error("Error uploading {}: {}", comp.native(), e);
            // keep the first exception
            if (!ex) {
                ex = std::move(e);
            }
        }).finally([gh = std::move(gh)] {});
        co_await coroutine::maybe_yield();
        co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
        if (impl::_as.abort_requested()) {
            ex = impl::_as.abort_requested_exception_ptr();
            break;
        }
    }

    co_await snapshot_dir_lister.close();
    co_await uploads.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

future<> backup_task_impl::run() {
    co_await _snap_ctl.run_snapshot_list_operation([this] {
        return do_backup();
    });
    snap_log.info("Finished backup");
}

} // db::snapshot namespace
