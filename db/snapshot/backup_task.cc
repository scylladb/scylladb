/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

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

void backup_task_impl::do_backup() {
    if (!file_exists(_snapshot_dir.native()).get()) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    gate uploads;
    auto wait = defer([&uploads] { uploads.close().get(); });

            auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
            auto close_snapshot_dir_lister = deferred_close(snapshot_dir_lister);

            while (auto component_ent = snapshot_dir_lister.get().get()) {
                auto gh = uploads.hold();
                auto component_name = _snapshot_dir / component_ent->name;
                auto destination = fmt::format("/{}/{}/{}", _bucket, _prefix, component_ent->name);
                snap_log.trace("Upload {} to {}", component_name.native(), destination);
                // Start uploading in the background. The caller waits for these fibers
                // with the _uploads gate.
                // Parallelism is implicitly controlled in two ways:
                //  - s3::client::claim_memory semaphore
                //  - http::client::max_connections limitation
                // FIXME -- s3::client is not abortable yet, but when it will be, need to
                // propagate impl::_as abort requests into upload_file's fibers
                std::ignore = _client->upload_file(component_name, destination).handle_exception([comp = component_name] (auto ex) {
                    snap_log.error("Error uploading {}: {}", comp.native(), ex);
                    std::rethrow_exception(ex);
                }).finally([gh = std::move(gh)] {});
                thread::maybe_yield();
                utils::get_local_injector().inject("backup_task_pause", [] (auto& handler) {
                    snap_log.info("backup task: waiting");
                    return handler.wait_for_message(db::timeout_clock::now() + std::chrono::minutes(2));
                }).get();
                impl::_as.check();
            }
}

future<> backup_task_impl::run() {
    co_await _snap_ctl.run_snapshot_list_operation([this] {
        return async([this] {
            do_backup();
            snap_log.info("Finished backup");
        });
    });
}

} // db::snapshot namespace
