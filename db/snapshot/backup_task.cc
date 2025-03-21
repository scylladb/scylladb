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
#include "sstables/sstables_manager.hh"
#include "utils/error_injection.hh"

extern logging::logger snap_log;

namespace db::snapshot {

backup_task_impl::backup_task_impl(tasks::task_manager::module_ptr module,
                                   snapshot_ctl& ctl,
                                   sstring endpoint,
                                   sstring bucket,
                                   sstring prefix,
                                   sstring ks,
                                   std::filesystem::path snapshot_dir,
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _endpoint(std::move(endpoint))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
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
    auto p =
        co_await _snap_ctl.container().map_reduce0([this](const auto&) { return _progress_per_shard[this_shard_id()]; }, s3::upload_progress(), std::plus<s3::upload_progress>());
    co_return tasks::task_manager::task::progress{
        .completed = p.uploaded,
        .total = p.total,
    };
}

tasks::is_user_task backup_task_impl::is_user_task() const noexcept {
    return tasks::is_user_task::yes;
}

future<> backup_task_impl::upload_component(shared_ptr<s3::client> client, abort_source& as, s3::upload_progress& progress, sstring name) {
    auto component_name = _snapshot_dir / name;
    auto destination = fmt::format("/{}/{}/{}", _bucket, _prefix, name);
    snap_log.trace("Upload {} to {}", component_name.native(), destination);

    // Start uploading in the background. The caller waits for these fibers
    // with the uploads gate.
    // Parallelism is implicitly controlled in two ways:
    //  - s3::client::claim_memory semaphore
    //  - http::client::max_connections limitation
    try {
        co_await client->upload_file(component_name, destination, progress, &as);
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

namespace {

future<std::vector<std::tuple<size_t, sstring>>> get_backup_files(const std::filesystem::path& snapshot_dir) {
    std::exception_ptr ex;
    auto snapshot_dir_lister = directory_lister(snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    std::vector<std::tuple<size_t, sstring>> backup_files;
    std::optional<directory_entry> component_ent;

    do {
        try {
            component_ent = co_await snapshot_dir_lister.get();
            if (component_ent) {
                auto component_name = snapshot_dir / component_ent->name;
                auto size = co_await file_size(component_name.native());
                backup_files.emplace_back(size, std::move(component_ent->name));
            }
        } catch (...) {
            ex = std::current_exception();
            break;
        }
    } while (component_ent);
    co_await snapshot_dir_lister.close();
    if (ex)
        co_await coroutine::return_exception_ptr(std::move(ex));

    std::ranges::sort(backup_files, std::greater());
    co_return backup_files;
}
} // namespace

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    auto backup_files = co_await get_backup_files(_snapshot_dir);
    std::atomic_size_t counter = 0;

    sharded<abort_source> sharded_aborter;
    co_await sharded_aborter.start();
    gate as_gate;

    // Subscribe to the task's main abort source (_as). When aborted, the subscription triggers an abort on every shard via the sharded aborter.
    auto s = std::make_unique<optimized_optional<abort_source::subscription>>(_as.subscribe([&]() noexcept {
        try {
            auto h = as_gate.hold();
            std::ignore = sharded_aborter.invoke_on_all([ex = _as.abort_requested_exception_ptr()](abort_source& aborter) {
                aborter.request_abort_ex(ex);
            }).finally([h = std::move(h)] {});
        } catch (...) {
        }
    }));

    // This will capture any exception from the backup process.
    std::exception_ptr ret_ex;
    co_await sharded_aborter
        .invoke_on_all([this, &backup_files, &counter, &sharded_aborter](abort_source& aborter) -> future<> {
            auto shard_id = this_shard_id();
            gate uploads;
            std::exception_ptr ex;
            auto sharded_aborter_fn = [&sharded_aborter, &aborter, &ex]() -> future<> {
                // If the `aborter` was requested to abort the backup, no need to propagate the exception to other shards since it can be initiated by two ways:
                // 1. The task's abort source requested to abort the backup on all shards (see the subscription creation).
                // 2. Another shard requested to abort the backup since it got an exception.
                if (ex && !aborter.abort_requested()) {
                    // On the contrary, if we have the `ex` set, we need to propagate it to other shards to abort the backup since the only reason it was filled
                    // it is any failure except the abortion from the `abort_source` (any of them).
                    co_await sharded_aborter.invoke_on_all([&ex](abort_source& abort_service) -> future<> {
                        abort_service.request_abort_ex(ex);
                        co_return;
                    });
                }
            };
            try {
                auto cln = _snap_ctl.storage_manager().container().local().get_endpoint_client(_endpoint);

                // Process backup files continuously until:
                //   1. There are no more files.
                //   2. An exception is thrown.
                //   3. An abort is requested.
                while (true) {
                    // One of two things happened: the `upload_component` threw an exception, `aborter`'s exception was set.
                    // The later can happen if the `aborter` was requested to abort the backup by task's abort source or another shard requested to abort since
                    // it got an exception
                    if (ex || aborter.abort_requested()) {
                        break;
                    }

                    // Fetching files sequentially within each shard allows less loaded shards to take on more work, compensating for those that are lagging behind.
                    auto name_idx = counter++;
                    if (name_idx >= backup_files.size()) {
                        break;
                    }
                    auto gh = uploads.hold();
                    // Pre-upload break point. For testing abort in actual s3 client usage.
                    co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

                    // Start uploading the component asynchronously.
                    // Future is ignored here because:
                    // - The client (cln) is maintained via shared ownership.
                    // - Filenames are passed by value.
                    // - The abort source remains valid for the duration of the function.
                    // - The gate ensures that execution remains until completion.
                    std::ignore = upload_component(cln, aborter, _progress_per_shard[shard_id], std::get<1>(backup_files[name_idx]))
                                      .handle_exception([&ex, &sharded_aborter_fn](std::exception_ptr e)->future<> {
                                          // Record the exception to eventually abort the rest of the backup.
                                          ex = std::move(e);
                                          co_await sharded_aborter_fn();
                                      }).finally([gh = std::move(gh)] {});

                    co_await coroutine::maybe_yield();
                    co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
                }
            } catch (...) {
                // Catch any exception thrown during backup processing.
                ex = std::current_exception();
            }
            co_await sharded_aborter_fn();
            co_await uploads.close();
            if (ex) {
                // Rethrow to allow the outer handler to catch and propagate the exception.
                std::rethrow_exception(std::move(ex));
            }
        }).handle_exception([&ret_ex](std::exception_ptr e) {
            // Capture any exception rethrown from the shard operations.
            ret_ex = std::move(e);
        });
    co_await as_gate.close();
    s.reset();
    co_await sharded_aborter.stop();
    if (ret_ex || _as.abort_requested()) {
        // Either the sharded backup ended with an exception or the task's abort source requested to abort the backup.
        co_await coroutine::return_exception_ptr(ret_ex ? ret_ex : _as.abort_requested_exception_ptr());
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
