/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <exception>

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "sstables/component_type.hh"
#include "utils/lister.hh"
#include "utils/s3/client.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "schema/schema_fwd.hh"
#include "sstables/exceptions.hh"
#include "sstables/generation_type.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
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

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    std::exception_ptr ex;
    // FIXME: this should be configurable, or derived dynamically from
    // the concurrency level of S3 uploads
    constexpr size_t max_concurrency = sstables::num_component_types * 2;
    auto concurrency_sem = semaphore(max_concurrency);
    gate uploads;
    auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());

    using comps_vector = utils::small_vector<std::string, sstables::num_component_types>;
    using comps_map = std::unordered_map<sstables::generation_type, comps_vector>;
    comps_map sstable_comps;
    size_t num_sstable_comps = 0;
    comps_vector non_sstable_files;
    std::vector<sstables::generation_type> unlinked_sstables;

    try {
        snap_log.debug("backup_task: listing {}", _snapshot_dir.native());
        for (;;) {
            auto component_ent = co_await snapshot_dir_lister.get();
            if (!component_ent.has_value()) {
                break;
            }

            const auto& name = component_ent->name;
            try {
                auto file_path = _snapshot_dir / name;
                auto [desc, ks, cf] = sstables::parse_path(file_path);
                sstable_comps[desc.generation].emplace_back(name);
                ++num_sstable_comps;

                if (desc.component == sstables::component_type::Data) {
                    auto st = co_await file_stat(file_path.native());
                    // If the sstable is already unlinked after the snapshot was taken
                    // track its generation in the unlinked_sstables list
                    // so it can be prioritized for backup
                    if (st.number_of_links == 1) {
                        snap_log.trace("do_backup: sstable with gen={} is already unlinked", desc.generation);
                        unlinked_sstables.push_back(desc.generation);
                    }
                }
            } catch (const sstables::malformed_sstable_exception&) {
                non_sstable_files.emplace_back(name);
            }
        }
        snap_log.debug("backup_task: found {} SSTables consisting of {} component files, and {} non-sstable files",
                sstable_comps.size(), num_sstable_comps, non_sstable_files.size());
    } catch (...) {
        ex = std::current_exception();
        snap_log.error("backup_task: listing {} failed: {}", _snapshot_dir.native(), ex);
    }
    co_await snapshot_dir_lister.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }

    class subscriber : public sstables::sstables_manager_subscriber {
        const comps_map& _sstable_comps;
        std::vector<sstables::generation_type>& _unlinked_sstables;
        unsigned _orig_shard;

    public:
        subscriber(const comps_map& sstable_comps, std::vector<sstables::generation_type>& unlinked_sstables, unsigned orig_shard)
            : _sstable_comps(sstable_comps)
            , _unlinked_sstables(unlinked_sstables)
            , _orig_shard(orig_shard)
        {}

        virtual future<> on_unlink(sstables::generation_type gen) override {
            return smp::submit_to(_orig_shard, [this, gen] {
                if (_sstable_comps.contains(gen)) {
                    snap_log.trace("subscriber::on_unlink: gen={}", gen);
                    _unlinked_sstables.push_back(gen);
                }
            });
        }
    };
    std::vector<foreign_ptr<shared_ptr<subscriber>>> subscribers;
    subscribers.resize(smp::count);
    auto orig_shard = this_shard_id();
    co_await _snap_ctl.db().invoke_on_all([&] (replica::database& db) {
        auto schema = db.find_schema(_table_id);
        auto sub = seastar::make_shared<subscriber>(sstable_comps, unlinked_sstables, orig_shard);
        db.get_sstables_manager(*schema).subscribe(sub);
        subscribers[this_shard_id()] = make_foreign(std::move(sub));
    });

    auto backup_comp = [&] (sstring name) -> future<> {
        auto gh = uploads.hold();
        auto units = co_await get_units(concurrency_sem, 1, _as);
        // okay to drop future since uploads is always closed before exiting the function
        std::ignore = upload_component(std::move(name)).handle_exception([&ex] (std::exception_ptr e) {
            // keep the first exception
            if (!ex) {
                ex = std::move(e);
            }
        }).finally([gh = std::move(gh), units = std::move(units)] {});
    };

    auto backup_sstable = [&] (sstables::generation_type gen, const comps_vector& comps) -> future<> {
        snap_log.debug("Backing up SSTable generation {}", gen);
        for (auto it = comps.begin(); it != comps.end() && !ex; ++it) {
            // Pre-upload break point. For testing abort in actual s3 client usage.
            co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

            const auto& name = *it;
            co_await backup_comp(name);

            co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
        }
    };

    try {
        while (!sstable_comps.empty() && !ex) {
            auto to_backup = sstable_comps.begin();
            // Prioritize unlinked sstables to free-up their disk space earlier.
            // This is particularly important when running backup at high utilization levels (e.g. over 90%)
            if (!unlinked_sstables.empty()) {
                auto gen = unlinked_sstables.back();
                unlinked_sstables.pop_back();
                if (auto it = sstable_comps.find(gen); it != sstable_comps.end()) {
                    snap_log.debug("Prioritizing unlinked sstable gen={}", gen);
                    to_backup = it;
                } else {
                    snap_log.trace("Unlinked sstable gen={} was not found", gen);
                }
            }
            auto ent = sstable_comps.extract(to_backup);
            co_await backup_sstable(ent.key(), ent.mapped());
        }

        for (auto it = non_sstable_files.begin(); it != non_sstable_files.end() && !ex; ++it) {
            co_await backup_comp(*it);
        }
    } catch (...) {
        ex = std::current_exception();
    }

    co_await uploads.close();

    co_await _snap_ctl.db().invoke_on_all([&] (replica::database& db) -> future<> {
        auto schema = db.find_schema(_table_id);
        auto sub = subscribers[this_shard_id()].release();
        co_await db.get_sstables_manager(*schema).unsubscribe(sub);
    });

    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
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
