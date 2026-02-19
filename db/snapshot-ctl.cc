/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#include <algorithm>
#include <stdexcept>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "replica/database.hh"
#include "replica/global_table_ptr.hh"
#include "sstables/sstables_manager.hh"

using namespace std::chrono_literals;

logging::logger snap_log("snapshots");

namespace db {

snapshot_ctl::snapshot_ctl(sharded<replica::database>& db, tasks::task_manager& tm, sstables::storage_manager& sstm, config cfg)
    : _config(std::move(cfg))
    , _db(db)
    , _ops("snapshot_ctl")
    , _task_manager_module(make_shared<snapshot::task_manager_module>(tm))
    , _storage_manager(sstm)
{
    tm.register_module("snapshot", _task_manager_module);
    // FIXME: scan existing snapshots on disk and schedule garbage collection for those with ttl.
    if (this_shard_id() == 0) {
        _garbage_collector = garbage_collector();
    }
}

void snapshot_ctl::shutdown() noexcept {
    snap_log.debug("Shutdown");
    _shutdown = true;
    _gc_cond.signal();
}

future<> snapshot_ctl::stop() {
    shutdown();
    co_await std::exchange(_garbage_collector, make_ready_future<>());
    co_await _ops.close();
    co_await _task_manager_module->stop();
}

future<> snapshot_ctl::check_snapshot_not_exist(sstring ks_name, sstring name, std::optional<std::vector<sstring>> filter) {
    auto& ks = _db.local().find_keyspace(ks_name);
    return parallel_for_each(ks.metadata()->cf_meta_data(), [this, ks_name = std::move(ks_name), name = std::move(name), filter = std::move(filter)] (auto& pair) {
        auto& cf_name = pair.first;
        if (filter && std::find(filter->begin(), filter->end(), cf_name) == filter->end()) {
            return make_ready_future<>();
        }        
        auto& cf = _db.local().find_column_family(pair.second);
        return cf.snapshot_exists(name).then([ks_name = std::move(ks_name), name] (bool exists) {
            if (exists) {
                throw std::runtime_error(format("Keyspace {}: snapshot {} already exists.", ks_name, name));
            }
        });
    });
}

future<> snapshot_ctl::run_snapshot_modify_operation(noncopyable_function<future<>()>&& f) {
    return with_gate(_ops, [f = std::move(f), this] () mutable {
        return container().invoke_on(0, [f = std::move(f)] (snapshot_ctl& snap) mutable {
            return with_lock(snap._lock.for_write(), std::move(f));
        });
    });
}

future<> snapshot_ctl::take_snapshot(sstring tag, std::vector<sstring> keyspace_names, snapshot_options opts) {
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    if (keyspace_names.size() == 0) {
        std::ranges::copy(_db.local().get_keyspaces() | std::views::keys, std::back_inserter(keyspace_names));
    };

    return run_snapshot_modify_operation([tag = std::move(tag), keyspace_names = std::move(keyspace_names), opts, this] () mutable {
        return do_take_snapshot(std::move(tag), std::move(keyspace_names), opts);
    });
}

future<> snapshot_ctl::do_take_snapshot(sstring tag, std::vector<sstring> keyspace_names, snapshot_options opts) {
    co_await coroutine::parallel_for_each(keyspace_names, [tag, this] (const auto& ks_name) {
        return check_snapshot_not_exist(ks_name, tag);
    });
    co_await coroutine::parallel_for_each(keyspace_names, [this, tag = std::move(tag), opts] (const auto& ks_name) {
        return replica::database::snapshot_keyspace_on_all_shards(_db, ks_name, tag, opts);
    });
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, snapshot_options opts) {
    if (ks_name.empty()) {
        throw std::runtime_error("You must supply a keyspace name");
    }
    if (tables.empty()) {
        throw std::runtime_error("You must supply a table name");
    }
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    return run_snapshot_modify_operation([this, ks_name = std::move(ks_name), tables = std::move(tables), tag = std::move(tag), opts] () mutable {
        return do_take_column_family_snapshot(std::move(ks_name), std::move(tables), std::move(tag), opts);
    });
}

future<> snapshot_ctl::do_take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, snapshot_options opts) {
    co_await check_snapshot_not_exist(ks_name, tag, tables);
    snap_log.debug("take_snapshot: tag={} keyspace={} tables={}: skip_flush={} created_at={} expires_at={}",
            tag, ks_name, fmt::join(tables, ","),
            opts.skip_flush, opts.created_at, opts.expires_at.value_or(gc_clock::time_point::min()));
    co_await replica::database::snapshot_tables_on_all_shards(_db, ks_name, std::move(tables), std::move(tag), opts);
}

future<> snapshot_ctl::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name) {
    snap_log.debug("clear_snapshot: tag={} keyspaces={} table={}", tag, fmt::join(keyspace_names, ","), cf_name);
    return run_snapshot_modify_operation([this, tag = std::move(tag), keyspace_names = std::move(keyspace_names), cf_name = std::move(cf_name)] {
        return _db.local().clear_snapshot(tag, keyspace_names, cf_name);
    });
}

future<std::unordered_map<sstring, snapshot_ctl::db_snapshot_details>>
snapshot_ctl::get_snapshot_details() {
    using snapshot_map = std::unordered_map<sstring, db_snapshot_details>;

    snap_log.debug("get_snapshot_details");
    co_return co_await run_snapshot_list_operation(coroutine::lambda([this] () -> future<snapshot_map> {
        return _db.local().get_snapshot_details();
    }));
}

future<int64_t> snapshot_ctl::true_snapshots_size() {
    snap_log.debug("true_snapshots_size");
    co_return co_await run_snapshot_list_operation(coroutine::lambda([this] () -> future<int64_t> {
        int64_t total = 0;
        for (auto& [name, details] : co_await _db.local().get_snapshot_details()) {
            total += std::accumulate(details.begin(), details.end(), int64_t(0), [] (int64_t sum, const auto& d) { return sum + d.details.live; });
        }
        co_return total;
    }));
}

future<tasks::task_id> snapshot_ctl::start_backup(sstring endpoint, sstring bucket, sstring prefix, sstring keyspace, sstring table, sstring snapshot_name, bool move_files) {
    if (this_shard_id() != 0) {
        co_return co_await container().invoke_on(0, [&](auto& local) {
            return local.start_backup(endpoint, bucket, prefix, keyspace, table, snapshot_name, move_files);
        });
    }

    co_await coroutine::switch_to(_config.backup_sched_group);
    snap_log.info("Backup sstables from {}({}) to {}", keyspace, snapshot_name, endpoint);
    auto global_table = co_await get_table_on_all_shards(_db, keyspace, table);
    auto& storage_options = global_table->get_storage_options();
    if (!storage_options.is_local_type()) {
        throw std::invalid_argument("not able to backup a non-local table");
    }
    auto& local_storage_options = std::get<data_dictionary::storage_options::local>(storage_options.value);
    //
    // The keyspace data directories and their snapshots are arranged as follows:
    //
    //  <data dir>
    //  |- <keyspace name1>
    //  |  |- <column family name1>
    //  |     |- snapshots
    //  |        |- <snapshot name1>
    //  |          |- <snapshot file1>
    //  |          |- <snapshot file2>
    //  |          |- ...
    //  |        |- <snapshot name2>
    //  |        |- ...
    //  |  |- <column family name2>
    //  |  |- ...
    //  |- <keyspace name2>
    //  |- ...
    //
    auto dir = (local_storage_options.dir /
                sstables::snapshots_dir /
                std::string_view(snapshot_name));
    auto task = co_await _task_manager_module->make_and_start_task<::db::snapshot::backup_task_impl>(
        {}, *this, _storage_manager.container(), std::move(endpoint), std::move(bucket), std::move(prefix), keyspace, dir, global_table->schema()->id(), move_files);
    co_return task->id();
}

future<int64_t> snapshot_ctl::true_snapshots_size(sstring ks, sstring cf) {
    co_return co_await run_snapshot_list_operation(coroutine::lambda([this, ks = std::move(ks), cf = std::move(cf)] () -> future<int64_t> {
        int64_t total = 0;
        for (auto& [name, details] : co_await _db.local().find_column_family(ks, cf).get_snapshot_details()) {
            total += details.total;
        }
        co_return total;
    }));
}

future<> snapshot_ctl::garbage_collector() {
    auto garbage_collect_snapshot = [this] (const gc_info& info) -> future<> {
        snap_log.info("Garbage collecting snapshot {} of table {}.{}", info.tag, info.ks_name, info.table_name);
        try {
            co_await clear_snapshot(info.tag, {info.ks_name}, info.table_name);
        } catch (...) {
            snap_log.warn("Failed to garbage collect snapshot {} of table {}.{}: {}: Ignored", info.tag, info.ks_name, info.table_name, std::current_exception());
        }
    };
    while (!_shutdown) {
        future<> gc_future = make_ready_future();
        // FIXME: do not garbage collect snapshots during backup
        while (!_gc_queue.empty() && _gc_queue.front().expires_at <= gc_clock::now()) {
            auto info = _gc_queue.front();
            std::ranges::pop_heap(_gc_queue, std::greater{}, &gc_info::expires_at);
            _gc_queue.resize(_gc_queue.size() - 1);
            gc_future = gc_future.then([&, info = std::move(info)] () mutable {
                return garbage_collect_snapshot(info);
            });
        }
        co_await std::move(gc_future);
        auto wait_duration = !_gc_queue.empty() ? _gc_queue.front().expires_at - gc_clock::now() : 3600s;
        snap_log.debug("Garbage collection waiting for {}: queued={}", wait_duration, _gc_queue.size());
        try {
            co_await _gc_cond.wait(wait_duration);
        } catch (const condition_variable_timed_out&) {
            // expected, just loop again and check the queue
        } catch (...) {
            snap_log.warn("Garbage collector failed: {}: Ignored", std::current_exception());
        }
    }
}

void snapshot_ctl::schedule_garbage_collection(gc_clock::time_point when, sstring ks_name, sstring table_name, sstring tag) {
    if (this_shard_id() != 0) {
        on_internal_error(snap_log, "schedule_garbage_collection must be called on shard 0");
    }
    if (!_shutdown) {
        snap_log.info("Scheduling garbage collection of snapshot {} of table {}.{} at {}", ks_name, table_name, tag, when);
        _gc_queue.emplace_back(gc_info{
            .expires_at = when,
            .ks_name = std::move(ks_name),
            .table_name = std::move(table_name),
            .tag = std::move(tag)
        });
        std::ranges::push_heap(_gc_queue, std::greater{}, &gc_info::expires_at);
        _gc_cond.signal();
    }
}

} // namespace db
