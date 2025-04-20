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
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "replica/database.hh"
#include "replica/global_table_ptr.hh"
#include "sstables/sstables_manager.hh"

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
}

future<> snapshot_ctl::stop() {
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

future<> snapshot_ctl::take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf) {
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    if (keyspace_names.size() == 0) {
        std::ranges::copy(_db.local().get_keyspaces() | std::views::keys, std::back_inserter(keyspace_names));
    };

    return run_snapshot_modify_operation([tag = std::move(tag), keyspace_names = std::move(keyspace_names), sf, this] () mutable {
        return do_take_snapshot(std::move(tag), std::move(keyspace_names), sf);
    });
}

future<> snapshot_ctl::do_take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf) {
    co_await coroutine::parallel_for_each(keyspace_names, [tag, this] (const auto& ks_name) {
        return check_snapshot_not_exist(ks_name, tag);
    });
    co_await coroutine::parallel_for_each(keyspace_names, [this, tag = std::move(tag), sf] (const auto& ks_name) {
        return replica::database::snapshot_keyspace_on_all_shards(_db, ks_name, tag, bool(sf));
    });
}

future<> snapshot_ctl::take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf) {
    if (ks_name.empty()) {
        throw std::runtime_error("You must supply a keyspace name");
    }
    if (tables.empty()) {
        throw std::runtime_error("You must supply a table name");
    }
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    return run_snapshot_modify_operation([this, ks_name = std::move(ks_name), tables = std::move(tables), tag = std::move(tag), sf] () mutable {
        return do_take_column_family_snapshot(std::move(ks_name), std::move(tables), std::move(tag), sf);
    });
}

future<> snapshot_ctl::do_take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf) {
    co_await check_snapshot_not_exist(ks_name, tag, tables);
    co_await replica::database::snapshot_tables_on_all_shards(_db, ks_name, std::move(tables), std::move(tag), bool(sf));
}

future<> snapshot_ctl::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name) {
    return run_snapshot_modify_operation([this, tag = std::move(tag), keyspace_names = std::move(keyspace_names), cf_name = std::move(cf_name)] {
        return _db.local().clear_snapshot(tag, keyspace_names, cf_name);
    });
}

future<std::unordered_map<sstring, snapshot_ctl::db_snapshot_details>>
snapshot_ctl::get_snapshot_details() {
    using snapshot_map = std::unordered_map<sstring, db_snapshot_details>;

    co_return co_await run_snapshot_list_operation(coroutine::lambda([this] () -> future<snapshot_map> {
        return _db.local().get_snapshot_details();
    }));
}

future<int64_t> snapshot_ctl::true_snapshots_size() {
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
    auto cln = _storage_manager.get_endpoint_client(endpoint);
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
        {}, *this, std::move(cln), std::move(bucket), std::move(prefix), keyspace, dir, global_table->schema()->id(), move_files);
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

}
