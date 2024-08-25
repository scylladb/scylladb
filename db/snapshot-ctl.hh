/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#pragma once

#include <vector>

#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include "replica/database_fwd.hh"
#include "tasks/task_manager.hh"
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

using namespace seastar;

namespace sstables { class storage_manager; }

namespace db {

namespace snapshot {

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "snapshot") {}
};

class backup_task_impl;

} // snapshot namespace

class snapshot_ctl : public peering_sharded_service<snapshot_ctl> {
public:
    using skip_flush = bool_class<class skip_flush_tag>;
    using snap_views = bool_class<class snap_views_tag>;

    struct table_snapshot_details {
        int64_t total;
        int64_t live;
    };

    struct table_snapshot_details_ext {
        sstring ks;
        sstring cf;
        table_snapshot_details details;
    };

    struct config {
        seastar::scheduling_group backup_sched_group;
    };

    using db_snapshot_details = std::vector<table_snapshot_details_ext>;

    snapshot_ctl(sharded<replica::database>& db, tasks::task_manager& tm, sstables::storage_manager& sstm, config cfg);

    future<> stop();

    /**
     * Takes the snapshot for all keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_snapshot(sstring tag, skip_flush sf = skip_flush::no) {
        return take_snapshot(tag, {}, sf);
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspace_names the names of the keyspaces to snapshot; empty means "all"
     */
    future<> take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf = skip_flush::no);

    /**
     * Takes the snapshot of multiple tables. A snapshot name must be specified.
     *
     * @param ks_name the keyspace which holds the specified column family
     * @param tables a vector of tables names to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, snap_views, skip_flush sf = skip_flush::no);

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param ks_name the keyspace which holds the specified column family
     * @param cf_name the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag, snap_views, skip_flush sf = skip_flush::no);

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     * If a cf_name is specified, only that table will be deleted
     */
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name);

    future<tasks::task_id> start_backup(sstring endpoint, sstring bucket, sstring keyspace, sstring snapshot_name);

    future<std::unordered_map<sstring, db_snapshot_details>> get_snapshot_details();

    future<int64_t> true_snapshots_size();
    future<int64_t> true_snapshots_size(sstring ks, sstring cf);
private:
    config _config;
    sharded<replica::database>& _db;
    seastar::rwlock _lock;
    seastar::gate _ops;
    shared_ptr<snapshot::task_manager_module> _task_manager_module;
    sstables::storage_manager& _storage_manager;

    future<> check_snapshot_not_exist(sstring ks_name, sstring name, std::optional<std::vector<sstring>> filter = {});

    template <typename Func>
    std::invoke_result_t<Func> run_snapshot_modify_operation(Func&&);

    template <typename Func>
    std::invoke_result_t<Func> run_snapshot_list_operation(Func&& f) {
        return with_gate(_ops, [f = std::move(f), this] () {
            return container().invoke_on(0, [f = std::move(f)] (snapshot_ctl& snap) mutable {
                return with_lock(snap._lock.for_read(), std::move(f));
            });
        });
    }

    friend class snapshot::backup_task_impl;

    future<> do_take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf = skip_flush::no);
    future<> do_take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, snap_views, skip_flush sf = skip_flush::no);
};

}
