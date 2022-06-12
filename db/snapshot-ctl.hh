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
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

using namespace seastar;

namespace db {

class snapshot_ctl : public peering_sharded_service<snapshot_ctl> {
public:
    using skip_flush = bool_class<class skip_flush_tag>;
    using allow_view_snapshots = bool_class<class allow_view_snapsots_tag>;

    struct snapshot_details {
        int64_t live;
        int64_t total;
        sstring cf;
        sstring ks;
    };
    explicit snapshot_ctl(sharded<replica::database>& db) : _db(db) {}

    future<> stop() {
        return _ops.close();
    }

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
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    future<> take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf = skip_flush::no);

    /**
     * Takes the snapshot of multiple tables. A snapshot name must be specified.
     *
     * @param ks_name the keyspace which holds the specified column family
     * @param tables a vector of tables names to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf = skip_flush::no, allow_view_snapshots av = allow_view_snapshots::no);

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param keyspaceName the keyspace which holds the specified column family
     * @param columnFamilyName the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag, skip_flush sf = skip_flush::no, allow_view_snapshots av = allow_view_snapshots::no);

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     * If a cf_name is specified, only that table will be deleted
     */
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name);

    future<std::unordered_map<sstring, std::vector<snapshot_details>>> get_snapshot_details();

    future<int64_t> true_snapshots_size();
private:
    sharded<replica::database>& _db;
    seastar::rwlock _lock;
    seastar::gate _ops;

    future<> check_snapshot_not_exist(sstring ks_name, sstring name, std::optional<std::vector<sstring>> filter = {});

    template <typename Func>
    std::result_of_t<Func()> run_snapshot_modify_operation(Func&&);

    template <typename Func>
    std::result_of_t<Func()> run_snapshot_list_operation(Func&&);

    future<> do_take_snapshot(sstring tag, std::vector<sstring> keyspace_names, skip_flush sf = skip_flush::no);
    future<> do_take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag, skip_flush sf = skip_flush::no, allow_view_snapshots av = allow_view_snapshots::no);
};

}
