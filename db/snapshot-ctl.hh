/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#pragma once

#include <vector>

#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include "database_fwd.hh"
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

using namespace seastar;

namespace db {

class snapshot_ctl : public peering_sharded_service<snapshot_ctl> {
public:
    struct snapshot_details {
        int64_t live;
        int64_t total;
        sstring cf;
        sstring ks;
    };
    explicit snapshot_ctl(sharded<database>& db) : _db(db) {}

    future<> stop() {
        return _ops.close();
    }

    /**
     * Takes the snapshot for all keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_snapshot(sstring tag) {
        return take_snapshot(tag, {});
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    future<> take_snapshot(sstring tag, std::vector<sstring> keyspace_names);

    /**
     * Takes the snapshot of multiple tables. A snapshot name must be specified.
     *
     * @param ks_name the keyspace which holds the specified column family
     * @param tables a vector of tables names to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, std::vector<sstring> tables, sstring tag);

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param keyspaceName the keyspace which holds the specified column family
     * @param columnFamilyName the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag);

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     * If a cf_name is specified, only that table will be deleted
     */
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name);

    future<std::unordered_map<sstring, std::vector<snapshot_details>>> get_snapshot_details();

    future<int64_t> true_snapshots_size();
private:
    sharded<database>& _db;
    seastar::rwlock _lock;
    seastar::gate _ops;

    future<> check_snapshot_not_exist(sstring ks_name, sstring name, std::optional<std::vector<sstring>> filter = {});

    template <typename Func>
    std::result_of_t<Func()> run_snapshot_modify_operation(Func&&);

    template <typename Func>
    std::result_of_t<Func()> run_snapshot_list_operation(Func&&);
};

}
