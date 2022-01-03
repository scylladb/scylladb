/*
 * Copyright (C) 2021-present ScyllaDB
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
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "utils/UUID.hh"
#include "sstables/shared_sstable.hh"

using namespace seastar;

namespace replica {
class database;
}

namespace netw { class messaging_service; }
namespace db {
class system_distributed_keyspace;
namespace view {
class view_update_generator;
}
}

// The handler of the 'storage_service/load_new_ss_tables' endpoint which, in
// turn, is the target of the 'nodetool refresh' command.
// Gets sstables from the upload directory and makes them available in the
// system. Built on top of the distributed_loader functionality.
class sstables_loader : public seastar::peering_sharded_service<sstables_loader> {
    sharded<replica::database>& _db;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::view::view_update_generator>& _view_update_generator;
    netw::messaging_service& _messaging;

    // Note that this is obviously only valid for the current shard. Users of
    // this facility should elect a shard to be the coordinator based on any
    // given objective criteria
    //
    // It shouldn't be impossible to actively serialize two callers if the need
    // ever arise.
    bool _loading_new_sstables = false;

    future<> load_and_stream(sstring ks_name, sstring cf_name,
            utils::UUID table_id, std::vector<sstables::shared_sstable> sstables,
            bool primary_replica_only);

public:
    sstables_loader(sharded<replica::database>& db,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::view::view_update_generator>& view_update_generator,
            netw::messaging_service& messaging)
        : _db(db)
        , _sys_dist_ks(sys_dist_ks)
        , _view_update_generator(view_update_generator)
        , _messaging(messaging)
    {
    }

    /**
     * Load new SSTables not currently tracked by the system
     *
     * This can be called, for instance, after copying a batch of SSTables to a CF directory.
     *
     * This should not be called in parallel for the same keyspace / column family, and doing
     * so will throw an std::runtime_exception.
     *
     * @param ks_name the keyspace in which to search for new SSTables.
     * @param cf_name the column family in which to search for new SSTables.
     * @return a future<> when the operation finishes.
     */
    future<> load_new_sstables(sstring ks_name, sstring cf_name,
            bool load_and_stream, bool primary_replica_only);
};
