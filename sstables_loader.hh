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

using namespace seastar;

class database;
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
    sharded<database>& _db;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::view::view_update_generator>& _view_update_generator;
    netw::messaging_service& _messaging;

public:
    sstables_loader(sharded<database>& db,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::view::view_update_generator>& view_update_generator,
            netw::messaging_service& messaging)
        : _db(db)
        , _sys_dist_ks(sys_dist_ks)
        , _view_update_generator(view_update_generator)
        , _messaging(messaging)
    {
        (void)_db;
        (void)_sys_dist_ks;
        (void)_view_update_generator;
        (void)_messaging;
    }
};
