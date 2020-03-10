/*
 * Copyright (C) 2018 ScyllaDB
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

#include <vector>
#include "gms/inet_address.hh"
#include "db/system_distributed_keyspace.hh"
#include "repair/repair.hh"
#include <seastar/core/distributed.hh>

class row_level_repair_gossip_helper;

namespace gms {
    class gossiper;
}

struct repair_service {
    distributed<gms::gossiper>& _gossiper;
    shared_ptr<row_level_repair_gossip_helper> _gossip_helper;
    tracker _tracker;
    repair_service(distributed<gms::gossiper>& gossiper, size_t max_repair_memory);
    ~repair_service();
    future<> stop();
private:
    bool _stopped = false;
};

future<> repair_init_messaging_service_handler(repair_service& rs, distributed<db::system_distributed_keyspace>& sys_dist_ks, distributed<db::view::view_update_generator>& view_update_generator);

class repair_info;

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes);

future<> shutdown_all_row_level_repair();
