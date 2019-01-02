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

future<> repair_init_messaging_service_handler(distributed<db::system_distributed_keyspace>& sys_dist_ks, distributed<db::view::view_update_from_staging_generator>& view_update_generator);

class repair_info;

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes);
