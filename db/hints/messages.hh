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

#include <vector>
#include <seastar/core/lowres_clock.hh>
#include "gms/inet_address.hh"

#include "utils/UUID.hh"
#include "gms/inet_address.hh"

namespace db {

namespace hints {

struct sync_point_create_request {
    // The ID of the sync point to create.
    utils::UUID sync_point_id;
    // Towards which nodes hints should be replayed for this sync point?
    std::vector<gms::inet_address> target_endpoints;
    // The sync point will be deleted at this point of time if hints won't be
    // sent by that time.
    lowres_clock::time_point mark_deadline;
};

struct sync_point_create_response {};

struct sync_point_check_request {
    // The ID of the sync point whose status we want to check
    utils::UUID sync_point_id;
};

struct sync_point_check_response {
    // Returns true if the sync point has expired: either hints were replayed,
    // or the deadline was reached.
    bool expired;
};

}

}
